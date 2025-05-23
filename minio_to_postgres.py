from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, regexp_extract, udf
from pyspark.sql.types import StringType, IntegerType
from dotenv import load_dotenv
import phonenumbers
import os

# ---------------------- Load Environment Variables ----------------------
load_dotenv()

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"
PG_URL = f"jdbc:postgresql://{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DB')}"
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
SOURCE_FILE = os.getenv("DESTINATION_FILE")  # This is the file in the bucket
BUCKET_NAME = os.getenv("BUCKET_NAME")
SPARK_JARS = [
    os.getenv("postgresql-42.7.5.jar"),
    os.getenv("hadoop-aws-3.3.4.jar"),
    os.getenv("aws-java-sdk-bundle-1.12.783.jar")
]

# ---------------------- Spark Session ----------------------
spark = SparkSession.builder \
    .appName("MinIO + PostgreSQL ETL") \
    .config("spark.jars", ",".join(SPARK_JARS)) \
    .config("spark.driver.extraJavaOptions", "-Daws.java.v1.disableDeprecationAnnouncement=true") \
    .getOrCreate()

# Configure Hadoop for MinIO (s3a)
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", MINIO_ACCESS_KEY)
hadoop_conf.set("fs.s3a.secret.key", MINIO_SECRET_KEY)
hadoop_conf.set("fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.connection.ssl.enabled", str(MINIO_SECURE).lower())
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# ---------------------- Extract ----------------------
print("\nReading data from MinIO (CSV)...")
df = spark.read.option("header", "true").csv(f"s3a://{BUCKET_NAME}/{SOURCE_FILE}")
print("\nSchema of raw data:")
df.printSchema()
print("\nPreview of raw data:")
df.show()

# ---------------------- Transform ----------------------

EMAIL_REGEX = r'^(?!\.)[a-zA-Z0-9._%+-=]+(?<!\.)@[a-zA-Z0-9-]+(\.[a-zA-Z]{2,})+$'
BLACKLISTED_EMAILS = []
BLACKLISTED_PHONES = []

def clean_and_format_number(number):
    try:
        clean_number = ''.join(filter(str.isdigit, number))
        if clean_number.startswith('9191'):
            clean_number = clean_number[-10:]
        parsed_number = phonenumbers.parse(clean_number, "IN")
        if phonenumbers.is_valid_number(parsed_number) and parsed_number.country_code == 91:
            return phonenumbers.format_number(parsed_number, phonenumbers.PhoneNumberFormat.E164)
    except Exception:
        return None
    return None

clean_and_format_udf = udf(clean_and_format_number, StringType())

def transform_email(df: DataFrame, email_col: str = "email") -> DataFrame:
    return df.withColumn(
        email_col,
        when(
            (regexp_extract(col(email_col), EMAIL_REGEX, 0) != "") &
            (~col(email_col).isin(BLACKLISTED_EMAILS)),
            col(email_col)
        ).otherwise(None)
    )

def transform_phone(df: DataFrame, phone_col: str = "phone") -> DataFrame:
    df = df.withColumn("id", df["id"].cast(IntegerType()))
    return df.withColumn(
        phone_col,
        clean_and_format_udf(col(phone_col))
    ).withColumn(
        phone_col,
        when(
            ~col(phone_col).isin(BLACKLISTED_PHONES),
            col(phone_col)
        ).otherwise(None)
    )

print("\nTransforming data (validating email and formatting phone)...")
new_df = transform_email(df, email_col="email")
new_df = transform_phone(new_df, phone_col="phone")

print("\nSchema of transformed data:")
new_df.printSchema()
print("\nPreview of transformed data:")
new_df.show()

# ---------------------- Load ----------------------
print("\nWriting transformed data to PostgreSQL...")
new_df.write.jdbc(
    url=PG_URL,
    table="processed_minio_data",
    mode="overwrite",
    properties={
        "user": PG_USER,
        "password": PG_PASSWORD,
        "driver": "org.postgresql.Driver",
        "truncate": "true"
    }
)

print("\nData successfully written to PostgreSQL table: 'processed_minio_data'.")
spark.stop()