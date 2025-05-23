from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, regexp_extract, udf
from pyspark.sql.types import StringType
from dotenv import load_dotenv
import phonenumbers
import os

# ---------------------- Load Environment Variables ----------------------
load_dotenv()

PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_DB = os.getenv("PG_DB")
POSTGRES_JAR = os.getenv("postgresql-42.7.5.jar")

# ---------------------- Spark Session ----------------------
spark = SparkSession.builder \
    .appName("PostgreSQL ETL with PySpark") \
    .config("spark.jars", POSTGRES_JAR) \
    .getOrCreate()

url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
properties = {
    "user": PG_USER,
    "password": PG_PASSWORD,
    "driver": "org.postgresql.Driver"
}

raw_data_table = "raw_data"
processed_data_table = "processed_data"

# ---------------------- Extract ----------------------
print("\nExtracting data from PostgreSQL...")
df = spark.read.jdbc(url, raw_data_table, properties=properties)
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

print("\nTransforming data (cleaning emails and phones)...")
new_df = transform_email(df, email_col="email")
new_df = transform_phone(new_df, phone_col="phone")

print("\nSchema of transformed data:")
new_df.printSchema()
print("\nPreview of transformed data:")
new_df.show()

# ---------------------- Load ----------------------
print("\nLoading transformed data into PostgreSQL...")
new_df.write.jdbc(
    url=url,
    table=processed_data_table,
    mode="overwrite",
    properties={**properties, "truncate": "true"}
)

print("\nData successfully written to PostgreSQL table: 'processed_data'.")
spark.stop()
