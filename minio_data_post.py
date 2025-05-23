from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv
import os

load_dotenv()

minio_endpoint = os.getenv("MINIO_ENDPOINT")
minio_access_key = os.getenv("MINIO_ACCESS_KEY")
minio_secret_key = os.getenv("MINIO_SECRET_KEY")
minio_secure = os.getenv("MINIO_SECURE", "false").lower() == "true"
source_file = os.getenv("SOURCE_FILE")
bucket_name = os.getenv("BUCKET_NAME", "email-phone-data")
destination_file = os.getenv("DESTINATION_FILE")

def main():
    # Initialize MinIO client
    client = Minio(
        minio_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=minio_secure,
    )

    # Create bucket if it doesn't already exist
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"Bucket created: '{bucket_name}'")
    else:
        print(f"Bucket already exists: '{bucket_name}'")

    # Upload file to the bucket
    client.fput_object(
        bucket_name, destination_file, source_file,
    )
    print("File upload successful.")

if __name__ == "__main__":
    try:
        main()
    except S3Error as exc:
        print("MinIO operation failed. Please check your configuration and try again.")
