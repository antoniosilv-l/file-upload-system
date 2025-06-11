import boto3
import os

def upload_to_s3(file):
    """
    Upload a file to S3.

    Args:
        file: The uploaded file object.
    """
    s3 = boto3.client("s3")
    bucket = os.getenv("S3_BUCKET_NAME", "meu-bucket-datalake")
    key = f"staging/{file.name}"

    file.seek(0)
    s3.upload_fileobj(file, bucket, key)