import boto3
import os
import io
from datetime import datetime
from ._normalizer import normalize_column_name

def upload_to_s3(df, original_filename, assunto, sub_assunto):
    """
    Upload a dataframe to S3 as CSV with organized folder structure.

    Args:
        df: The processed dataframe to upload.
        original_filename: The original filename for reference.
        assunto: The main subject/topic.
        sub_assunto: The sub-subject/topic.
    """
    s3 = boto3.client("s3")
    bucket = os.getenv("S3_BUCKET_NAME", "file-upload-system-s3-prd")
    
    assunto_normalized = normalize_column_name(assunto)
    sub_assunto_normalized = normalize_column_name(sub_assunto)
    
    upload_date = datetime.now().strftime("%Y-%m-%d")
    
    # Always save as CSV, change extension
    base_filename = os.path.splitext(original_filename)[0]
    csv_filename = f"{base_filename}.csv"
    
    key = f"{assunto_normalized}/{sub_assunto_normalized}/{upload_date}/{csv_filename}"

    # Convert dataframe to CSV and upload
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False, encoding='utf-8')
    csv_buffer.seek(0)
    
    # Convert to bytes for upload
    csv_bytes = io.BytesIO(csv_buffer.getvalue().encode('utf-8'))
    
    s3.upload_fileobj(csv_bytes, bucket, key)