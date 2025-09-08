# s3_uploader.py
import os
import boto3
from config import AWS_REGION, S3_BUCKET_NAME, S3_FOLDER

# Initialize S3 client
s3_client = boto3.client("s3", region_name=AWS_REGION)

def upload_file_to_s3(filepath: str, s3_key: str):
    """
    Upload a single file to S3.
    """
    try:
        s3_client.upload_file(filepath, S3_BUCKET_NAME, s3_key)
        print(f"Uploaded {filepath} to s3://{S3_BUCKET_NAME}/{s3_key}")
    except Exception as e:
        print(f"Failed to upload {filepath}: {e}")

def extract_and_upload():
    """
    Extract all media files from the local folder and upload to S3.
    """
    for filename in os.listdir(MEDIA_INPUT_FOLDER):
        filepath = os.path.join(MEDIA_INPUT_FOLDER, filename)
        if os.path.isfile(filepath):
            s3_key = os.path.join(S3_FOLDER, filename)
            upload_file_to_s3(filepath, s3_key)

if __name__ == "__main__":
    extract_and_upload()
