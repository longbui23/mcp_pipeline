import boto3
from dotenv import load_dotenv
import os
import io
import json

class S3Hook:
    def __init__(self):
        load_dotenv()
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name="us-east-2"
        )

    def upload_file(self, content, bucket: str, s3_key: str):
        try:
            if not isinstance(content, str):
                content = json.dumps(content, default=str)
            self.s3.put_object(Body=content, Bucket=bucket, Key=s3_key)
            print(f"Uploaded content to s3://{bucket}/{s3_key}")
        except Exception as e:
            print(f"Error uploading to S3: {e}")
            raise

    def close(self):
        self.s3 = None
        print("S3 connection closed.")