import os
import boto3
import json


CREDENTIALS_FILE = "credentials.json"

with open(CREDENTIALS_FILE, "r") as file:
    credentials = json.load(file)

AWS_ACCESS_KEY = credentials["aws_access_key"]
AWS_SECRET_KEY = credentials["aws_secret_key"]
BUCKET_NAME = credentials["s3_bucket"]
LOCAL_CSV_FOLDER = credentials["local_csv_folder"]

s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
)

def upload_to_s3(local_folder, bucket_name):
    for file_name in os.listdir(local_folder):
        if file_name.endswith(".csv"):
            print(file_name)
            local_file_path = os.path.join(local_folder, file_name)
            s3_key = f"raw/{file_name}"  # under raw folder in s3 bucket
            s3_client.upload_file(local_file_path, bucket_name, s3_key)
            print(f"Uploaded {file_name} to s3://{bucket_name}/{s3_key}")

