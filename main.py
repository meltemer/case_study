from functions import upload_to_s3,create_tables,load_data
import json

CREDENTIALS_FILE = "credentials.json"

with open(CREDENTIALS_FILE, "r") as file:
    credentials = json.load(file)

BUCKET_NAME = credentials["s3_bucket"]
LOCAL_CSV_FOLDER = credentials["local_csv_folder"]

if __name__ == "__main__":

    upload_to_s3(LOCAL_CSV_FOLDER, BUCKET_NAME)

    create_tables()

    load_data()

    print('process completed successfully')
