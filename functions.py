import os
import boto3
import json
import psycopg2
from sqlalchemy import create_engine


CREDENTIALS_FILE = "credentials.json"

with open(CREDENTIALS_FILE, "r") as file:
    credentials = json.load(file)

AWS_ACCESS_KEY = credentials["aws_access_key"]
AWS_SECRET_KEY = credentials["aws_secret_key"]
BUCKET_NAME = credentials["s3_bucket"]
LOCAL_CSV_FOLDER = credentials["local_csv_folder"]
REDSHIFT_HOST = credentials["redshift_host"]
REDSHIFT_DB = credentials["redshift_db"]
REDSHIFT_USER = credentials["redshift_user"]
REDSHIFT_PASSWORD = credentials["redshift_password"]
IAM_ROLE = credentials["iam_role"]


s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
)

conn = psycopg2.connect(
        dbname=REDSHIFT_DB,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD,
        host=REDSHIFT_HOST
    )
cur = conn.cursor()


def upload_to_s3(local_folder, bucket_name):
    for file_name in os.listdir(local_folder):
        if file_name.endswith(".csv"):
            print(file_name)
            local_file_path = os.path.join(local_folder, file_name)
            s3_key = f"raw/{file_name}"  # under raw folder in s3 bucket
            s3_client.upload_file(local_file_path, bucket_name, s3_key)
            print(f"Uploaded {file_name} to s3://{bucket_name}/{s3_key}")


def create_tables(cur):
    table_queries = [
        """
        CREATE TABLE IF NOT EXISTS customers (
            id INT PRIMARY KEY,
            customer_name VARCHAR(255)
        );
        """
        ,
        """
        CREATE TABLE IF NOT EXISTS countries (
            id INT PRIMARY KEY,
            city  VARCHAR(100),
            country  VARCHAR(100),
            sales_channel VARCHAR(255)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS sales (
            category   VARCHAR(100),
            subcategory   VARCHAR(100),
            product_family   VARCHAR(100),
            key_product   VARCHAR(100),
            sku   VARCHAR(100),
            description   VARCHAR(300),
            grade   VARCHAR(100),
            country_id  INT,
            cost_currency   VARCHAR(10),
            cost_per_device  DECIMAL(10,3) ,
            sales_date  DATE ,
            sold_currency   VARCHAR(10),
            price_sold_per_device  DECIMAL(10,3) ,
            status   VARCHAR(100),
            customer_id  INT ,
            quantity  INT ,
            sales_order_id  INT PRIMARY KEY,
            serial  INT,
            bin_id   VARCHAR(10),
            FOREIGN KEY (customer_id) REFERENCES customers(id),
            FOREIGN KEY (country_id) REFERENCES countries(id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS fx_rates (
            Date DATE,
            GBPUSD DECIMAL(10,5), 
            EURUSD DECIMAL(10,5), 
            AUDUSD DECIMAL(10,5), 
            EURGBP DECIMAL(10,5), 
            EURAUD DECIMAL(10,5), 
            GBPAUD DECIMAL(10,5),
            JPYUSD DECIMAL(10,5), 
            JPYAUD DECIMAL(10,5), 
            JPYEUR DECIMAL(10,5), 
            JPYGBP DECIMAL(10,5), 
            KRWUSD DECIMAL(10,5), 
            KRWAUD DECIMAL(10,5), 
            KRWEUR DECIMAL(10,5),
            KRWGBP DECIMAL(10,5), 
            KRWJPY DECIMAL(10,5), 
            HKDUSD DECIMAL(10,5), 
            HKDAUD DECIMAL(10,5), 
            HKDEUR DECIMAL(10,5), 
            HKDGBP DECIMAL(10,5), 
            HKDJPY DECIMAL(10,5),
            HKDKRW DECIMAL(10,5), 
            TWDUSD DECIMAL(10,5), 
            TWDAUD DECIMAL(10,5), 
            TWDEUR DECIMAL(10,5), 
            TWDGBP DECIMAL(10,5), 
            TWDJPY DECIMAL(10,5), 
            TWDKRW DECIMAL(10,5),
            TWDHKD DECIMAL(10,5)

        );
        """,
        """
        CREATE TABLE IF NOT EXISTS costs (
            "Instance type"  VARCHAR(50),
            "db.r5.4xlarge($)"  DECIMAL(10,8) ,
            "db.r6g.xlarge($)"  DECIMAL(10,8) ,
            "db.r6g.4xlarge($)"  DECIMAL(10,8) ,
            "db.r6g.large($)"  DECIMAL(10,8) ,
            "db.r5.xlarge($)"  DECIMAL(10,8) ,
            "No instance type($)"  DECIMAL(10,8) ,
            "db.r5.large($)"  DECIMAL(10,8) ,
            "db.t3.medium($)"  DECIMAL(10,8) ,
            "db.t4g.medium($)"  DECIMAL(10,8) ,
            "db.t2.medium($)"  DECIMAL(10,8) ,
            "Total costs($)"  DECIMAL(10,8) 
            );
        """
    ]

    for query in table_queries:
        cur.execute(query)

    cur.close()
    conn.close()
    print("Tables created successfully!")


def load_data(cur):


    load_queries = [
        f"""
        COPY customers FROM 's3://{BUCKET_NAME}/raw/customers.csv'
        IAM_ROLE '{IAM_ROLE}'
        CSV IGNOREHEADER 1;
        """,
        f"""
        COPY sales FROM 's3://{BUCKET_NAME}/raw/sales.csv'
        IAM_ROLE '{IAM_ROLE}'
        CSV IGNOREHEADER 1;
        """,
        f"""
        COPY countries FROM 's3://{BUCKET_NAME}/raw/countries.csv'
        IAM_ROLE '{IAM_ROLE}'
        CSV IGNOREHEADER 1;
        """,
        f"""
        COPY fx_rates FROM 's3://{BUCKET_NAME}/raw/fx_rates.csv'
        IAM_ROLE '{IAM_ROLE}'
        CSV IGNOREHEADER 1;
        """,
        f"""
        COPY costs FROM 's3://{BUCKET_NAME}/raw/costs.csv'
        IAM_ROLE '{IAM_ROLE}'
        CSV IGNOREHEADER 1;
        """
    ]

    for query in load_queries:
        cur.execute(query)

    conn.commit()
    cur.close()
    conn.close()
    print("Data loaded successfully into Redshift!")

