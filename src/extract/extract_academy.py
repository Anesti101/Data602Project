import boto3
import pandas as pd
from io import BytesIO
import re
import os
from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION"),
)

bucket_name = os.getenv("BUCKET_NAME")

paginator = s3.get_paginator("list_objects_v2")

# List all Academy CSVs
# academy_files = [
#     obj["Key"]
#     for page in paginator.paginate(Bucket=bucket_name, Prefix="Academy/")
#     for obj in page.get("Contents", [])
#     if obj["Key"].endswith(".csv")
# ]

# print(f"Found {len(academy_files)} Academy files")
# for f in academy_files[:5]:
#     print(f)


# def parse_filename(key):
#     # Extract filename from full path e.g. 'Academy/Business_20_2019-02-11.csv'
#     filename = key.split("/")[-1].replace(".csv", "")
#     # Split into parts: ['Business', '20', '2019-02-11']
#     parts = filename.split("_", 2)
#     course = parts[0]  # 'Business'
#     cohort = int(parts[1])  # 20
#     date = parts[2]  # '2019-02-11'
#     return course, cohort, date


def extract_academy() -> pd.DataFrame:
    academy_files = [
        obj["Key"]
        for page in paginator.paginate(Bucket=bucket_name, Prefix="Academy/")
        for obj in page.get("Contents", [])
        if obj["Key"].endswith(".csv")
    ]

    dfs = []

    for file in academy_files:
        obj = s3.get_object(Bucket=bucket_name, Key=file)
        df = pd.read_csv(BytesIO(obj["Body"].read()))
        df["source_file"] = file  # only metadata we add — provenance tracking
        dfs.append(df)

    return pd.concat(dfs, ignore_index=True)


if __name__ == "__main__":
    df = extract_academy()
    print(f"Extracted {df.shape[0]} rows, {df.shape[1]} columns")
    print(df.head())

# print(f"Total rows: {all_academy.shape[0]}")
# print(f"Columns: {all_academy.shape[1]}")
# print(all_academy.head())
# print(all_academy.dtypes)
