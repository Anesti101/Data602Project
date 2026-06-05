"""
Module: extract_applicants.py

Purpose:
Extract applicant CSV files from S3 and persist them as a Spark table.

Responsibilities:
- Connect to AWS S3 and locate applicant CSV files by suffix
- Read each CSV into pandas and tag rows with source filename
- Concatenate all applicant files for Spark ingestion
- Persist the combined applicant dataset as a Spark table

Author: Project Team
"""

import boto3
import pandas as pd
from io import BytesIO
from pprint import pprint as pp
from pyspark.sql import SparkSession

s3 = boto3.client("s3")

bucket_name = "data602-final-project"

paginator = s3.get_paginator("list_objects_v2")

applicants_files = [
    obj["Key"]
    for obj in paginator.paginate(Bucket=bucket_name)
    for obj in obj.get("Contents", [])
    if obj["Key"].endswith("Applicants.csv")
]

print("Applicants files in bucket:")
for file in applicants_files:
    print(file)

dfs = []

for file in applicants_files:
    obj = s3.get_object(Bucket=bucket_name, Key=file)
    df = pd.read_csv(BytesIO(obj["Body"].read()))
    df["Source_file"] = file
    dfs.append(df)

all_applicants = pd.concat(dfs, ignore_index=True)

spark_df = spark.createDataFrame(all_applicants)
spark_df.write.mode("overwrite").saveAsTable("applicants")
spark.sql("SELECT * FROM applicants").show()
