"""
Module: extract_academy.py

Purpose:
Extract Academy CSV files from S3 and persist them as a Spark table.

Responsibilities:
- Connect to AWS S3 and locate academy CSV files under the Academy prefix
- Read each file into pandas and preserve source filename metadata
- Combine records for Spark persistence
- Persist the academy dataset as a Spark table for downstream analytics

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

academy_files = [
    obj["Key"]
    for page in paginator.paginate(
        Bucket=bucket_name,
        Prefix="Academy/"
    )
    for obj in page.get("Contents", [])
    if obj["Key"].endswith(".csv")
]


print(f"Found {len(academy_files)} academy source files")

dfs = []

for file in academy_files:
    obj = s3.get_object(Bucket=bucket_name, Key=file)
    df = pd.read_csv(BytesIO(obj["Body"].read()))
    df["source_file"] = file
    dfs.append(df)

all_academy = pd.concat(dfs, ignore_index=True)

spark_df = spark.createDataFrame(all_academy)
spark_df.write.mode("overwrite").saveAsTable("academy")
spark.sql("SELECT * FROM academy").show()
