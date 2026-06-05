"""
Module: extract_talent.py

Purpose:
Extract talent JSON data from the S3 source bucket and persist it as a Spark table.

Responsibilities:
- Connect to AWS S3 and locate Talent JSON files
- Load JSON content into pandas and normalize nested structures
- Write the aggregated talent data to a Spark table
- Document source extraction rules and Spark persistence strategy

Author: Project Team
"""

import boto3
import pandas as pd
from io import BytesIO
import json
from pyspark.sql import SparkSession

# AWS S3 bucket containing final project source datasets.
s3 = boto3.client('s3')

bucket_name = 'data602-final-project'

paginator = s3.get_paginator('list_objects_v2')

talent_json_files = [
    obj['Key']
    for page in paginator.paginate(Bucket=bucket_name)
    for obj in page.get("Contents", [])
    if obj['Key'].startswith('Talent/')
    and obj['Key'].endswith('.json')
]

print(f"Found {len(talent_json_files)} files")
print(talent_json_files[:5])

dfs = []

for file in talent_json_files[:5]:
    obj = s3.get_object(
        Bucket=bucket_name,
        Key=file
    )

    data = json.loads(obj['Body'].read().decode('utf-8'))
    df = pd.json_normalize(data)
    df['Source_file'] = file
    dfs.append(df)

# Combine talent records into a single DataFrame so Spark can create a table.
all_talent = pd.concat(dfs, ignore_index=True)

spark_df = spark.createDataFrame(all_talent)
spark_df.write.mode("overwrite").saveAsTable("talent")
spark.sql("SELECT * FROM talent").show()
