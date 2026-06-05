"""
Module: extract_assessments.py

Purpose:
Extract assessment text source files from S3 and persist line-level records to Spark.

Responsibilities:
- Connect to AWS S3 and find assessment text files in the Talent prefix
- Parse each file into line_text records with source_file metadata
- Create a Spark table for downstream assessment parsing
- Document extraction and normalization behavior for assessment data

Author: Project Team
"""

import boto3
import pandas as pd
from io import BytesIO
from pyspark.sql import SparkSession

s3 = boto3.client("s3")

bucket_name = "data602-final-project"

paginator = s3.get_paginator("list_objects_v2")

assessment_files = [
    obj["Key"]
    for page in paginator.paginate(Bucket=bucket_name)
    for obj in page.get("Contents", [])
    if obj["Key"].startswith("Talent/")
    and obj["Key"].endswith(".txt")
]

rows = []

for file in assessment_files:
    obj = s3.get_object(
        Bucket=bucket_name,
        Key=file
    )

    body = obj["Body"].read().decode("utf-8")
    lines = body.splitlines()

    for line in lines:
        rows.append({
            "source_file": file,
            "line_text": line
        })

# Flatten the assessment text into a DataFrame to support regex extraction later.
all_assessments = pd.DataFrame(rows)

spark_df = spark.createDataFrame(all_assessments)
spark_df.write.mode("overwrite").saveAsTable("assessments")
spark.sql("SELECT * FROM assessments").show()
