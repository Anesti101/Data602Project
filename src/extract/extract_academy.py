import boto3
import pandas as pd
from io import BytesIO
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
 
dfs = []
 
for file in academy_files:
    obj = s3.get_object(
        Bucket=bucket_name,
        Key=file
    )
 
    df = pd.read_csv(BytesIO(obj["Body"].read()))
    df["source_file"] = file
    dfs.append(df)
 
all_academy = pd.concat(dfs, ignore_index=True)
spark_df = spark.createDataFrame(all_applicants)
spark_df.write.mode("overwrite").saveAsTable("applicants")
spark.sql("SELECT * FROM applicants").show()