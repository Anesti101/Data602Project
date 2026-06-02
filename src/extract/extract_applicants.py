import boto3
import pandas as pd
from io import BytesIO
from pprint import pprint as pp

s3 = boto3.client("s3")

bucket_name = "data602-final-project"

paginator = s3.get_paginator("list_objects_v2")


applicants_files = [
    obj["Key"]  # Extract the key from the S3 object
    for obj in paginator.paginate(Bucket=bucket_name)
    for obj in obj.get("Contents", [])
    if obj["Key"].endswith(
        "Applicants.csv"
    )  # Filter for objects that end with 'Applicants.csv'
]

dfs = []

for file in applicants_files:
    obj = s3.get_object(Bucket=bucket_name, Key=file)
    df = pd.read_csv(BytesIO(obj["Body"].read()))  # Read the CSV file into a DataFrame
    df["Source_file"] = (
        file  # Add a new column to the DataFrame with the source file name
    )
    dfs.append(df)

all_applicants = pd.concat(
    dfs, ignore_index=True
)  # Concatenate all the DataFrames into a single DataFrame and ignore the index

print(all_applicants.head())
