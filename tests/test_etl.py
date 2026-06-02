import boto3
import pandas as pd
from io import BytesIO

s3 = boto3.client("s3")

bucket_name = "data602-final-project"

paginator = s3.get_paginator("list_objects_v2")

applicants_files = [
    file_obj["Key"]
    for page in paginator.paginate(Bucket=bucket_name)
    for file_obj in page.get("Contents", [])
    if file_obj["Key"].endswith("Applicants.csv")
]

print("Applicants files in bucket:")
for file in applicants_files:
    print(file)

dfs = []

for file in applicants_files:
    obj = s3.get_object(Bucket=bucket_name, Key=file)

    df = pd.read_csv(BytesIO(obj["Body"].read()))
    df["source_file"] = file
    dfs.append(df)

all_applicants = pd.concat(dfs, ignore_index=True)

print(all_applicants.head())
print(all_applicants.shape)
