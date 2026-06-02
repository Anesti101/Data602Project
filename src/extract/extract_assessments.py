import boto3
import pandas as pd
from io import BytesIO

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

all_assessments = pd.DataFrame(rows)

print(all_assessments.head(20))
print(all_assessments.shape)