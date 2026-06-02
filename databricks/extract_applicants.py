import boto3
import pandas as pd
from io import BytesIO
from pprint import pprint as pp
 
s3 = boto3.client('s3')
 
bucket_name = 'data602-final-project'
 
response = s3.list_objects_v2(Bucket = bucket_name)
 
pp(response)
 
print(len(response.get('Contents', [])))

applicants_files = [
    obj['key'] # Extract the key from the S3 object
    for obj in paginator.paginate(Bucket=bucket_name)
    for obj in obj.get("Contents", [])
    if obj['Key'].endswith('Applicants.csv') # Filter for objects that end with 'Applicants.csv'
]
 
 
print("Applicants files in bucket:")
for file in applicants_files:
    print(file) # Print the key of each file that matches the filter
 
dfs = []
 
for file in applicants_files:
    obj = s3.get_object(
        Bucket = bucket_name,
        Key = file
    )
    df = pd.read_csv(BytesIO(obj['Body'].read())) # Read the CSV file into a DataFrame
    df['Source_file'] = file # Add a new column to the DataFrame with the source file name
    dfs.append(df)
   
all_applicants = pd.concat(dfs, ignore_index = True) # Concatenate all the DataFrames into a single DataFrame and ignore the index
 
print(all_applicants.head())