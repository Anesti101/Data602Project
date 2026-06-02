import boto3
import pandas as pd
from io import BytesIO
from pprint import pprint as pp
 
s3 = boto3.client('s3')
 
bucket_name = 'data602-final-project'
 
response = s3.list_objects_v2(Bucket = bucket_name)
 
pp(response)
 