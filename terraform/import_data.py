import boto3
import os
from botocore.config import Config
from os import walk 
from data import data

bucket = "postagram-hiba-karim-bucket20250504153442719700000001"

table = "Postgram"

# Create an S3 resource
s3 = boto3.resource('s3')
base_dir = os.path.dirname(os.path.abspath(__file__))
s3_dir = os.path.join(base_dir, 's3')


f = []
for (dirpath, dirnames, filenames) in walk(s3_dir):
    for filename in filenames:
        file_path = os.path.join(dirpath, filename)
        key = f"{'/'.join(dirpath.split(os.sep)[-2:])}/{filename}"
        print(f"Uploading: {key}")
        with open(file_path, 'rb') as file:
            s3.Object(bucket, key).put(Body=file)
# Batch upload
# Get the service resource.
# Get the service resource.
my_config = Config(
    region_name='us-east-1',
    signature_version='v4',
)

dynamodb = boto3.resource('dynamodb', config=my_config)
# Get the table.
table = dynamodb.Table(table)
# Read file
with table.batch_writer() as batch:
    for row in data:
        batch.put_item(Item=row)