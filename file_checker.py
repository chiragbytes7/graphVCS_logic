import boto3

def list_all_s3_files(bucket_name):
    # Initialize an S3 client
    s3 = boto3.client('s3')
    
    # List all files in the bucket
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name):
        if 'Contents' in page:
            for obj in page['Contents']:
                print(obj['Key'])
        else:
            print("nothing found")
# Example usage
bucket_name = 'neo4jvcs'
list_all_s3_files(bucket_name)