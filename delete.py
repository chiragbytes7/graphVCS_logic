import boto3

s3 = boto3.resource('s3')
bucket = s3.Bucket('neo4jvcs')

# This deletes all objects in the bucket
bucket.objects.all().delete()