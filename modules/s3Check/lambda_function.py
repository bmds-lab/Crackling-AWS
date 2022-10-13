import urllib.parse
import boto3
import os
import re
from pathlib import Path
from botocore.exceptions import ClientError

from common_funcs import *

s3_client = boto3.client('s3')

QUEUE = os.environ['QUEUE']

def filetest(s3_bucket,key):
    try:
        # s3.head_object(Bucket=s3_bucket, Key=key)
        # return True
        file_content = s3_client.get_object(Bucket=s3_bucket, Key=key)["Body"].read()
        return file_content
    except ClientError:
        return ""

def lambda_handler(event, context):
    print(event)
    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        accession = "/".join(Path(key).parts[:-1])
        issltest = os.path.join(accession,'issl.notif')
        bt2test = os.path.join(accession,'bt2.notif')
        
        file_content = ""
        if "bt2" in key:
            print(f"\"{issltest}\" exists.\nTesting to see if \"{bt2test}\" is present.")
            file_content = filetest(bucket,issltest)
        elif "issl" in key:
            print(f"\"{bt2test}\" exists.\nTesting to see if \"{issltest}\" is present.")
            file_content = filetest(bucket,bt2test)
            
        if(len(file_content)>0):
            print("Both Lambdas have finished.")
            s3_client.put_object(
                Body=file_content,
                Bucket=bucket,
                Key=os.path.join(accession,'success')
            )
            s3_delete(s3_client,bucket,issltest)
            s3_delete(s3_client,bucket,bt2test)
            msg = file_content.decode("utf-8")
            print(f"Pushing data to SQS:{msg}")
            QUEUE = os.getenv('QUEUE')
            sendSQS(QUEUE,msg)
        else:
            print("waiting on other file/s")
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
