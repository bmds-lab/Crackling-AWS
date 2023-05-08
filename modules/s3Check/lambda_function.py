import urllib.parse, boto3, os, json

from pathlib import Path
from botocore.exceptions import ClientError

from common_funcs import *

s3_log_bucket = os.environ['LOG_BUCKET']

s3_client = boto3.client('s3')

QUEUE = os.environ['QUEUE']

def filetest(s3_bucket,key):
    try:
        file_content = s3_client.get_object(Bucket=s3_bucket, Key=key)["Body"].read()
        return file_content
    except ClientError:
        return ""

def create_multiple_logs(content, context, name):
    json_string = json.loads(content)
    create_log(s3_client, s3_log_bucket, context, json_string['Genome'] , json_string['Sequence'], json_string['JobID'], name)

def lambda_handler(event, context):
    # get bucket
    bucket = event['Records'][0]['s3']['bucket']['name']
    # break key into components
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    try:
        accession = "/".join(Path(key).parts[:-1])
        issltest = os.path.join(accession,'issl.notif')
        bt2test = os.path.join(accession,'bt2.notif')
        file_content = ""

        # test what bucket key is and if other lambda has finished
        if "bt2" in key:
            print(f"\"{bt2test}\" exists.\nTesting to see if \"{issltest}\" is present.")
            #store lambda id for event matching bt2 key
            create_multiple_logs(filetest(bucket,bt2test), context, 'S3CheckBt2')
            file_content = filetest(bucket,issltest)
        elif "issl" in key:
            print(f"\"{issltest}\" exists.\nTesting to see if \"{bt2test}\" is present.")
            #store lambda id for event matching issl key
            create_multiple_logs(filetest(bucket,issltest), context, 'S3CheckIssl')
            file_content = filetest(bucket,bt2test)
        
        # if both lambdas have finished, Send and SQS message
        if(len(file_content)>0):
            print("Both Lambdas have finished.")
            s3_client.put_object(
                Body=file_content,
                Bucket=bucket,
                Key=os.path.join(accession,'success')
            )
            # clean-up files
            s3_delete(s3_client,bucket,issltest)
            s3_delete(s3_client,bucket,bt2test)

            msg = file_content.decode("utf-8")
            print(f"Sending message to SQS: {msg}")
            QUEUE = os.getenv('QUEUE')
            sendSQS(QUEUE,msg)
        else:
            print("Waiting on other lambda function to finish.\n")

    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e