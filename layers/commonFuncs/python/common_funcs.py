import boto3
import os
import io
import re
import shutil
import tempfile, json

from time import time, time_ns, sleep
from datetime import datetime
from botocore.exceptions import ClientError

starttime = time_ns()

# try:
#     s3_bucket = os.environ['BUCKET']
# except:
#     s3_bucket = 'macktest'

# main
timeout = 900
# s3_try_lock
s3_lock_attempts = 15
lock_delay = 0.15

def s3_try_lock(s3_client,s3_bucket,key):
    count = 0
    while count<s3_lock_attempts:
        try:
            s3_client.head_object(Bucket=s3_bucket, Key=key)
        except ClientError:
            s3_client.put_object(
                Bucket=s3_bucket,
                Key=key
            )
            return True
        count +=1
        sleep(lock_delay)
    raise RuntimeError("s3 csv file locked") 

def s3_delete(s3_client,bucket,key):
    s3_client.delete_object(
        Bucket=bucket,
        Key=key
    )

def s3_unlock(s3_client,s3_bucket,key):
    s3_delete(s3_client,s3_bucket,key)

def s3_success(s3_client,s3_bucket,accession,key,body):
    key = f'{key}.notif'
    print(f"s3 success. Creating {key}.")
    s3_client.put_object(
        Body=body.encode('ascii'),
        Bucket=s3_bucket,
        Key=os.path.join(accession,key)
    )

def create_csv_if_not_exist(s3_client, s3_bucket, filename):
    try:
        s3_client.head_object(Bucket=s3_bucket, Key=filename)
    except ClientError:
        file = tempfile.NamedTemporaryFile()
        f = open(file.name, 'a')
        f.write("datetime,genome,filesize,downloadtime")
        f.close()
        s3_client.upload_file(file.name, s3_bucket, filename)
        file.close()

def s3_csv_append(s3_client,s3_bucket,accession,filesize,Time,csv_fn,lock_key):
    str = f'\n{datetime.now()},{accession},{filesize},{Time}'
    file = tempfile.NamedTemporaryFile()
    
    #file lock
    time_1 = time()
    if s3_try_lock(s3_client,s3_bucket,lock_key):
        #create csv file if not exists
        try:
            create_csv_if_not_exist(s3_client,s3_bucket,csv_fn)
        except Exception as e:
            print(f"{type(e)}: {e}")
            file.close()
            s3_unlock(s3_client, lock_key)
            return
        #download file from s3    
        file_content = s3_client.get_object(
            Bucket=s3_bucket, Key=csv_fn)["Body"].read()
        #write files to memory
        f = open(file.name, 'wb')
        f.write(file_content)
        f.close()
        
        f = open(file.name, 'a')
        f.write(str)
        f.close()
        
        s3_client.upload_file(file.name, s3_bucket, csv_fn)
        s3_unlock(s3_client,s3_bucket,lock_key)
        time_2 = time()
        print(f'\nAppending to csv:{str}\n')
        print(f'Time to append s3file: {(time_2-time_1)} sec.')
    file.close()

def s3_dir_size(s3_client,s3_bucket,path):
    filesize = 0
    paginator = s3_client.get_paginator("list_objects_v2")
    response = paginator.paginate(Bucket=s3_bucket, Prefix=path, 
        PaginationConfig={"PageSize": 1000})
    # print(response[0])
    for page in response:
        # print("getting 2 files from S3")
        files = page.get("Contents")
        for file in files:
            # print(f"file_name: {file['Key']}, size: {file['Size']}")
            if ".fa" in file['Key']:
                filesize += file['Size']
    return filesize

def s3_accession_to_tmp(s3_client,s3_bucket,accession,suffix=".fa"):
    tmpArr = tempfile.mkdtemp()
    names = []
    print(s3_bucket)
    paginator = s3_client.get_paginator("list_objects_v2")
    response = paginator.paginate(Bucket=s3_bucket, Prefix=accession, 
        PaginationConfig={"PageSize": 1000})
    # print(response[0])
    for page in response:
        # print("getting 2 files from S3")
        files = page.get("Contents")
        for file in files:
            # print(f"file_name: {file['Key']}, size: {file['Size']}")
            filename = file['Key']
            if suffix in filename:
                # get file name
                name = re.search(r'([^\/]+$)',filename).group(0)
                name = f"{tmpArr}/{name}"
                names.append(name)
                #download file from s3    
                file_content = s3_client.get_object(
                    Bucket=s3_bucket, Key=filename)["Body"].read()
                #write files to memory
                f = open(name, 'wb') #tempfile.NamedTemporaryFile()
                f.write(file_content)
                f.close()
                print(f"Downloaded to: {name}")
    print("tmpArr:",tmpArr)
    return tmpArr, ','.join(names)

def upload_dir_to_s3(s3_client,s3_bucket,path,s3_folder):
    #upload files individually to s3
    files = os.listdir(path)
    for file in files:
        print(f'Uploading: \"{file}\"...',end="")
        #Add directory structure to string name
        name = f"{s3_folder}/{file}"
        #upload to s3
        s3_client.upload_file(f'{path}/{file}', s3_bucket, name)
        print(" Done.")
    # close temp directory
    shutil.rmtree(path)

def thread_task(accession, context, filesize, s3_client, s3_bucket, csv_fn, lock_key):
    testtime = time_ns()
    print("context.get_remaining_time_in_millis()",context.get_remaining_time_in_millis())
    delay = context.get_remaining_time_in_millis()*.995-(s3_lock_attempts*lock_delay*1000)
    print("delay",delay)
    delay = 0 if delay < 0 else delay
    sleep(delay)
    testtime = time_ns()
    str = f'Your out of touch I\'m out of time(exec time > {testtime - starttime})'
    s3_csv_append(s3_client, s3_bucket, accession, filesize, str, csv_fn, lock_key)

def sendSQS(sqsURL,msg):
    sqs = boto3.client('sqs')
    sqs.send_message(
        QueueUrl=sqsURL,
        MessageBody=msg
    )
# https://boto3.amazonaws.com/v1/documentation/api/latest/guide/sqs.html
#     queue.send_message(MessageBody='boto3', MessageAttributes={
#     'Author': {
#         'StringValue': 'Daniel',
#         'DataType': 'String'
#     }
# })

def recv(event):
    for record in event['Records']:
        try:
            json_obj =json.loads(record['body'])
        except Exception as e:
            print(f"Exception: {e}")
            json_obj="fail"
            continue
    
    return json_obj,record['body']

def main(accession):
    print('This should not run on a Lambda, this is for testing on local systems.')
    event = {
        "Records": [
            {
            "messageId": "b688ac38-d0da-462f-a99d-4a89c94edbd0",
            "receiptHandle": "AQEBgG2u6GKrt3InnHQBhZJf4i2MUD9pApPwYXsm3ZTZIBDxMuU9x7T7dJIauSSo8Cc5xJrUx7kSV46f0X7Hg3BKwnxMgvN8P6ZNvxHCI+NSFOgX+M493A8+IqGnx0zTwyJkiB267tmqGyBOgM1S7mSYpYLMsrvFpt0+zJxuFlenEa7TlYSpBPIcBwDpLUpsJUealusXOmszrb+86pXMhkuxiTD/glkLg1V3+w1RZ0PZEPFir4lrU0lWEX3IvEfZHLE9DG2TE9QtNRjQ8Fhh//3GtHY7lPrUqgI/KK5JrYhrLl9uCF5jilQgA3gW9ut9VsrJBEcCC0pc25Tb1br5ZQRuvBRUthPx5fsf3d9+C7aiIfmG4Y6AZ1Kwt6iyCQV9i8+6EvPqBaHRs0oQ9iP2uryZjw==",
            "body": accession,
            "attributes": {
                "ApproximateReceiveCount": "1",
                "SentTimestamp": "1660624595423",
                "SenderId": "AROAVPURMR73HNIE3X3NN:BenchmarkLambda",
                "ApproximateFirstReceiveTimestamp": "1660624595443"
            },
            "messageAttributes": {},
            "md5OfBody": "034039ceaa0f1a83717103149a1ff7ca",
            "eventSource": "aws:sqs",
            "eventSourceARN": "arn:aws:sqs:ap-southeast-2:377188290550:BenchmarkPrepQueue",
            "awsRegion": "ap-southeast-2"
            }
        ]
    }
    class Contextsim:
        def __init__(self):
            self.data = []
        def get_remaining_time_in_millis(self):
            return (timeout*1000) - ((time_ns()-starttime) // 1_000_000)
    context = Contextsim()
    print(context.get_remaining_time_in_millis())

    dataset_bin = '/config/genomes/datasets'

    return event, context
    # lambda_handler(event, context)