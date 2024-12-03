from genericpath import isfile
import os, re, shutil, tempfile, boto3, json, sys
from unicodedata import name

from time import time, time_ns, sleep
from datetime import datetime
from botocore.exceptions import ClientError

# Time variable for faux-context
starttime = time_ns()
timeout = 10800

def get_tmp_dir():
    return tempfile.mkdtemp()

##########################################################
def s3_fna_dir_size(s3_client,s3_bucket,path):
    filesize = 0
    paginator = s3_client.get_paginator("list_objects_v2")
    response = paginator.paginate(Bucket=s3_bucket, Prefix=path, 
        PaginationConfig={"PageSize": 1000})
    for page in response:
        files = page.get("Contents")
        if files is None:
            return 0
        for file in files:
            if ".fna" in file['Key']:
                filesize += file['Size']
    return filesize

#############################################################



#gets the size of a file, stackoverflow: questions/5315603
def s3_get_file_size(s3_client, s3_bucket, path):
    response = s3_client.head_object(Bucket = s3_bucket, Key = path)
    filesize = response['ContentLength']
    return filesize

# Upload directory of files to S3 bucket
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
    print("Cleaning Up...")
    shutil.rmtree(path)
     
def s3_object_exists(s3_client, s3_bucket, key):
    try:
        s3_client.head_object(Bucket=s3_bucket, Key=key)
        return True
    except s3_client.exceptions.NoSuchKey:
        return False
    except s3_client.exceptions.NoSuchBucket:
        return False
    except Exception as e:
        return False

# determine if all wanted genome issl files exist
def issl_files_exist_s3(s3_client, s3_bucket, accession_list):
    # expected files exist
    for accession in accession_list:
        file_to_expect = f"{accession}.issl"
        key = f"{accession}/issl/{file_to_expect}"
        #any file missing means failure
        if not s3_object_exists(s3_client, s3_bucket, key):
            return False
    return True

# Provide list of files to check if they exist in a directory
def files_exist_s3_dir(s3_client, s3_bucket, s3_path, files_to_expect):
    # expected files exist
    for file in files_to_expect:
        key = f"{s3_path}/{file}"
        #any file missing means failure
        if not s3_object_exists(s3_client, s3_bucket, key):
            return False
    return True

# Send SQS message
def sqs_send_message(sqsURL, msg):
    sqs = boto3.client('sqs')
    sqs.send_message(
        QueueUrl=sqsURL,
        MessageBody=msg
    )

# Convert event to python dictionary
def recv(event):
    for record in event['Records']:
        try:
            json_obj =json.loads(record['body'])
        except Exception as e:
            print(f"Exception: {e}")
            json_obj="fail"
            continue
    
    return json_obj,record['body']

# Enables modules to be ran on a local development machine, instead of AWS Lambda
def local_lambda_invocation(genome,sequence,jobid):
    dictionary ={ 
        "Genome": genome, 
        "Sequence": sequence, 
        "JobID": jobid
    }
    json_object = json.dumps(dictionary)
    event = { "Records": [{"body":json_object,}] }

    class Contextsim:
        def __init__(self):
            self.data = []
        def get_remaining_time_in_millis(self):
            return (timeout*1000) - ((time_ns()-starttime) // 1_000_000)
    context = Contextsim()
    print(context.get_remaining_time_in_millis())

    return event, context

# perform an action on a job object from dydb, then perform a set on dydb in a threadsafe manner
def set_job_table(dynamoDbClient, tableName, action, jobID):
    # keep trying to add data too db until 
    from boto3.dynamodb.conditions import Attr
    table = dynamoDbClient.Table(tableName)

    while True:    
        #get current job
        job = table.get_item(Key={"JobID" : str(jobID)})['Item']

        # get current version, then increment to next version
        currentVersion = job["Version"]
        job["Version"] += 1

        # perform some action on the job object
        job = action(job)

        try:
            # Attempt to update the DB, if job has been overwritten since it was
            # retrieved, this statement will error, and we can get data again and 
            # start from beginning
            table.put_item(
                Item=job,
                ConditionExpression=Attr("Version").eq(currentVersion)
            )

            return job # return the up to date job

        except ClientError as err:
            #data has been access since fetched, keep looping
            if err.response["Error"]["Code"] != 'ConditionalCheckFailedException':
                # if the error isn't a result of concurrent access, raise it
                    raise err


# Thread safe function to set the total number of tasks (to be completed) in jobs table
def set_task_total(dynamoDbClient, tableName, jobID, taskCount):

    def setNumGuides(job):
        job["NumGuides"] = taskCount
        return job
    
    return set_job_table(dynamoDbClient, tableName, setNumGuides, jobID)


# Thread safe function to update the task counter in jobs table
def update_task_counter(dynamoDbClient, tableName, jobID, field, taskCount):
    
    def updateCompletedTasks(job):    
        job[field] += taskCount
        return job
    
    return set_job_table(dynamoDbClient, tableName, updateCompletedTasks, jobID)
