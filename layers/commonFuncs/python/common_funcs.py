from genericpath import isfile
import os, re, shutil, tempfile, boto3, json, sys
from unicodedata import name

from time import time, time_ns, sleep
from datetime import datetime
from botocore.exceptions import ClientError

# Time variable for faux-context
starttime = time_ns()
timeout = 10800

# s3_try_lock variables
s3_lock_attempts = 15
lock_delay = 0.15

# try to get mutex write lock, error after specified no. of attempts
def s3_try_lock(s3_client,s3_bucket,accession):
    count = 0
    while count<s3_lock_attempts:
        try:
            s3_client.head_object(Bucket=s3_bucket, Key=accession)
        except ClientError:
            s3_client.put_object(
                Bucket=s3_bucket,
                Key=accession
            )
            return True
        count +=1
        sleep(lock_delay)
    raise RuntimeError("s3 csv file locked") 

# delete s3 file
def s3_delete(s3_client,bucket,accession):
    s3_client.delete_object(
        Bucket=bucket,
        Key=accession
    )

# unlock s3 csv writing
def s3_unlock(s3_client,s3_bucket,accession):
    s3_delete(s3_client,s3_bucket,accession)

# Create .notif files for s3check module to use
def s3_success(s3_client,s3_bucket,accession,body):
    accession = f'{accession}.notif'
    print(f"s3 success. Creating {accession}.")
    s3_client.put_object(
        Body=body.encode('ascii'),
        Bucket=s3_bucket,
        Key=os.path.join(accession,accession)
    )

def get_tmp_dir():
    return tempfile.mkdtemp()

def get_named_tmp_file():
    return tempfile.NamedTemporaryFile()

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

# Add info about run to csv file for logging
def s3_csv_append(s3_client,s3_bucket,accession,filesize,Time,csv_fn,lock_key):
    string = f'\n{datetime.now()},{accession},{filesize},{Time}'
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
        #write filecontent binary data to fileobj then close
        f = open(file.name, 'wb')
        f.write(file_content)
        f.close()
        # reopen file in text appending mode and write new data
        f = open(file.name, 'a')
        f.write(string)
        f.close()
        # overwrite old file with new appended file.
        s3_client.upload_file(file.name, s3_bucket, csv_fn)
        s3_unlock(s3_client,s3_bucket,lock_key)
        time_2 = time()
        print(f"Appending to csv: \"{string}\"")
        print(f'Time to append s3file: {(time_2-time_1)} sec.')
    file.close()

# return genome size from s3 file storage
def s3_fasta_dir_size(s3_client,s3_bucket,path):
    filesize = 0
    paginator = s3_client.get_paginator("list_objects_v2")
    response = paginator.paginate(Bucket=s3_bucket, Prefix=path, 
        PaginationConfig={"PageSize": 1000})
    for page in response:
        files = page.get("Contents")
        if files is None:
            return 0
        for file in files:
            if ".fa" in file['Key']:
                filesize += file['Size']
    return filesize


#gets the size of a file, stackoverflow: questions/5315603
def s3_get_file_size(s3_client, s3_bucket, path):
    response = s3_client.head_object(Bucket = s3_bucket, Key = path)
    filesize = response['ContentLength']
    return filesize


# download fasta files from S3 bucket to tmp directory and return csv string of fasta tmp filepaths
def s3_files_to_tmp_old(s3_client,s3_bucket,accession,suffix=".fa"):
    tmpArr = tempfile.mkdtemp()
    names = []
    print(s3_bucket)
    paginator = s3_client.get_paginator("list_objects_v2")
    response = paginator.paginate(Bucket=s3_bucket, Prefix=accession, 
        PaginationConfig={"PageSize": 1000})
    for page in response:
        files = page.get("Contents")
        for file in files:
            filename = file['Key']
            if suffix in filename:
                # get file name
                name = re.search(r'([^\/]+$)',filename).group(0)
                name = f"{tmpArr}/{name}"
                names.append(name)
                #download file from s3    
                file_content = s3_client.get_object(
                    Bucket=s3_bucket, Key=filename)["Body"].read()
                # write files to tmp
                f = open(name, 'wb')
                f.write(file_content)
                f.close()
                print(f"Downloaded to: {name}")
    print("Files from s3 bucket: ",tmpArr)
    return tmpArr, ','.join(names)

# return csv string of fasta tmp filepaths
def list_tmp(tmp_dir):
    print("Files in tmp directory: ",tmp_dir)
    names = []
    tmpArr = os.listdir(tmp_dir)
    for filename in tmpArr:
        name = re.search(r'([^\/]+$)',filename).group(0)
        name = f"{tmp_dir}/{name}"
        names.append(name)
    return tmpArr, ','.join(names)

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
    

#### HELPER FUNCTIONS
def clean_s3_folder(s3_client, s3_bucket, accession):
    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        response = paginator.paginate(Bucket=s3_bucket, Prefix=accession, 
            PaginationConfig={"PageSize": 1000})
        for page in response:
            files = page.get("Contents")
            for filename in files:
                print(filename)
                s3_client.delete_object(
                    Bucket=s3_bucket,
                    Key=filename
                )
        print("cleaned-up s3 folder after download failure")
    except ParamValidationError as aa:
        print("verified clean-up of s3 folder after download failure")
    except Exception as e:
        print(f"{type(e)}: {e}")

def is_fasta_in_s3(s3_client, s3_bucket, accession):
    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        response = paginator.paginate(Bucket=s3_bucket, Prefix=accession,PaginationConfig={"PageSize": 1000})
        for page in response:
            files = page.get("Contents")
            if len(files) > 0:
                print(f"{accession} exists in s3")
                return True
            else:
                print(f"{accession} does not exist in s3")
                return False
    except Exception as e:
        print(f"{type(e)}: {e}")
        return False

def s3_object_exists(s3_client, s3_bucket, key):
    try:
        s3_client.head_object(Bucket=s3_bucket, Key=key)
        print("Success - object exists.")
        return True
    except s3_client.exceptions.NoSuchKey:
        print("Error - No such object.")
        return False
    except s3_client.exceptions.NoSuchBucket:
        sys.exit('Error - No such bucket.')
    except Exception as e:
        print("No directory exists: "+ str(e))
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


# Put log object in to s3 bucket
def create_log(s3_client, s3_log_bucket, context, genome, jobid, func_name):
    #store context of lambda log group and id for future access
    context_dict = {
        "log_group_name": context.log_group_name,
        "request_id": context.aws_request_id
    }
    context_string = json.dumps(context_dict, default=str)
    
    #upload json context based on genome chosen and jobid
    s3_client.put_object(
        Bucket = s3_log_bucket,
        Key = f'{genome}/jobs/{jobid}/{func_name}.json',
        Body = context_string
    )

# Provide list of files to check if they exist in a directory
def file_exist(path, files_to_expect):
    # expected files exist
    for file in files_to_expect:
        file_path = f"{path}/{file}"
        #any file missing means failure
        if not os.path.isfile(file_path):
            return False
    return True

# Thread task to write to csv if about to run out of execution time
def thread_task(accession, context, filesize, s3_client, s3_bucket, csv_fn, lock_key):
    testtime = time_ns()
    delay = context.get_remaining_time_in_millis()*.995-(s3_lock_attempts*lock_delay*1000)
    delay = 0 if delay < 0 else delay
    sleep(delay)
    testtime = time_ns()
    string = f'Your out of touch I\'m out of time(exec time > {testtime - starttime})'
    s3_csv_append(s3_client, s3_bucket, accession, filesize, string, csv_fn, lock_key)

# Send SQS message
def sendSQS(sqsURL,msg):
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

# Create context and event to run lambda_handlers
def main(genome,sequence,jobid):
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
def set_job_table(dynamoDbClient,tableName,action, jobID):
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


# Thread safe function too set the total number of tasks (to be completed) in jobs table
def set_task_total(dynamoDbClient, tableName, jobID, taskCount):
    def setTotalTasks(job):
        #update values
        job["TotalTasks"] = taskCount

        return job
    
    return set_job_table(dynamoDbClient, tableName, setTotalTasks, jobID)


# Thread safe function too update the task counter in jobs table
def update_task_counter(dynamoDbClient, tableName, jobID, taskCount):
    # function to increment the task counter
    def updateCompletedTasks(job):    
        #update values
        job["CompletedTasks"] += taskCount

        return job
    
    return set_job_table(dynamoDbClient, tableName, updateCompletedTasks, jobID)

# Thread safe function too set the total number of tasks (to be completed) in jobs table
def set_task_finished(dynamoDbClient, tableName, jobID):
    set_task_total(dynamoDbClient, tableName, jobID, "Completed")


# check if all of a job's tasks are completed. Takes an entry form the jobs table 
# as input (which allows data returned from "update_task_counter" to be piped in)
def spawn_notification_if_complete(dynamoDbClient, tableName,job,notification_queue_url):
    # Try and parse TotalTasks as int to confirm that job isn't "creating" or "completed"
    try:
        int(job["TotalTasks"])
    except:
        print(f"Job not initialized or is already completed. Job details:\n{job}")
        return
    
    # check if all tasks are completed
    if (job["TotalTasks"]/job["CompletedTasks"]) >= 0.54:
        print("All tasks complete, spawning a notification lambda")
        sendSQS(notification_queue_url,job["JobID"])

        # mark job as finished
        set_task_finished(dynamoDbClient, tableName, job["JobID"])