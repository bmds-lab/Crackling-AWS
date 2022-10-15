from genericpath import isfile
import os, re, shutil, tempfile, boto3, json
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

def get_tmp_dir(ec2=False):
    if ec2:
        return tempfile.mkdtemp(dir="/data")
    else:
        return tempfile.mkdtemp()

def get_named_tmp_file(ec2=False):
    if ec2:
        return tempfile.NamedTemporaryFile(dir="/data")
    else:
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
        #write files to memory
        f = open(file.name, 'wb')
        f.write(file_content)
        f.close()
        
        f = open(file.name, 'a')
        f.write(string)
        f.close()
        
        s3_client.upload_file(file.name, s3_bucket, csv_fn)
        s3_unlock(s3_client,s3_bucket,lock_key)
        time_2 = time()
        print(f"Appending to csv: \"{string}\"")
        print(f'Time to append s3file: {(time_2-time_1)} sec.')
    file.close()

def s3_fasta_dir_size(s3_client,s3_bucket,path):
    filesize = 0
    paginator = s3_client.get_paginator("list_objects_v2")
    response = paginator.paginate(Bucket=s3_bucket, Prefix=path, 
        PaginationConfig={"PageSize": 1000})
    for page in response:
        files = page.get("Contents")
        for file in files:
            if ".fa" in file['Key']:
                filesize += file['Size']
    return filesize

def s3_files_to_tmp(s3_client,s3_bucket,accession,suffix=".fa"):
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

def list_tmp(tmp_dir):
    print("Files in tmp directory: ",tmp_dir)
    names = []
    tmpArr = os.listdir(tmp_dir)
    for filename in tmpArr:
        name = re.search(r'([^\/]+$)',filename).group(0)
        name = f"{tmp_dir}/{name}"
        names.append(name)
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
    delay = context.get_remaining_time_in_millis()*.995-(s3_lock_attempts*lock_delay*1000)
    delay = 0 if delay < 0 else delay
    sleep(delay)
    testtime = time_ns()
    string = f'Your out of touch I\'m out of time(exec time > {testtime - starttime})'
    s3_csv_append(s3_client, s3_bucket, accession, filesize, string, csv_fn, lock_key)

def sendSQS(sqsURL,msg):
    sqs = boto3.client('sqs')
    sqs.send_message(
        QueueUrl=sqsURL,
        MessageBody=msg
    )

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