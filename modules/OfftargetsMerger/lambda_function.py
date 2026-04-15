import sys, os, shutil, boto3, heapq
import zipfile, gzip
import tempfile

from threading import Thread
from botocore.exceptions import ClientError, ParamValidationError
from time import time,time_ns, sleep
from datetime import datetime

from common_funcs import *

# Global variables
s3_bucket = os.environ['BUCKET']
ISSL_QUEUE = os.environ['QUEUE']
#byte - megabyte magnitude
BYTE_TO_MB_DIVIDER = 1048576
#max fasta file size
CUT_OFF_MB = 650

SEQ_LEN = 20
BYTES_PER_SEQ = (SEQ_LEN * 2 + 7) // 8
    
# Create S3 client
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

def record_reader(fp):
    while True:
        chunk = fp.read(BYTES_PER_SEQ)
        if not chunk:
            break
        yield (int.from_bytes(chunk, "big"), chunk)

# Downloads and unzips multiple fasta files from S3 bucket 
def s3_offtargets_to_tmp(s3_client, s3_bucket, accession):

    prefix = f"{accession}/issl/"
    paginator = s3_client.get_paginator('list_objects_v2')
    response_iterator = paginator.paginate(Bucket=s3_bucket, Prefix=prefix)

    downloaded_files = []

    for page in response_iterator:
        files = [obj['Key'] for obj in page.get('Contents', [])]

        for s3_file_path in files:
            file_name = os.path.basename(s3_file_path)
            print(file_name)
            downloaded_files.append(file_name)

    # Temp folder for files 
    tmp_dir = get_tmp_dir()

    downloaded_file_paths = []

    for file in downloaded_files:
        s3_full_filepath = f"{accession}/issl/{file}"
        local_file_path = os.path.join(tmp_dir, file)

        print(f"Downloading {s3_full_filepath} to {local_file_path}")
        s3_client.download_file(s3_bucket, s3_full_filepath, local_file_path)

        downloaded_file_paths.append(local_file_path)

    # Print downloaded files
    print(downloaded_file_paths)
    return downloaded_file_paths, tmp_dir



# Extract Off-targets
def performMerge(accession, tmp_offtargets_dir):
    
    print("\nMerging Offtargets...")

    tmp_dir = get_tmp_dir()
    offtargetfn = os.path.join(tmp_dir,f"{accession}.offtargets")
    print(f"Creating: {offtargetfn}")

    sortedFiles = [os.path.join(tmp_offtargets_dir, f) for f in os.listdir(tmp_offtargets_dir)]

    sortedFilesPointers = [open(f, 'rb') for f in sortedFiles]
    with open(offtargetfn, 'wb') as outFile:
        # Merge using heapq on record iterators
        merged_iter = heapq.merge(*(record_reader(file) for file in sortedFilesPointers))
        for _, record in merged_iter:
            outFile.write(record)

    for file in sortedFilesPointers:
        file.close()

    print("tmp_dir:", tmp_dir)

    s3_destination_path = f"{accession}/issl"

    print("Deleting multiple files in s3")

    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=s3_bucket, Prefix=s3_destination_path + "/"):
        if "Contents" in page:
            delete_objects = {"Objects": [{"Key": obj["Key"]} for obj in page["Contents"]]}
            s3_client.delete_objects(Bucket=s3_bucket, Delete=delete_objects)

    print("Files in tmp_dir before upload:", os.listdir(tmp_dir))

    upload_dir_to_s3(s3_client, s3_bucket, tmp_dir, s3_destination_path)


def lambda_handler(event, context):

    args,body = recv(event)
    accession = args['Genome']
    sequence = args['Sequence']
    jobid = args['JobID']

    body ={ 
        "Genome": accession, 
        "Sequence": sequence, 
        "JobID": jobid,
    }
    json_object = json.dumps(body)

    if accession == 'fail':
        sys.exit('Error: No accession found.')

    print(f"accession: {accession}")

    tmp_dir_offtargets, tmp_dir = s3_offtargets_to_tmp(s3_client, s3_bucket, accession)

    performMerge(accession, tmp_dir)

    sqs_send_message(ISSL_QUEUE, json_object) 

    print("These are the extracted file names", tmp_dir_offtargets)
    
    #close temp fasta file directory
    if os.path.exists(tmp_dir):
        print("Cleaning Up...")
        shutil.rmtree(tmp_dir)

    print("All Done... Terminating Program.")

if __name__== "__main__":
    event, context = local_lambda_invocation()
    lambda_handler(event, context)