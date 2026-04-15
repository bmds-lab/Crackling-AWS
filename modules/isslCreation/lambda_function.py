import sys, os, shutil, subprocess, boto3
import zipfile, gzip
import tempfile

from threading import Thread
from botocore.exceptions import ClientError, ParamValidationError
from time import time,time_ns, sleep
from datetime import datetime

from common_funcs import *

try:
    import extractOfftargets
except:
    sys.path.insert(0, '/opt/python/crackling/utils/')
    import extractOfftargets

# Global variables
s3_bucket = os.environ['BUCKET']
TARGET_SCAN_QUEUE = os.environ['QUEUE']
REINVOKE_QUEUE = os.environ['ISSL_QUEUE']
#byte - megabyte magnitude
BYTE_TO_MB_DIVIDER = 1048576
#max fasta file size
CUT_OFF_MB = 650
    
# Create S3 client
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

def s3_offtargets_to_tmp(s3_client, s3_bucket, accession, phase):

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
 
    tmp_dir = get_tmp_dir()

    downloaded_file_paths = []

    for file in downloaded_files:
        if phase == "0" and "offtargets" in file:
            print(f"Skipping {file}")
            continue
        
        s3_full_filepath = f"{accession}/issl/{file}"
        local_file_path = os.path.join(tmp_dir, file)

        print(f"Downloading {s3_full_filepath} to {local_file_path}")
        s3_client.download_file(s3_bucket, s3_full_filepath, local_file_path)

        downloaded_file_paths.append(local_file_path)

    # Print downloaded files
    print(downloaded_file_paths)
    return downloaded_file_paths, tmp_dir



# Build isslIndex
def isslcreate(accession, tmp_offtargets_dir, phase, seqVal, sitesVal):
    
    print("\nInitialising isslcreate...")

    offtargetfn = os.path.join(tmp_offtargets_dir,f"{accession}.offtargets")

    isslBin = "/opt/ISSL/isslCreateIndex"

    tmp_dir = get_tmp_dir()

    if sitesVal == "0":
        issl_path = os.path.join(tmp_dir, f"{accession}.issl")
    else:
        issl_path = os.path.join(tmp_offtargets_dir, f"{accession}.issl")

    result = subprocess.run(
        [isslBin, offtargetfn, "20", "8", issl_path, phase, sitesVal, seqVal],
        capture_output=True,
        text=True
    )

    print(result.stderr)
    print(result.stdout)

    if result.returncode == 1:
        for line in result.stdout.splitlines():
            if line.startswith("SEQCOUNT="):
                seqVal = line.split("=")[1]
        for line in result.stdout.splitlines():
            if line.startswith("SITES="):
                sitesVal = line.split("=")[1]

    s3_destination_path = f"{accession}/issl"
    
    print("Uploading issl index to s3...")
    s3_client.upload_file(issl_path, s3_bucket, f"{s3_destination_path}/{accession}.issl")

    shutil.rmtree(tmp_dir)

    return seqVal, sitesVal, result.returncode


def lambda_handler(event, context):

    args,body = recv(event)
    accession = args['Genome']
    sequence = args['Sequence']
    jobid = args['JobID']

    if 'SequenceCount' in args:
        seqVal = args['SequenceCount']
        phase = "0"
    else:
        seqVal = "0"
        phase = "1"

    if 'Sites' in args:
        sitesVal = args['Sites']
    else:
        sitesVal = "0"

    if accession == 'fail':
        sys.exit('Error: No accession found.')

    print(f"accession: {accession}")

    tmp_dir_offtargets, tmp_dir = s3_offtargets_to_tmp(s3_client, s3_bucket, accession, phase)

    # Create issl files
    seqVal, sitesVal, isslResult = isslcreate(accession, tmp_dir, phase, seqVal, sitesVal)

    print("These are the extracted file names", tmp_dir_offtargets)
    
    #close temp fasta file directory
    if os.path.exists(tmp_dir):
        print("Cleaning Up...")
        shutil.rmtree(tmp_dir)

    print("All Done... Terminating Program.")

    if isslResult == 1:
        body ={ 
            "Genome": accession, 
            "Sequence": sequence, 
            "JobID": jobid,
            "SequenceCount": seqVal,
            "Sites": sitesVal
        }
        json_object = json.dumps(body)
        sqs_send_message(REINVOKE_QUEUE, json_object)
    else:
        body ={ 
            "Genome": accession, 
            "Sequence": sequence, 
            "JobID": jobid,
        }
        json_object = json.dumps(body)
        sqs_send_message(TARGET_SCAN_QUEUE, json_object)

if __name__== "__main__":
    event, context = local_lambda_invocation()
    lambda_handler(event, context)