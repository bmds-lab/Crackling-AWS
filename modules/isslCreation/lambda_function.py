import sys, os, shutil, boto3
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
#byte - megabyte magnitude
BYTE_TO_MB_DIVIDER = 1048576
#max fasta file size
CUT_OFF_MB = 650
    
# Create S3 client
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

#determine if fasta file exists and return its size
def fasta_size_check(accession):
    #filesize = s3_fasta_dir_size(s3_client,s3_bucket,os.path.join(accession,'fasta/')) ## see if function is used elsewhere 
    filesize = s3_fna_dir_size(s3_client,s3_bucket,os.path.join(accession,'fasta/'))
    if(filesize < 1):
        print(s3_client)
        print(s3_bucket)
        print("This is the filesize")
        print(filesize)
        sys.exit("Error - Accession file is missing.")
    filesize_in_MB = filesize/BYTE_TO_MB_DIVIDER

    # notImplemented -  (requires Carl's issl split implemention of CracklingPlusPlus)
    # Details - the memory bottleneck is reached at CUT_OFF_MB (600-650) due to file being written on memory.
    # It takes 10 minutes to construct at the CUT_OFF_MB fasta size and lambda has a limit of 15 minutes.
    print(filesize_in_MB)
    if (filesize_in_MB > CUT_OFF_MB):
        sys.exit("Error - Accession file is larger than function can handle (memory bottleneck) - 24/09/2023")
    return filesize_in_MB



# Downloads and unzips multiple fasta files from S3 bucket 
def s3_multi_file_to_tmp(s3_client, s3_bucket, accession):

    prefix = f"{accession}/fasta/"
    paginator = s3_client.get_paginator('list_objects_v2')
    response_iterator = paginator.paginate(Bucket=s3_bucket, Prefix=prefix)

    downloaded_files = []

    # Extract all .fna file names for genome accession 
    for page in response_iterator:
        files = [obj['Key'] for obj in page.get('Contents', [])]

        for s3_file_path in files:
            file_name = os.path.basename(s3_file_path)
            print(file_name)
            downloaded_files.append(file_name)

    # Temp folder for zipped files 
    tmp_dir = get_tmp_dir()
    # Temp folder for unzipped files 
    tmp_extract_dir = get_tmp_dir()
    # unzipped .fna file names 
    extracted_files = []

    for file in downloaded_files:

        fasta_file_name = file
        s3_full_filepath = f"{accession}/fasta/{fasta_file_name}"
        print(f"This is the fasta file name {fasta_file_name}")

        # Use temp directory for file writing in local
        tmp_gz_file = os.path.join(tmp_dir, fasta_file_name)
         
        # download each .fna file from S3
        print(f"Downloading {s3_full_filepath} to {tmp_gz_file}")
        s3_client.download_file(s3_bucket, s3_full_filepath, tmp_gz_file)

        # Unzip the downloaded .gz file
        tmp_extract_file = os.path.join(tmp_extract_dir, os.path.splitext(fasta_file_name)[0])  # Remove .gz extension
        with gzip.open(tmp_gz_file, 'rb') as f_in:
            with open(tmp_extract_file, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        extracted_files.append(tmp_extract_file)

        os.remove(tmp_gz_file)
        print(f"Deleted temporary gz file: {tmp_gz_file}")

    # print extracted files 
    print(extracted_files)
    return extracted_files, tmp_extract_dir



# Build isslIndex
def isslcreate(accession, tmp_fasta_dir):
    
    print("\nExtracting Offtargets...")

    # extract offtarget command
    tmp_dir = get_tmp_dir()
    offtargetfn = os.path.join(tmp_dir,f"{accession}.offtargets")
    print(f"Creating: {offtargetfn}")

    # Lambda code
    extractOfftargets.startSequentalprocessing([tmp_fasta_dir], offtargetfn, 1, 100)
    isslBin = "/opt/ISSL/isslCreateIndex"

    issl_path = os.path.join(tmp_dir, f"{accession}.issl")

    os.system(f"{isslBin} {offtargetfn} 20 8 {issl_path}")

    s3_destination_path = f"{accession}/issl"
    upload_dir_to_s3(s3_client, s3_bucket, tmp_dir, s3_destination_path)


def lambda_handler(event, context):

    args,body = recv(event)
    accession = args['Genome']
    sequence = args['Sequence']
    jobid = args['JobID']

    body ={ 
        "Genome": accession, 
        "Sequence": sequence, 
        "JobID": jobid
    }
    json_object = json.dumps(body)

    if accession == 'fail':
        sys.exit('Error: No accession found.')

    print(f"accession: {accession}")
    
    #check that file size meets current limitations - 600MB file
    _ = fasta_size_check(accession)

    tmp_dir_fasta, tmp_dir = s3_multi_file_to_tmp(s3_client, s3_bucket, accession)

    # Create issl files
    isslcreate(accession, tmp_dir)

    sqs_send_message(TARGET_SCAN_QUEUE, json_object) 

    print("These are the extracted file names", tmp_dir_fasta)
    
    #close temp fasta file directory
    if os.path.exists(tmp_dir):
        print("Cleaning Up...")
        shutil.rmtree(tmp_dir)

    print("All Done... Terminating Program.")

if __name__== "__main__":
    event, context = local_lambda_invocation()
    lambda_handler(event, context)