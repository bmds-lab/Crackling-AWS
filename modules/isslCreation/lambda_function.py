import sys
import re
import os
import shutil
import zipfile
import tempfile
import json
import boto3
import subprocess as sp
from threading import Thread
from botocore.exceptions import ClientError, ParamValidationError

try:
    from common_funcs import *
except:
    sys.path.insert(0, '/config/common_funcs/python')
    from common_funcs import *

try:
    sys.path.insert(0, '/opt/python/crackling/utils/')
    import extractOfftargets
except:
    import extractOfftargets

from time import time,time_ns, sleep

from datetime import datetime

# try:
s3_bucket = os.environ['BUCKET']
# except:
#     s3_bucket = 'macktest'
    
# Create S3 client
s3_client = boto3.client('s3')

starttime = time_ns()

# Build isslIndex
def isslcreate(accession, chr_fns, tmp_fasta_dir):
    print("\nExtracting Offtargets...")
    
#     # print(os.listdir('/opt'))
#     # print(os.listdir('/opt/ISSL'))

#     # extract offtarget command
    tmp_dir = tempfile.mkdtemp()
    # offtargetPath = f"{tmp_dir}/genome"
    offtargetfn = os.path.join(tmp_dir,f"{accession}.offtargets")
    # Convert csv string to ssv
    files = chr_fns.split(',')

#     extractArgs = f"/opt/python/crackling/utils/extractOfftargets.py {offtargetPath} {chr_fns}"
   
    # if __name__== "__main__":
    #     extactScript = "/config/genomes/extractOfftargets.py"
    #     isslBin = "isslCreateIndex" 
    # else:
    #     extactScript = "/opt/python/crackling/utils/extractOfftargets.py"
    #     isslBin = "/opt/ISSL/isslCreateIndex" 
    # extractArgs = f"{extactScript} {offtargetPath} {chr_fns}"
    
    # print(extractArgs)
    
#     # https://www.reddit.com/r/Python/comments/n13zrm/can_anything_be_done_about_synchronizelock_in_38/

    # try:
    print("Extracting off target sequences...",end='')
    time_1 = time()
    try:
        extractOfftargets.startSequentalprocessing(files,offtargetfn,1,100)
        isslBin = "/opt/ISSL/isslCreateIndex"
    except:
        import multiprocessing
        mpPool = multiprocessing.Pool(os.cpu_count())
        extractOfftargets.startMultiprocessing(files,offtargetfn,mpPool,1,400)
        isslBin = "/ec2Code/isslCreateIndex"
    time_2 = time()
    print(f"Done. Time to extract offtargets: {(time_2-time_1)}.",
    '\n\nRunning: createIsslIndex... ')


    issl_path = os.path.join(tmp_dir,f"{accession}.issl")
    isslArgs = [isslBin, offtargetfn, '20', '8', issl_path]
    isslArgs = ' '.join(isslArgs)
    
    time_1 = time()
    # run isslcreation
    print(isslArgs)
    os.system(isslArgs)
    time_2 = time()
    print(f"\n\nTime to create issl index: {(time_2-time_1)}.\n")
    # except:
    #     shutil.rmtree(offtargetPath)

    upload_dir_to_s3(s3_client,s3_bucket,tmp_dir,f'{accession}/issl')


# def recv(event):
#     message = 'fail'
#     for record in event['Records']:
#         try:
#             message = record['body']["Genome"]
#             # receipt_handle = record["receiptHandle"]
#         except Exception as e:
#             print(f"Exception: {e}")
#             continue
    
#     args = {
#         'accession': message, 
#         'bowtie2_build': False, 
#         'issl_idx': False
#     }
    
#     return args,record['body']

def lambda_handler(event, context):
    print(event)
    args,body = recv(event)
    print(args)
    print(s3_bucket)
    accession = args['Genome']
    print(accession)
    if accession == 'fail':
        sys.exit('big rip')
    
    # get file size of accession from s3 before download 
    filesize = s3_dir_size(s3_client,s3_bucket,os.path.join(accession,'fasta/'))
    # filesize = 100
    # Check files exist
    if(filesize < 1):
        sys.exit("Accession file/s are missing.")

    
    csv_fn = 'issl_times.csv'
    lock_key = 'issl_lock'
    # Create new thread for time to monitor debug limit
    thread1 = Thread(target=thread_task, args=(accession,context,filesize,s3_client,s3_bucket,csv_fn,lock_key))
    thread1.daemon = True
    thread1.start()

    # download from s3 based on accession
    tmp_dir, chr_fns = s3_accession_to_tmp(s3_client,s3_bucket,accession)

    # Create issl files
    isslcreate(accession, chr_fns, tmp_dir)

    # Successful exec of bowtie, write success to s3
    s3_success(s3_client,s3_bucket,accession,"issl",body)

    s3_csv_append(s3_client,s3_bucket,accession,filesize,(time_ns()-starttime)*1e-9,csv_fn,lock_key)

    #close temp fasta file directory
    if os.path.exists(tmp_dir):
        shutil.rmtree(tmp_dir)

def ec2_start(s3_Client, event, context):
    global s3_client
    s3_client = s3_Client
    return lambda_handler(event, context)

if __name__== "__main__":
    event, context = main()
    lambda_handler(event, context)