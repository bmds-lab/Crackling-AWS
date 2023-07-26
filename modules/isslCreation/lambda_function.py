import sys, os, shutil, boto3

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
s3_log_bucket = os.environ['LOG_BUCKET']
ec2 = False
tmp_Dir = ""
starttime = time_ns()
    
# Create S3 client
s3_client = boto3.client('s3')

# Build isslIndex
def isslcreate(accession, chr_fns, tmp_fasta_dir):
    print("\nExtracting Offtargets...")

    # extract offtarget command
    tmp_dir = get_tmp_dir(ec2)
    offtargetfn = os.path.join(tmp_dir,f"{accession}.offtargets")
    print(f"Creating: {offtargetfn}")
    # Convert csv string to ssv
    files = chr_fns.split(',')

    # Extract offtargets
    print("Extracting off target sequences...",end='')
    time_1 = time()
    try:
        # Lambda code
        extractOfftargets.startSequentalprocessing(files,offtargetfn,1,100)
        isslBin = "/opt/ISSL/isslCreateIndex"
    except:
        # ec2 code
        import multiprocessing
        mpPool = multiprocessing.Pool(os.cpu_count())
        extractOfftargets.startMultiprocessing(files,offtargetfn,mpPool,os.cpu_count(),400)
        isslBin = "/ec2Code/isslCreateIndex"
    time_2 = time()

    print(f"Done. Time to extract offtargets: {(time_2-time_1)}.",
    '\n\nRunning: createIsslIndex... ')

    # Run isslcreation
    issl_path = os.path.join(tmp_dir,f"{accession}.issl")
    
    time_1 = time()
    os.system(f"{isslBin} {offtargetfn} 20 8 {issl_path}")
    time_2 = time()
    print(f"\n\nTime to create issl index: {(time_2-time_1)}.\n")

    # Upload issl and offtarget files to s3
    upload_dir_to_s3(s3_client,s3_bucket,tmp_dir,f'{accession}/issl')

def lambda_handler(event, context):
    args,body = recv(event)
    accession = args['Genome']
    sequence = args['Sequence']
    jobid = args['JobID']

    if accession == 'fail':
        sys.exit('Error: No accession found.')
    
    # get file size of accession from s3 before download 
    filesize = s3_fasta_dir_size(s3_client,s3_bucket,os.path.join(accession,'fasta/'))
    # Check files exist
    if(filesize < 1) and not ec2:
        sys.exit("Accession file/s are missing.")

    csv_fn = 'issl_times.csv'
    lock_key = 'issl_lock'

    # Create new thread for time to monitor debug limit
    thread1 = Thread(target=thread_task, args=(accession,context,filesize,s3_client,s3_bucket,csv_fn,lock_key))
    thread1.daemon = True
    thread1.start()

    # download from s3 based on accession
    if not ec2:
        tmp_dir, chr_fns = s3_files_to_tmp(s3_client,s3_bucket,accession)
    else:
        tmp_dir, chr_fns = list_tmp(tmp_Dir)

    # Create issl files
    isslcreate(accession, chr_fns, tmp_dir)

    # Successful exec of bowtie, write success to s3
    s3_success(s3_client,s3_bucket,accession,"issl",body)

    # Add run to s3 csv for logging
    s3_csv_append(s3_client,s3_bucket,accession,filesize,(time_ns()-starttime)*1e-9,csv_fn,lock_key)
    
    create_log(s3_client, s3_log_bucket, context, accession, jobid, 'IsslCreation')

    #close temp fasta file directory
    if not ec2 and os.path.exists(tmp_dir):
        print("Cleaning Up...")
        shutil.rmtree(tmp_dir)

    print("All Done... Terminating Program.")

# ec2 instance code entry and setup function
def ec2_start(s3_Client,tmp_dir, event, context):
    global s3_client
    s3_client = s3_Client
    global ec2
    ec2 = True
    global tmp_Dir
    tmp_Dir = tmp_dir
    return lambda_handler(event, context)

if __name__== "__main__":
    event, context = main()
    lambda_handler(event, context)