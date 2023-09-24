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
EFS_MOUNT_PATH = os.environ['EFS_MOUNT_PATH']

#tmp_Dir = ""
#starttime = time_ns()
    
# Create S3 client
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')


def s3_file_to_ephemeral(s3_client, s3_bucket, accession):
    tmp_dir = get_tmp_dir()
    fasta_file = f"{accession}.fa"
    s3_fasta_file = f"{accession}/fasta/{fasta_file}"
    tmp_dir_fasta = f"{tmp_dir}/{fasta_file}"
    #store fasta file in lambda's local storage
    s3_resource.meta.client.download_file(s3_bucket, s3_fasta_file, tmp_dir_fasta)
    return tmp_dir_fasta, tmp_dir

def upload_dir_to_efs(path, genome_path):
    files = os.listdir(path)
    for file in files:
        tmp_path_file = f"{path}/{file}"
        #print(tmp_path)
        #/tmp/tmpg5e1z4s2/GCA_000482205.1.issl
        print(f'Uploading: \"{file}\"...',end="")
        destination_path = f"{EFS_MOUNT_PATH}/{genome_path}"
        
        if not os.path.exists(destination_path):
            os.makedirs(destination_path)
        try:
            #/efs/GCA_000482205.1.issl
            shutil.copy2(tmp_path_file, destination_path)
            print(f"File moved to: {destination_path}")
        except Exception as e:
            print(f"Error moving file: {str(e)}")
    # close temp directory
    shutil.rmtree(path)


# Build isslIndex
#def isslcreate(accession, chr_fns, tmp_fasta_dir):
def isslcreate(accession, tmp_fasta_dir):
    
    print("\nExtracting Offtargets...")

    # extract offtarget command
    tmp_dir = get_tmp_dir()
    offtargetfn = os.path.join(tmp_dir,f"{accession}.offtargets")
    print(f"Creating: {offtargetfn}")

    # Extract offtargets
    print("Extracting off target sequences...",end='')
    time_1 = time()

    # Lambda code
    extractOfftargets.startSequentalprocessing([tmp_fasta_dir],offtargetfn,1,100)
    isslBin = "/opt/ISSL/isslCreateIndex"
   
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
    #upload_dir_to_s3(s3_client,s3_bucket,tmp_dir,f'{accession}/issl')
    # Upload iss and offtarget files to AWS Elastic File Storage (EFS)
    upload_dir_to_efs(tmp_dir, f"{accession}/issl")


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
    if(filesize < 1):
        sys.exit("Accession file/s are missing.")

    print(filesize)
    # CURRENT CONFIG DETAILS (requires Carl's issl split implemention of CracklingPlusPlus)
    CUT_OFF = 600 #in MegaBytes
    if (filesize/1048576 > CUT_OFF):
        sys.exit("Accession file is larger than function can handle (memory bottleneck) - 24/09/2023")

    #csv_fn = 'issl_times.csv'
    #lock_key = 'issl_lock'

    # Create new thread for time to monitor debug limit
    #thread1 = Thread(target=thread_task, args=(accession,context,filesize,s3_client,s3_bucket,csv_fn,lock_key))
    #thread1.daemon = True
    #thread1.start()

    # download from s3 based on accession
    tmp_dir_fasta, tmp_dir = s3_file_to_ephemeral(s3_client, s3_bucket, accession)
    #tmp_dir, chr_fns = s3_files_to_tmp(s3_client,s3_bucket,accession)

    # Create issl files
    #isslcreate(accession, chr_fns, tmp_dir)
    isslcreate(accession, tmp_dir_fasta)

    # Successful exec of bowtie, write success to s3
    #s3_success(s3_client,s3_bucket,accession,body)
    print("SEND TO SQS TARGET SCAN - SQS NOT IMPLEMENTED")

    # Add run to s3 csv for logging
    #s3_csv_append(s3_client,s3_bucket,accession,filesize,(time_ns()-starttime)*1e-9,csv_fn,lock_key)
    
    create_log(s3_client, s3_log_bucket, context, accession, jobid, 'IsslCreation')

    #close temp fasta file directory
    if os.path.exists(tmp_dir):
        print("Cleaning Up...")
        shutil.rmtree(tmp_dir)

    print("All Done... Terminating Program.")

if __name__== "__main__":
    event, context = main()
    lambda_handler(event, context)