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
TARGET_SCAN_QUEUE = os.environ['QUEUE']
EFS_MOUNT_PATH = os.environ['EFS_MOUNT_PATH']
#byte -> megabyte magnitude
BYTE_TO_MB_DIVIDER = 1048576
#max fasta file size - current issue with efs throughput causing a 11x duration (high lambda costs if scaled to 1000 target guides)
CUT_OFF_MB = 100
    
# Create S3 client
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')


def fasta_size_check(accession):

    # get file size of accession from s3 before download 
    filesize = s3_fasta_dir_size(s3_client,s3_bucket,os.path.join(accession,'fasta/'))

    # Check files exist
    if(filesize < 1):
        sys.exit("Error - Accession file is missing.")

    filesize_in_MB = filesize/BYTE_TO_MB_DIVIDER
    print(filesize_in_MB)

    # notImplemented -  (requires Carl's issl split implemention of CracklingPlusPlus)
    # Details - the memory bottleneck is reached at CUT_OFF_MB (600-650) due to file being written on memory.
    # It takes 10 minutes to construct at the CUT_OFF_MB fasta size and lambda has a limit of 15 minutes.
    if (filesize_in_MB > CUT_OFF_MB):
        sys.exit("Error - Accession file is larger than function can handle (memory bottleneck) - 24/09/2023")

    return filesize_in_MB

def s3_file_to_tmp(s3_client, s3_bucket, accession):

    fasta_file_name = f"{accession}.fa"

    # use temp directory and file name for future file writing
    tmp_dir = get_tmp_dir()
    tmp_dir_fasta = f"{tmp_dir}/{fasta_file_name}"

    #s3 directory where file is located
    s3_fasta_file = f"{accession}/fasta/{fasta_file_name}"

    #store fasta file in lambda's local storage
    s3_resource.meta.client.download_file(s3_bucket, s3_fasta_file, tmp_dir_fasta)
    return tmp_dir_fasta, tmp_dir


def copy_tmp_to_efs(accession, tmp_path):

    #efs directory to copy file into
    efs_destination_path = f"{EFS_MOUNT_PATH}/{accession}/issl"

    #create directory if not there
    if not os.path.exists(efs_destination_path):
        os.makedirs(efs_destination_path)

    #get files from temp directory consisting of issl and offtarget
    files = os.listdir(tmp_path)
    if files:
        for file in files:
            #file to copy
            tmp_source_file = f"{tmp_path}/{file}"
            try:
                print(f'Uploading: \"{file}\"...',end="")
                #copy file from tmp directory into efs at specfic directory
                shutil.copy2(tmp_source_file, efs_destination_path)
                print(f"File copy to: {efs_destination_path}")
            except Exception as e:
                sys.exit(f"Failure - Error copying file: {str(e)}")
    else:
        sys.exit('Failure - The expected files do not exist.')


# Build isslIndex
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

    # copy issl and offtarget files into AWS Elastic File Storage (EFS)
    copy_tmp_to_efs(accession, tmp_dir)


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
    
    #check that file size meets current limitations - 600MB file
    _ = fasta_size_check(accession)

    # download from s3 based on accession
    tmp_dir_fasta, tmp_dir = s3_file_to_tmp(s3_client, s3_bucket, accession)

    # Create issl files
    isslcreate(accession, tmp_dir_fasta)

    sendSQS(TARGET_SCAN_QUEUE, json_object) 
    
    create_log(s3_client, s3_log_bucket, context, accession, jobid, 'IsslCreation')

    #close temp fasta file directory
    if os.path.exists(tmp_dir):
        print("Cleaning Up...")
        shutil.rmtree(tmp_dir)

    print("All Done... Terminating Program.")

if __name__== "__main__":
    event, context = main()
    lambda_handler(event, context)