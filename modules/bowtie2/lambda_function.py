import sys, os, shutil, tempfile, boto3

import subprocess as sp
from threading import Thread
from time import time, time_ns

from common_funcs import *

# Global variables
s3_bucket = os.environ['BUCKET']
ec2 = False
tmp_Dir = ""
starttime = time_ns()

# Create S3 client
s3_client = boto3.client('s3')

#Build Bowtie2
def bowtie2(accession, tmp_fasta_dir, chr_fns):
    tmp_dir = tempfile.mkdtemp()
    cpu_count = os.cpu_count()
    print('cpu_count: ', cpu_count)

    print("\nBuilding Bowtie2...")
    print('Note: can take a while. Please be patient!')
    # Run bowtie2-build
    try:
    #run command     
        bt2Args = ["bowtie2-build", "--threads", str(cpu_count), chr_fns, 
            f'{tmp_dir}/{accession}']
        cmd = ' '.join(bt2Args)
        time_1 = time()
        
        pipe = sp.Popen(cmd, shell=True, stdout=sp.PIPE, stderr=sp.PIPE )
        
        if pipe.returncode != (None):
            print(pipe.returncode)
            raise Exception("An Error occured when trying to run \"bowtie2-build\".")    
        
        time_2 = time()
        print(f"Done. Time to build bowtie2: {(time_2-time_1)}.")
        upload_dir_to_s3(s3_client,s3_bucket,tmp_dir,f'{accession}/bowtie2')

    except Exception as e:
        shutil.rmtree(tmp_fasta_dir)
        shutil.rmtree(tmp_dir)
        print(e)
        sys.exit("Bowtie2 Build failed...")

def lambda_handler(event, context):
    args,body = recv(event)
    accession = args['Genome']

    if accession == 'fail':
        sys.exit('Error: No accession found.')
    
    # get file size of accession from s3 before download 
    filesize = s3_fasta_dir_size(s3_client,s3_bucket,os.path.join(accession,'fasta/'))
    # Check files exist
    if(filesize < 1):
        sys.exit("Error: accession file/s are missing.")

    csv_fn = 'bt2_times.csv'
    lock_key = 'bt2_lock'

    # Create new threads
    thread1 = Thread(target=thread_task, args=(accession,context,filesize,s3_client,s3_bucket,csv_fn,lock_key))
    thread1.daemon = True
    thread1.start()
    
    # error handling if files bigger than XGB (remember bt2 files need to be stored)
    
    # download from s3 based on accession
    if not ec2:
        tmp_dir, chr_fns = s3_files_to_tmp(s3_client,s3_bucket,accession)
    else:
        tmp_dir, chr_fns = list_tmp(tmp_Dir)

    # Create Bowtie2 files
    bowtie2(accession, tmp_dir, chr_fns)
    
    # Successful exec of bowtie, write success to s3
    s3_success(s3_client,s3_bucket,accession,"bt2",body)

    # Add run to s3 csv for logging
    s3_csv_append(s3_client,s3_bucket,accession,filesize,(time_ns()-starttime)*1e-9,csv_fn,lock_key)

    #close temp fasta file directory
    if os.path.exists(tmp_dir) and not ec2:
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