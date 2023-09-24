import sys, re, os, shutil, zipfile, boto3, json

from threading import Thread
from time import time, time_ns
from botocore.exceptions import ParamValidationError

from common_funcs import *

try:
    import ncbi.datasets
except ImportError:
    print('ncbi.datasets module not found. To install, run `pip install ncbi-datasets-pylib`.')

# Global variables
s3_bucket = os.environ['BUCKET']
s3_log_bucket = os.environ['LOG_BUCKET']
#starttime = time_ns()
TARGET_SCAN_QUEUE = os.environ['TARGET_SCAN_QUEUE']
ISSL_QUEUE = os.getenv('ISSL_QUEUE')
LIST_PREFIXES = [".issl", ".offtargets"]
EFS_MOUNT_PATH = os.environ['EFS_MOUNT_PATH']

# Create S3 client
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

#### HELPER FUNCTIONS
def clean_s3_folder(accession):
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

def is_fasta_in_s3(accession):
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

def is_issl_in_efs(accession):
    #prepare file_name for comparison
    result = False
    lambda_mapped_efs_dir = f"{EFS_MOUNT_PATH}/{accession}/issl"
   
    #get list of expected files
    files_to_expect = []
    for prefix in LIST_PREFIXES:
        files_to_expect.append(accession+prefix)

    #check files inside efs at specific genome directory
    found_files = []
    for _,_,f in os.walk(lambda_mapped_efs_dir):
        found_files.append(f)
    
    #compare existing files with expected files in efs
    if found_files:
        for file in found_files[0]:
            if (file in files_to_expect):
                result = True

    return result


# Function uses the ncbi api to download the accession data and it uploads it to AWS S3 bucket
def dl_accession(accession):

    time_1 = time()
    tmp_dir = get_tmp_dir()

    print(f'\nStarting download of {accession}.\nNote: can take a while to download. Please be patient!')

    # initalise API
    genome_api = ncbi.datasets.GenomeApi(ncbi.datasets.ApiClient())

    #download object
    dl_response = genome_api.download_assembly_package(
        accessions = [accession],
        exclude_sequence = False,
        _preload_content = False )

    #Save Zip File
    zip_file = get_named_tmp_file()
    print(zip_file.name)

    # Download zip file
    try:
        with open(zip_file.name, 'wb') as f:
            f.write(dl_response.data)
        time_2 = time()
        print(f"Time to download file: {(time_2-time_1)}.")
        print(f'Download saved to tmp_file: {zip_file.name}')
    except: 
        zip_file.close()
        return f'"Download failed..."',0,0
    
    chr_fns = []

    # Unzip file
    print("\nUnzipping file...", end='')
    try:
        with zipfile.ZipFile(zip_file.name, 'r') as zip_ref:
            # loop through files in zip file
            for file_in_zip in zip_ref.namelist():
                # Don't move chrMT files
                if 'chrMT' in file_in_zip:
                    continue
                # f file has fasta file extension
                if ".fna" in file_in_zip:
                    # open file_obj for file to extract file
                    fasta_file = zip_ref.open(file_in_zip)
                    # Get file_name without ".fna" file extension
                    name = re.search(r'([^\/]+$)',file_in_zip).group(0)[:-4]
                    #Add directory structure to string name
                    s3_name = f"{accession}/fasta/{accession}.fa"
                    print(s3_name)
                    #save name to array
                    tmp_name = f'{tmp_dir}/{name}.fa'
                    chr_fns.append(tmp_name)
                    #upload to s3
                    s3_client.upload_fileobj(fasta_file, s3_bucket, s3_name)
                    #wait until file is uploaded 
                    s3_resource.Object(s3_bucket, s3_name).wait_until_exists()
                    fasta_file.close()
                    #write file to tmp dir
                    # if (__name__== "__main__"):
                    #     fasta_file = zip_ref.open(file_in_zip)
                    #     f = open(tmp_name,'wb')
                    #     f.write(fasta_file.read())
                    #     f.close()
                    #     fasta_file.close()
            print(" Done.")
    except Exception as e:
        zip_file.close()
        clean_s3_folder(accession)
        print()
        return f'Unzipping file/s failed: {e}',0,0

    # Get rid of tmp file
    zip_file.close()

    return tmp_dir, ','.join(chr_fns)


def lambda_handler(event, context):
    
    print(event)
    
    # DynamoDB data rec code
    accession = event['Records'][0]["dynamodb"]["NewImage"]["Genome"]["S"]
    jobid = event['Records'][0]["dynamodb"]["NewImage"]["JobID"]["S"]
    sequence = event['Records'][0]['dynamodb']["NewImage"]["Sequence"]["S"]
    body ={ 
        "Genome": accession, 
        "Sequence": sequence, 
        "JobID": jobid
    }
    json_object = json.dumps(body)

    if accession == 'fail':
        sys.exit('Error: No accession found.')
    
    #Determine if the mounted EFS has the required issl file needed to skip ahead in pipeline
    if is_issl_in_efs(accession):
        print ("Issl file has already been generated. Moving to scoring process")
        sendSQS(TARGET_SCAN_QUEUE, json_object) 
        print("All Done... Terminating Program.")
        return 

    #Since issl file does not exist, check if a fasta file can be used to create the issl file
    if not is_fasta_in_s3(accession):
        tmp_dir, chr_fns = dl_accession(accession)
        # if download fails update string for csv
        #if 'fail' in tmp_dir:
            #time = tmp_dir
        # Add run to s3 csv for logging
        #s3_csv_append(s3_client,s3_bucket,accession,filesize,(time_ns()-starttime)*1e-9,csv_fn,lock_key)

        #close temp fasta file directory
        if os.path.exists(tmp_dir):
            print("Cleaning Up...")
            shutil.rmtree(tmp_dir)
        

    # fasta file exists or has been created, moving to generating issl file
    sendSQS(ISSL_QUEUE, json_object)
        
    create_log(s3_client, s3_log_bucket, context, accession, jobid, 'Downloader')

    print("All Done... Terminating Program.")


if __name__== "__main__":
    event, context = main()
    lambda_handler(event, context)