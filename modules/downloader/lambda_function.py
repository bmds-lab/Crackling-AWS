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

# Create S3 client
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')


def is_issl_in_s3(accession):
    s3_destination_path = f"{accession}/issl"

    #issl and offtarget files based on accession
    files_to_expect = []
    for prefix in LIST_PREFIXES:
        files_to_expect.append(accession + prefix)

    return files_exist_s3_dir(s3_client, s3_bucket, s3_destination_path, files_to_expect)


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
        sys.exit('Error - Download failed')
    
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
                    #upload to s3
                    s3_client.upload_fileobj(fasta_file, s3_bucket, s3_name)
                    #wait until file is uploaded 
                    s3_resource.Object(s3_bucket, s3_name).wait_until_exists()
                    fasta_file.close()
            print(" Done.")
    except Exception as e:
        zip_file.close()
        clean_s3_folder(s3_client, s3_bucket, accession)
        sys.exit('Unzipping file failed')

    # Get rid of tmp file
    zip_file.close()

    return tmp_dir


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
    
    #Determine if S3 has required issl files to skip ahead in pipeline
    if is_issl_in_s3(accession):
        print ("Issl file has already been generated. Moving to scoring process")
        sendSQS(TARGET_SCAN_QUEUE, json_object) 
        print("All Done... Terminating Program.")
        return 

    #Since issl file does not exist, check if a fasta file can be used to create the issl file
    if not is_fasta_in_s3(s3_client, s3_bucket, accession):
        tmp_dir = dl_accession(accession)
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