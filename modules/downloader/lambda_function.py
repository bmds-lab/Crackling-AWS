import sys, re, os, shutil, zipfile, boto3, json
from ftplib import FTP
import math
from threading import Thread
from time import time, time_ns
from botocore.exceptions import ParamValidationError

from common_funcs import *


try:
    import ncbi.datasets
except ImportError:
    print('ncbi.datasets module not found. To install, run `pip install ncbi-datasets-pylib`.')

# Global variables
S3_BUCKET = os.environ['BUCKET']
TARGET_SCAN_QUEUE = os.environ['TARGET_SCAN_QUEUE']
ISSL_QUEUE = os.getenv('ISSL_QUEUE')
LIST_PREFIXES = [".issl", ".offtargets"]
FILE_PARTS_QUEUE = os.getenv('FILE_PARTS_QUEUE')

# Create S3 client
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')



# Retrieves metadata for .fna files associated with a given genome accession from the NCBI FTP server
def retrieve_fasta_meta_data(genome_accession):
    try:
        # Connect to FTP server and login 
        ftp = FTP("ftp.ncbi.nih.gov")
        ftp.login()

        # Construct FTP URL based on accession
        path = f"/genomes/all/{genome_accession[0:3]}/{genome_accession[4:7]}/{genome_accession[7:10]}/{genome_accession[10:13]}"
        ftp.cwd(path)
      
        # find corresponding genome name
        directories = ftp.nlst()
        for directory in directories:
            if directory.startswith(genome_accession):
                required_directory = directory
                break

        # Change to identified directory
        ftp.cwd(required_directory)
        ftp_directory_path = f"{path}/{required_directory}"

        # list all files in the directory
        files = ftp.nlst()
        ftp.sendcmd("TYPE i")
        fna_file_details = []
        for file in files:
            if "genomic.fna" in file and "from_genomic" not in file:
                file_size = ftp.size(file)
                print(f"{file}: {file_size} bytes")
                fna_file_details.append({"file_name": file, "file_size": file_size})

        # close connection to ftp server
        ftp.quit()
        # chosen_fna_file = fna_file_details[0]["file_name"]

        # Construct the base HTTP URL for the genome directory
        http_base_url = "https://ftp.ncbi.nlm.nih.gov"
        http_url = f"{http_base_url}{ftp_directory_path}"
        return http_url, fna_file_details
    except Exception as e:
        print(f"Error downloading file: {e}")


# Starts a multipart upload to S3 and returns the upload ID
def start_part_upload(bucket_name, genome_accession, filename):
    object_key = f"{genome_accession}/fasta/{filename}"
    response = s3_client.create_multipart_upload(
        Bucket=bucket_name,
        Key=object_key
    )
    upload_id = response['UploadId']
    return upload_id



# Generates metadata for file parts needed for multipart upload to S3.
def file_parts(genome_accession, http_url, fna_file_details, json_object):
    num_files = len(fna_file_details)
    result = []

    # Convert JSON string to dictionary if necessary
    if isinstance(json_object, str):
        json_object = json.loads(json_object)

    min_multipart_file_size = 50000000

    for file in fna_file_details:
        chosen_file_name = file["file_name"]
        chosen_file_size = file["file_size"]

        file_http_url = f"{http_url}/{chosen_file_name}"
        object_key = f"{genome_accession}/fasta/{chosen_file_name}"

        if chosen_file_size <= min_multipart_file_size:
            # single part file
            part_info = {
                "Genome": json_object["Genome"], 
                "Sequence": json_object["Sequence"], 
                "JobID": json_object["JobID"],
                "genome_accession": genome_accession, # this is quite redundant, will need to change soon
                "num_files": num_files,
                "filename": chosen_file_name,
                "file_url": file_http_url,
                "part": 1,
                "start_byte": 0,
                "end_byte": chosen_file_size - 1,
                "upload_id": None,
                "object_key": object_key
            }
            result.append(part_info)

        else:

            part_size = 50000000 # Maximum size of each part in megabytes
            num_file_parts = math.ceil(chosen_file_size/ part_size)


            # num_file_parts = 7   # this detemines how many parts the file is going to split into 
            # part_size = math.ceil(chosen_file_size / num_file_parts)  # Size of each part

            # initialise the multipart upload
            upload_id = start_part_upload(S3_BUCKET, genome_accession, chosen_file_name)
            for i in range(num_file_parts):
                start_byte = i * part_size
                end_byte = min((i + 1) * part_size - 1, chosen_file_size - 1)
                
                part_info = {
                    "Genome": json_object["Genome"], 
                    "Sequence": json_object["Sequence"], 
                    "JobID": json_object["JobID"],
                    "genome_accession": genome_accession,
                    "num_files": num_files,
                    "parts_per_file": num_file_parts,
                    "filename": chosen_file_name,
                    "file_url": file_http_url,
                    "part": i+1,
                    "start_byte": start_byte,
                    "end_byte": end_byte,
                    "upload_id": upload_id,
                    "object_key": object_key
                }
                
                result.append(part_info)

    return result


def is_issl_in_s3(accession):
    s3_destination_path = f"{accession}/issl"
    s3_multipart_destination_part2 =  f"{accession}/issl"
    #issl and offtarget files based on accession
    files_to_expect = []
    for prefix in LIST_PREFIXES:
        files_to_expect.append(accession + prefix)

    actual = files_exist_s3_dir(s3_client, S3_BUCKET, s3_destination_path, files_to_expect)
    test = files_exist_s3_dir(s3_client, S3_BUCKET, s3_multipart_destination_part2, files_to_expect)

    return actual, test


def is_fasta_in_s3_multipart(accession):
    try:
        s3_multipart_destination_folder =  f"{accession}/fasta"
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix= s3_multipart_destination_folder)
        
        if 'Contents' in response:
            for obj in response['Contents']:
                if obj['Size'] > 0:
                    return True
            return False
        else:
            return False
    except Exception as e:
        print(f"An error occurred: {e}")
        return False


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

    actual_issl_exists, mulit_part_issl = is_issl_in_s3(accession)
    
    ## future me - in terms of fasta, ensure all files for accession are present
    # so don't just check if there is one file... but also how many files

    if not is_fasta_in_s3_multipart(accession):
        http_url, fna_file_details = retrieve_fasta_meta_data(accession)
        file_names = file_parts(accession, http_url, fna_file_details, json_object)
        print("The fasta files have yet to be created")
        for file in file_names:
            MessageBody=json.dumps(file)
            sqs_send_message(FILE_PARTS_QUEUE, MessageBody)
        print(file_names)
    else:
        if  mulit_part_issl:
            print ("Issl file has already been generated. Moving to scoring process")
            sqs_send_message(TARGET_SCAN_QUEUE, json_object) 
            print("All Done... Terminating Program.")
        else:
            print("The fasta files exist but the issl ones do not")
            sqs_send_message(ISSL_QUEUE, json_object)

    print("All Done... Terminating Program.")

if __name__== "__main__":
    event, context = local_lambda_invocation()
    lambda_handler(event, context)