import sys, re, os, shutil, zipfile, boto3, json

# NEW THINGS #############################
from ftplib import FTP
import math
#########################################

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
#starttime = time_ns()
TARGET_SCAN_QUEUE = os.environ['TARGET_SCAN_QUEUE']
ISSL_QUEUE = os.getenv('ISSL_QUEUE')
LIST_PREFIXES = [".issl", ".offtargets"]
FILE_PARTS_QUEUE = os.getenv('FILE_PARTS_QUEUE')

# Create S3 client
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')




def retrieve_fasta_meta_data(genome_accession):
    try:
        # Construct FTP URL
        ftp = FTP("ftp.ncbi.nih.gov")
        ftp.login()
        path = f"/genomes/all/{genome_accession[0:3]}/{genome_accession[4:7]}/{genome_accession[7:10]}/{genome_accession[10:13]}"
        ftp.cwd(path)
      
        # find genome name
        directories = ftp.nlst()
        for directory in directories:
            if directory.startswith(genome_accession):
                required_directory = directory
                break
    
        ftp.cwd(required_directory)
        ftp_directory_path = f"{path}/{required_directory}"

        files = ftp.nlst()
        ftp.sendcmd("TYPE i")

        # list .fna files
        fna_file_details = []
        for file in files:
            if ".fna" in file:
                file_size = ftp.size(file)
                print(f"{file}: {file_size} bytes")
                fna_file_details.append({"file_name": file, "file_size": file_size})

        # complete connection to ftp server
        ftp.quit()
        # chosen_fna_file = fna_file_details[0]["file_name"]
        http_base_url = "https://ftp.ncbi.nlm.nih.gov"
        http_url = f"{http_base_url}{ftp_directory_path}"
        return http_url, fna_file_details
    except Exception as e:
        print(f"Error downloading file: {e}")


# start the multipart upload 
def start_part_upload(bucket_name, genome_accession, filename):
    object_key = f"MultiPart_Testing/{genome_accession}/fasta/{filename}"
    response = s3_client.create_multipart_upload(
        Bucket=bucket_name,
        Key=object_key
    )
    upload_id = response['UploadId']
    return upload_id



def file_parts(genome_accession, http_url, fna_file_details):
    num_files = len(fna_file_details)
    result = []

    min_multipart_file_size = 50000000

    for file in fna_file_details:
        chosen_file_name = file["file_name"]
        chosen_file_size = file["file_size"]

        file_http_url = f"{http_url}/{chosen_file_name}"
        object_key = f"MultiPart_Testing/{genome_accession}/fasta/{chosen_file_name}"

        if chosen_file_size <= min_multipart_file_size:
            part_info = {
                "genome_accession": genome_accession,
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
            num_file_parts = 7   # this detemines how many parts the file is going to split into 
            part_size = math.ceil(chosen_file_size / num_file_parts)  # Size of each part

            # initialise the multipart upload
            upload_id = start_part_upload(s3_bucket, genome_accession, chosen_file_name)
            for i in range(num_file_parts):
                start_byte = i * part_size
                end_byte = min((i + 1) * part_size - 1, chosen_file_size - 1)
                
                part_info = {
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
    s3_multipart_destination_part2 =  f"MultiPart_Testing/{accession}/issl"
    

    #issl and offtarget files based on accession
    files_to_expect = []
    for prefix in LIST_PREFIXES:
        files_to_expect.append(accession + prefix)

    actual = files_exist_s3_dir(s3_client, s3_bucket, s3_destination_path, files_to_expect)
    test = files_exist_s3_dir(s3_client, s3_bucket, s3_multipart_destination_part2, files_to_expect)

    return actual, test


def is_fasta_in_s3_multipart(accession):
    try:
        s3_multipart_destination_folder =  f"MultiPart_Testing/{accession}/fasta"
        response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix= s3_multipart_destination_folder)
        
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

    actual_issl_exists, mulit_part_issl = is_issl_in_s3(accession)
    
    if mulit_part_issl:
        print("The ISSL for this has already been genereated")
    else:
        print("The ISSL doesn't exist in this direcotry")

    # checks if the file are in the S3 directory for multipart testing. If it is, it sends it over

    if not is_fasta_in_s3_multipart(accession):
        
        http_url, fna_file_details = retrieve_fasta_meta_data(accession)
        file_names = file_parts(accession, http_url, fna_file_details)
        print("The fasta files have yet to be created")
        for file in file_names:
            MessageBody=json.dumps(file)
            sendSQS(FILE_PARTS_QUEUE, MessageBody)
        print(file_names)
        # here i would send it over to the ISSL  queue after creating the fasta files
    else:
        print("the files already exists within the system, nothing should get sent through the queue, it would have to be send over to another queue in actuality")
        # here I would send it to the 

    #Determine if S3 has required issl files to skip ahead in pipeline
    if actual_issl_exists:
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

    print("All Done... Terminating Program.")

if __name__== "__main__":
    event, context = main()
    lambda_handler(event, context)