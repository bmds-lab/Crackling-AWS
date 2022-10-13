import sys, re, os, shutil, zipfile, tempfile, boto3

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
ec2 = False
starttime = time_ns()

# Create S3 client
s3_client = boto3.client('s3')

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

#download accession data and put it in correct directory
def dl_accession(accession):
    tmp_dir = tempfile.mkdtemp()
    filesize_count = 0
    
    time_1 = time()
    print(f'\nStarting download of {accession}.\nNote: can take a while to download. Please be patient!')

    # initalise API
    genome_api = ncbi.datasets.GenomeApi(ncbi.datasets.ApiClient())

    #download object
    dl_response = genome_api.download_assembly_package(
        accessions = [accession],
        exclude_sequence = False,
        _preload_content = False )

    #Save Zip File
    zip_file = tempfile.NamedTemporaryFile()

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
                    to_extract = zip_ref.open(file_in_zip)
                    # Get file_name without ".fna" file extension
                    name = re.search(r'([^\/]+$)',file_in_zip).group(0)[:-4]
                    #Add directory structure to string name
                    s3_name = f"{accession}/fasta/{name}.fa"
                    print(s3_name)
                    #save name to array
                    tmp_name = f'{tmp_dir}/{name}.fa'
                    chr_fns.append(tmp_name)
                    #upload to s3
                    s3_client.upload_fileobj(to_extract, s3_bucket, s3_name)
                    to_extract.close()
                    #write file to tmp dir
                    if (__name__== "__main__") or ec2:
                        to_extract = zip_ref.open(file_in_zip)
                        f = open(tmp_name,'wb')
                        f.write(to_extract.read())
                        f.close()
                        to_extract.close()
            print(" Done.")
    except Exception as e:
        zip_file.close()
        clean_s3_folder(accession)
        print()
        return f'Unzipping file/s failed: {e}',0,0

    # Get rid of tmp file
    zip_file.close()

    return tmp_dir, ','.join(chr_fns), time_2-time_1

def sum_filesize(accession):    
    assembly_accessions = accession.split() ## needs to be a full accession.version
    
    api_instance = ncbi.datasets.GenomeApi(ncbi.datasets.ApiClient())
    genome_summary = api_instance.assembly_descriptors_by_accessions(assembly_accessions, page_size=1)

    print(f"Number of assemblies: {genome_summary.total_count}")
    
    sum = 0
    try:
        for a in genome_summary.assemblies[0].assembly.chromosomes:
            sum = sum + int(a.length)
    except:
        print("Genome is missing filesize data")
    print(f" The sum of the chromosome length is {sum}")
    return sum


def lambda_handler(event, context):
    args,body = recv(event)
    accession = args['Genome']

    if accession == 'fail':
        sys.exit('Error: No accession found.')
    
    # Get file size for 
    filesize = sum_filesize(accession)

    csv_fn = 'times.csv'
    lock_key = 'lock_key'

    # Create new threads
    thread1 = Thread(target=thread_task, args=(accession,context,filesize,s3_client,s3_bucket,csv_fn,lock_key))
    thread1.daemon = True
    thread1.start()

    # Download accession
    tmp_dir, chr_fns,time = dl_accession(accession)
    
    # if download fails update string for csv
    if 'fail' in tmp_dir:
        time = tmp_dir
    
    # Add run to s3 csv for logging
    s3_csv_append(s3_client,s3_bucket,accession,filesize,(time_ns()-starttime)*1e-9,csv_fn,lock_key)

    #close temp fasta file directory
    if os.path.exists(tmp_dir) and not ec2:
        print("Cleaning Up...")
        shutil.rmtree(tmp_dir)

    # if running on ec2, return early with tmp directory
    if ec2:
        print("All Done... Terminating Program.")
        return tmp_dir

    # send SQS messages to following two lambdas
    ISSL_QUEUE = os.getenv('ISSL_QUEUE')
    BT2_QUEUE = os.getenv('BT2_QUEUE')
    sendSQS(ISSL_QUEUE,body)
    sendSQS(BT2_QUEUE,body)
        
    print("All Done... Terminating Program.")

# ec2 instance code entry and setup function
def ec2_start(s3_Client, event, context):
    global s3_client
    s3_client = s3_Client
    global ec2
    ec2 = True
    return lambda_handler(event, context)

if __name__== "__main__":
    event, context = main()
    lambda_handler(event, context)