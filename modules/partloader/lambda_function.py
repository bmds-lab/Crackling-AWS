import json, os
import requests 
import boto3
from common_funcs import *
from boto3.dynamodb.conditions import Key
import time


# define AWS resources
s3_bucket = os.environ['BUCKET']
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
FILES_TABLE_NAME = os.environ['FILES_TABLE']
FILES_TABLE = dynamodb.Table(FILES_TABLE_NAME)
ISSL_QUEUE = os.getenv('ISSL_QUEUE')


# Downloads a specific byte range of a fasta file from an NCBI server using HTTP range requests
def download_part_file(filename, file_url, part, start_byte, end_byte, is_stream):

    max_retries = 3
    retries = 0
    
    while retries < max_retries:
        print(f"Part {part}_{filename} downloaded commencing....")

        # Construct http range request 
        headers = {'Range': f'bytes={start_byte}-{end_byte}'}
        http_response = requests.get(file_url, headers=headers, stream = is_stream)
        
        # Ensure partial content is sent through
        if http_response.status_code == 206:  # Partial Content
            part_filename = f"Part{part}_{filename}"
            print(f"Part {part} downloaded and saved as {part_filename}")
            return http_response
        elif http_response.status_code == 404: # allow retries incase of 404 errors
            retries += 1
            print(f"Part {part} not found (HTTP 404). Retry {retries}/{max_retries} in 5 seconds...")
            time.sleep(5) 
            print(f"Failed to download part {part}: HTTP {http_response.status_code}")
        else:
            print(f"Failed to download part {part}: HTTP {http_response.status_code}")
            return None
    return None

# Streaming the entire file to S3
def upload_to_s3(response,  object_key):
    try:
        response = s3_client.upload_fileobj(response.raw, s3_bucket, object_key)  
        print(f"File uploaded to S3: s3://{s3_bucket}/{object_key}")
        print(response)
        return True  
    except Exception as e:
        print(f"Error uploading to S3: {str(e)}")
        raise 


# Uploading partial file to S3 using multi-part upload
def part_upload_to_s3(response, upload_id, part_number, object_key):
    try:
        part_response = s3_client.upload_part(
            Bucket=s3_bucket,
            Key=object_key,
            UploadId=upload_id,
            PartNumber=part_number,
            Body=response.content
        )
        etag = part_response['ETag']
        print(f"File part uploaded to S3: s3://{s3_bucket}/{object_key} (Part Number: {part_number}, ETag: {etag})")
        return etag
    except Exception as e:
        print(f"Error uploading part to S3: {str(e)}")
        # Abort the multipart upload if part upload fails
        try:
            s3.abort_multipart_upload(
                Bucket=s3_bucket,
                Key=object_key,
                UploadId=upload_id
            )
            print(f"Aborted multipart upload: {upload_id}")
        except Exception as abort_error:
            print(f"Error aborting multipart upload: {str(abort_error)}")
        raise


# Check if all the parts of a file have been uploaded 
def all_parts_uploaded(filename, total_parts):
    # Query the table to count the number of parts uploaded
    response = FILES_TABLE.query(
        KeyConditionExpression=Key('GenomePartFileName').eq(filename)
    )
    uploaded_parts = response['Count']
    print("Uploaded part: ", uploaded_parts)
    for item in response['Items']:
        print(item)
    
    if uploaded_parts == total_parts:
        return True
    else:
        return False
    

def extract_etags_and_parts(filename):
    try:
        # Query DynamoDB to get all parts for the given filename
        response = FILES_TABLE.query(
            KeyConditionExpression=Key('GenomePartFileName').eq(filename)
        ) 
        items = response['Items']
        parts = [{'ETag': item['etag'], 'PartNumber': int(item['FileNamePartNumber'])} for item in items]
        
        # Sort the parts by part number
        parts.sort(key=lambda x: x['PartNumber'])
        
        return parts
    except Exception as e:
        print(f"Error querying DynamoDB: {str(e)}")
        raise
    


# Combine all the partial files into a single file 
def complete_file_multipart_upload(object_key, upload_id, parts):
    try:
        response = s3_client.complete_multipart_upload(
            Bucket=s3_bucket,
            Key=object_key,
            UploadId=upload_id,
            MultipartUpload={
                'Parts': parts
            }
        )
        print(f"Multipart upload completed successfully for {object_key}")
        return response
    except Exception as e:
        print(f"Error completing multipart upload: {str(e)}")
        raise


# Record each partial upload to s3 into table, along with Etag and part numbers
def file_upload_record(filename, part, etag):
    response_dynamo = FILES_TABLE.put_item(
                    Item={
                        'GenomePartFileName': filename,
                        'FileNamePartNumber': part,
                        'etag': etag, 
                    }
                )
    return response_dynamo



# Check is all files for a genome accession have been uploaded 
def are_all_files_uploaded(num_files, accession):
    s3_multipart_destination_folder =  f"{accession}/fasta"

    try:
        # list all the files uploaded to s3 for genome accession
        response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_multipart_destination_folder)
        
        if 'Contents' not in response:
            print(f"No files found in the folder {s3_multipart_destination_folder}")
            return False
        file_count = 0
        for obj in response['Contents']:
            # Check if the object is a file (not a folder)
            if not obj['Key'].endswith('/'):
                file_count += 1

        # check is all files have been uploaded
        if file_count == num_files:
            print(f"All {num_files} files are uploaded.")
            return True
        else:
            print(f"Expected {num_files} files, but found {file_count} files.")
            return False
    except Exception as e:
        print(f"An error occurred: {e}")
        return False


def lambda_handler(event, context):
    # total_parts = 7
    args,body = recv(event)

    accession = args['Genome']
    jobid = args['JobID']
    sequence = args['Sequence']

    body ={ 
        "Genome": accession, 
        "Sequence": sequence, 
        "JobID": jobid
    }

    json_object = json.dumps(body)

    genome_accession = args['genome_accession']
    num_files = args['num_files']
    filename = args['filename']
    file_url = args['file_url']
    part = args['part']
    start_byte = args['start_byte']
    end_byte = args['end_byte']
    upload_id = args['upload_id']
    object_key = args['object_key']


    try:
        if upload_id != None: # Check if this is a multi-part upload
            is_stream = False 
            # Download part of the fasta file
            http_response = download_part_file(filename, file_url, part, start_byte, end_byte, is_stream)
            #Upload part file to S3 and get ETag 
            etag = part_upload_to_s3(http_response, upload_id, part, object_key)
            # Record Upload in DynamoDB
            response_dynamo = file_upload_record(filename, part, etag)
            print("Item uploaded to S3 and DynamoDB:", response_dynamo)
            
            total_parts = args['parts_per_file']
            # Check if all parts of file have been uploaded 
            if all_parts_uploaded(filename, total_parts):
                # Extract EEtags and part numbers 
                parts = extract_etags_and_parts(filename)
                #combine part files
                response_S3_complete = complete_file_multipart_upload(object_key, upload_id, parts)
                print(response_S3_complete)

                # check if all fasta files for genome accession have been uploaded 
                if are_all_files_uploaded(num_files, genome_accession):
                    sqs_send_message(ISSL_QUEUE, json_object)
                    # send it to the ISSL CREATE SQS QUEUE
                    print("All files uploaded. Next Step ready")
                else:
                    print("All files not uploaded")
            else:
                print("Not all parts for this file have been uploaded")
            return
        
        else: # this is a normal upload

            is_stream = True 
            # Download the entire file 
            http_response = download_part_file(filename, file_url, part, start_byte, end_byte, is_stream)
            # Stream file to S3
            if upload_to_s3(http_response, object_key):
                # Check if all files for genome accession have been uploaded
                if are_all_files_uploaded(num_files, genome_accession):
                    print("All files uploaded. Next Step ready")
                    sqs_send_message(ISSL_QUEUE, json_object)
                    # send it to the ISSL CREATE SQS QUEUE
                else:
                    print("All files not uploaded")
            else:
                print("Normal upload into s3 was unsuccessful")

    except Exception as e:
        print(f"Error processing: {str(e)}")
        return {
            'statusCode': 500,
            'body': 'Failed to download and upload fasta file'
        }

    return 


if __name__== "__main__":
    event, context = local_lambda_invocation()
    lambda_handler(event, context)