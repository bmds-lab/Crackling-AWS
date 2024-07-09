import json, os
import requests 
import boto3
from common_funcs import *


def download_part_file(args):
    genome_accession = args['genome_accession']
    num_files = args['num_files']
    filename = args['filename']
    file_url = args['file_url']
    part = args['part']
    start_byte = args['start_byte']
    end_byte = args['end_byte']
    
    headers = {'Range': f'bytes={start_byte}-{end_byte}'}
    response = requests.get(file_url, headers=headers, stream=True)
    
    if response.status_code == 206:  # Partial Content
        part_filename = f"Part{part}_{filename}"
        print(f"Part {part} downloaded and saved as {part_filename}")
    else:
        print(f"Failed to download part {part}: HTTP {response.status_code}")

    return response, genome_accession, part_filename, part


def upload_to_s3(response, genome_accession, part_filename):
    s3 = boto3.client('s3')
    s3_key = f"Testing/{genome_accession}/fasta/{part_filename}"

    # Upload streamed data to S3
    s3.upload_fileobj(response.raw, 'your-bucket-name', s3_key)


def upload_to_s3(response, genome_accession, part_filename, s3_bucket):
    s3 = boto3.client('s3')
    s3_key = f"Testing/{genome_accession}/fasta/{part_filename}"

    try:
        s3.upload_fileobj(response.raw, s3_bucket, s3_key)  
        print(f"File uploaded to S3: s3://{s3_bucket}/{s3_key}")
        return True  
    except Exception as e:
        print(f"Error uploading to S3: {str(e)}")
        raise 



def lambda_handler(event, context):
    s3_bucket = os.environ['BUCKET']
    FILES_TABLE = os.environ['FILES_TABLE']
    
    args,body = recv(event)
    response, genome_accession, part_filename, part = download_part_file(args)

    if response.status_code == 206:
        try:
        # Upload to S3
            if upload_to_s3(response, genome_accession, part_filename, s3_bucket):
                # Update DynamoDB after successful S3 upload
                response_dynamo = FILES_TABLE.put_item(
                    Item={
                        'GenomeFileName': FILES_TABLE,
                        'FileNamePartNumber': part
                    }
                )
                
                print("Item uploaded to S3 and DynamoDB:", response_dynamo)
                
                return {
                    'statusCode': 200,
                    'body': 'Item uploaded to S3 and written to DynamoDB successfully'
                }
            else:
                return {
                    'statusCode': 500,
                    'body': 'Failed to upload file to S3'
                }
        except Exception as e:
            print(f"Error processing: {str(e)}")
            return {
                'statusCode': 500,
                'body': 'Failed to process request'
            }


if __name__== "__main__":
    event, context = main()
    lambda_handler(event, context)