import json
import boto3
import os
import re

#s3_client = boto3.client('s3', region_name='ap-southeast-2', endpoint_url='https://s3.ap-southeast-2.amazonaws.com')
bucket_name = os.environ['BUCKET_NAME']
region_name = os.environ['REGION_NAME']

s3_client = boto3.client('s3', region_name=region_name, endpoint_url=f'https://s3.{region_name}.amazonaws.com')

# TO DO: For new files that are uploaded to s3, ensure their names have .fna in them otherwise will result in error
# or take out the error

def lambda_handler(event, context):
    try:
        # Extract file name and type from the event
        if event['httpMethod'] == 'GET' and 'queryStringParameters' in event:

            action = event['queryStringParameters'].get('action', '')
            print(action)

            if action == 'generate_presigned_url':

                file_name = event['queryStringParameters']['file_name']
                file_type = event['queryStringParameters']['file_type']

                base_name, _ = os.path.splitext(file_name)
                folder_name = base_name

                presigned_url = s3_client.generate_presigned_url('put_object',
                                                            Params={'Bucket': bucket_name, 
                                                                    'Key':   folder_name + '/' + 'fasta/' + file_name, 
                                                                    'ContentType': file_type},
                                                            ExpiresIn=3600)
                
            # Return the presigned URL as part of the response
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'url': presigned_url,
                    }),
                    'headers': {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*',
                        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                        'Access-Control-Allow-Headers': 'Content-Type'
                    }
                }
            
            elif action == 'list_data':


                response = s3_client.list_objects_v2(Bucket=bucket_name, Delimiter='/')
                objects = [prefix['Prefix'].rstrip('/') for prefix in response.get('CommonPrefixes', [])]
                # remove genomes from NCBI to just show custom datasets 
                genome_accession_pattern = re.compile(r'^(GCA|GCF)_\d{9}\.\d+$')
                filtered_objects = [obj for obj in objects if not genome_accession_pattern.match(obj) and obj != 'Test_Packages']
                print("Top-level custom directories:", filtered_objects)

                return {
                    'statusCode': 200,
                    'body': json.dumps({'object_keys': filtered_objects}),
                    'headers': {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*',
                        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                        'Access-Control-Allow-Headers': 'Content-Type'
                    }
                }

    
    except Exception as e:
        print("There is an error")

        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            }),
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                'Access-Control-Allow-Headers': 'Content-Type'
                
            }
        }
