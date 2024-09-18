import json
import boto3
import os

s3_client = boto3.client('s3', region_name='ap-southeast-2', endpoint_url='https://s3.ap-southeast-2.amazonaws.com')
bucket_name = os.environ['BUCKET_NAME']


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
                                                                    'Key': folder_name+ '/' + 'fasta/' + file_name, 
                                                                    'ContentType': file_type},
                                                            ExpiresIn=3600)
                print(presigned_url)

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

                #response = s3_client.list_objects_v2(Bucket=bucket_name)


                response = s3_client.list_objects_v2(Bucket=bucket_name, Delimiter='/')

                # Extract prefixes (which represent the folders)
                objects = [prefix['Prefix'].rstrip('/') for prefix in response.get('CommonPrefixes', [])]
                print("Top-level directories:", objects)

               #objects = [obj['Key'] for obj in response.get('Contents', [])]

                return {
                    'statusCode': 200,
                    'body': json.dumps({'object_keys': objects}),
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
                'Content-Type': 'application/json'
            }
        }
