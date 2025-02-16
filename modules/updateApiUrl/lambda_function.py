import os
import boto3

s3 = boto3.client('s3')
cloudfront = boto3.client('cloudfront')

def lambda_handler(event, context):
    bucket_name = os.environ['BUCKET_NAME']
    object_key = os.environ['OBJECT_KEY']
    new_api_url = os.environ['NEW_API_URL']
    distribution_id = os.environ['CLOUDFRONT_DISTRIBUTION_ID'] 

    response = s3.get_object(Bucket=bucket_name, Key=object_key)

    index_html_content = response['Body'].read().decode('utf-8')

    # to avoid an infinite loop of S3 update events triggering this lambda function...
    # first check if the index.html file contains the placeholder
    contains_placeholder = "{{API_URL}}" in index_html_content

    if contains_placeholder:

        updated_content = index_html_content.replace("{{API_URL}}", new_api_url)
        s3_put_reposnse = s3.put_object(Bucket=bucket_name, Key='index.html', Body=updated_content, ContentType='text/html')

        invalidation_response = cloudfront.create_invalidation(
            DistributionId=distribution_id,
            InvalidationBatch={
                'Paths': {
                    'Quantity': 1,
                    'Items': [
                        '/index.html',  # Specify the path to invalidate
                    ]
                },
                'CallerReference': str(context.aws_request_id)  
            }
        )
        print("Invalidation Response:", invalidation_response)

        return {
            'statusCode': 200,
            'body': 'index.html updated successfully'
        }
    else:
        
        return {
            'statusCode': 200,
            'body': 'index.html did not contain the placeholder. did nothing.'
        }
