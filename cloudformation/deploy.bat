:: deploy.bat profile ap-southeast-2 stage-name

set PROFILE=%1
set AWS_DEFAULT_REGION=%2
set STACK_NAME=%3
set S3_BUCKET=crackling-%AWS_DEFAULT_REGION%-%STACK_NAME%

aws s3 mb --profile %PROFILE% s3://%S3_BUCKET%
aws cloudformation package --profile %PROFILE% --template-file ./CracklingCloudFormation.yaml --output-template-file CracklingCloudFormation.yaml.packaged --s3-bucket %S3_BUCKET%
aws cloudformation deploy --profile %PROFILE% --template-file ./CracklingCloudFormation.yaml.packaged --stack-name %STACK_NAME% --capabilities CAPABILITY_IAM --s3-bucket %S3_BUCKET%
