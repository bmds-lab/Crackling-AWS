:: deploy.bat ap-southeast-2 stage-name

set AWS_DEFAULT_REGION=%1
set STACK_NAME=%2
set S3_BUCKET=crackling-%AWS_DEFAULT_REGION%-%STACK_NAME%

aws s3 mb s3://%S3_BUCKET%
aws cloudformation package --template-file ./CracklingCloudFormation.yaml --output-template-file CracklingCloudFormation.yaml.packaged --s3-bucket %S3_BUCKET%


aws cloudformation deploy --template-file ./CracklingCloudFormation.yaml.packaged --stack-name %STACK_NAME% --capabilities CAPABILITY_IAM --s3-bucket %S3_BUCKET%
