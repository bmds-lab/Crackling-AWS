#!/usr/bin/env bash
export AWS_DEFAULT_REGION="$1"
export STACK_NAME="$2"
export S3_BUCKET="cf-${STACK_NAME}-${AWS_DEFAULT_REGION}"

aws s3 mb s3://$S3_BUCKET
aws cloudformation package --template-file ./template_gtscan.yaml --output-template-file packaged-template.yaml --s3-bucket $S3_BUCKET
aws cloudformation deploy --template-file ./packaged-template.yaml --stack-name $STACK_NAME --capabilities CAPABILITY_IAM --s3-bucket $S3_BUCKET
