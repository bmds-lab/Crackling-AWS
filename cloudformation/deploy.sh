#!/usr/bin/env bash

export PROFILE="$1"
export AWS_DEFAULT_REGION="$2"
export STACK_NAME="$3"
export S3_BUCKET="cf-${STACK_NAME}-${AWS_DEFAULT_REGION}"

aws s3 mb --profile $PROFILE s3://$S3_BUCKET
aws cloudformation package --profile $PROFILE --template-file ./CracklingCloudFormation.yaml --output-template-file CracklingCloudFormation.yaml.packaged --s3-bucket $S3_BUCKET
aws cloudformation deploy --profile $PROFILE --template-file ./CracklingCloudFormation.yaml.packaged --stack-name $STACK_NAME --capabilities CAPABILITY_IAM --s3-bucket $S3_BUCKET
