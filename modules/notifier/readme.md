Steps to setup notifier:

1. Modify the "FROM_EMAIL" varibale in lambda_function.py to be an email address you own
2. Verify this email address in the AWS console (https://docs.aws.amazon.com/ses/latest/dg/creating-identities.html)
3. Move SES from sandbox to production (https://docs.aws.amazon.com/ses/latest/dg/request-production-access.html)
