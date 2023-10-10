import boto3, os, json
from botocore.exceptions import ClientError

JOBS_TABLE = os.getenv('JOBS_TABLE', 'jobs')
FRONTEND_URL = os.getenv('FRONTEND_URL')

# Defines for email
FROM_EMAIL = "email@placeholder.com" # SET EMAIL ADDRESS HERE
EMAIL_SUBJECT = f"test" #f"Crackling Query Complete | {job['Genome']}"
CHARSET = "UTF-8" # The character encoding for the email.
EMAIL_HEADER = '''
    <style>
    table {
    font-family: arial, sans-serif;
    border-collapse: collapse;
    width: 100%;
    }

    td, th {
    border: 1px solid #dddddd;
    text-align: left;
    padding: 8px;
    }

    tr:nth-child(even) {
    background-color: #dddddd;
    }
    </style>'''

# connect to DYDB
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(JOBS_TABLE)

# connect to SES
ses = boto3.client('ses',region_name="ap-southeast-2")

def lambda_handler(event, context):
    for record in event['Records']:
        jobID = record["body"]

        #get email address from jobs table
        job = table.get_item(Key={"JobID" : str(jobID)})['Item']

        toEmail = job["Email"]

        # populate email template
        emailBody = f"""
        <html>
        <head>
        {EMAIL_HEADER}
        </head>
        <body>Hello!<br>
        <br>
        Your Crackling Query for Genome {job["Genome"]} is complete. Please find the results <a href="{FRONTEND_URL}">here</a><br>
        Job Information:<br>
        <table>
        <tr>
            <th>Time</th>
            <th>Genome</th>
            <th>JobID</th>
            <th>Query Sequence</th>
        </tr>
        <tr>
            <td>{job["DateTimeHuman"]}</td>
            <td>{job["Genome"]}</td>
            <td>{job["JobID"]}</td>
            <td>{job["Sequence"]}</td>
        </tr>
        </html>
        """

        # Try to send the email.
        try:
            #Provide the contents of the email.
            response = ses.send_email(
                Destination={
                    'ToAddresses': [
                        toEmail,
                    ],
                },
                Message={
                    'Body': {
                        'Html': {
                            'Charset': CHARSET,
                            'Data': emailBody,
                        }
                    },
                    'Subject': {
                        'Charset': CHARSET,
                        'Data': EMAIL_SUBJECT,
                    },
                },
                Source=FROM_EMAIL
            )
        # Display an error if something goes wrong.	
        except ClientError as e:
            print(e.response['Error']['Message'])
        else:
            print("Email sent! Message ID:"),
            print(response['MessageId'])
