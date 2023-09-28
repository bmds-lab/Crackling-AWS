import boto3, os, re, json, smtplib
# from common_funcs import *

# HTML Support
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

FROM_EMAIL = "notification@crackling.com" # change this to change the address the email is sent from

JOBS_TABLE = os.getenv('JOBS_TABLE', 'jobs')
FRONTEND_URL = os.getenv('FRONTEND_URL')

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(JOBS_TABLE)

emailHeader = '''
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



def lambda_handler(event, context):

    with smtplib.SMTP('localhost') as smtp_server:
        for record in event['Records']:

            jobID = json.loads(record['body'])['JobID']

            #get email address from jobs table
            # job = table.get_item(Key={"JobID" : str(jobID)})['Item']

            # populate email template
            emailBody = "this is a test"
            # emailBody = f"""
            # <html>
            # <head>
            # {emailHeader}
            # </head>
            # <body>Hello!<br>
            # <br>
            # Your Crackling Query for Genome {job["Genome"]} is complete. Please find the results <a href="{FRONTEND_URL}">here</a><br>
            # Job Information:<br>
            # <table>
            # <tr>
            #     <th>Time</th>
            #     <th>Genome</th>
            #     <th>JobID</th>
            #     <th>Query Sequence</th>
            # </tr>
            # <tr>
            #     <td>{job["DateTimeHuman"]}</td>
            #     <td>{job["Genome"]}</td>
            #     <td>{job["JobID"]}</td>
            #     <td>{job["Sequence"]}</td>
            # </tr>
            # </html>
            # """

            

            # generate the message
            email = MIMEMultipart('alternative')
            email['Subject'] = f"test"
            # email['Subject'] = f"Crackling Query Complete | {job['Genome']}"
            email['To'] = "mattias.winsen@outlook.com"
            # email['To'] = job["Email"]
            email['From'] = FROM_EMAIL
            email.attach(MIMEText(emailBody,'html'))

            # send the message
            # smtp_server.sendmail(FROM_EMAIL, job["Email"], email.as_string())
            smtp_server.sendmail(FROM_EMAIL, "mattias.winsen@outlook.com", email.as_string())


lambda_handler("e","e")