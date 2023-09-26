import boto3, os, re, json, smtplib
from common_funcs import *

# HTML Support
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


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
            job = table.get_item(Key={"JobID" : str(jobID)})['Item']


            emailBody = f"""
            <html>
            <head>
            {emailHeader}
            </head>
            <body>Hello!<br>
            <br>
            Your Crackling Query for Genome {genome} is complete. Please find the results <a href="{FRONTEND_URL}/results/{jobID}/targets">here</a><br>
            <table>
            <tr>
                <th>Time</th>
                <th>Genome</th>
                <th>JobID</th>
                <th>Query Sequence</th>
            </tr>
            <tr>
                <td>{job[""]}</td>
                <td>{job[""]}</td>
                <td>{job[""]}</td>
                <td>{job[""]}</td>
            </tr>
            </html>
            """





        
    
    