from webbrowser import Chrome
import ncbi.datasets
# import json
import os
import boto3
import csv

# try:
#     from common_funcs import *
# except:
#     sys.path.insert(0, '/config/common_funcs/python')
#     from common_funcs import *

s3 = boto3.client('s3')

AMI = os.environ['AMI']
INSTANCE_TYPE = os.environ['INSTANCE_TYPE']
KEY_NAME = os.environ['KEY_NAME']
SUBNET_ID = os.environ['SUBNET_ID']
REGION = os.environ['REGION']
QUEUE = os.environ['QUEUE']

# eventJSON = '{"accessions": ["GCF_001433935.1","GCF_002211085.1","GCA_003073215.1","GCF_901001135.1","GCA_900231445.1"]}'

# event = json.loads(eventJSON)

ec2 = boto3.client('ec2', region_name=REGION)

def handler(event, context):
    cutoffValues = []
    
    obj = s3.get_object(Bucket="downloadercutoff", Key="Downloader.csv") #get the csv from the bucket
    data = obj['Body'].read().decode('utf-8').splitlines() # parse the data
    records = csv.reader(data)
    
    for eachRecord in records: #
        cutoffValues.append(eachRecord) #append each row of the cutoff csv into a list for later use
        
    print(cutoffValues)
        
    def SpawnLambda():
        genome = event['Records'][0]["dynamodb"]["NewImage"]["Genome"]["S"]
        print(genome)
        sendSQS(QUEUE,genome) #send to sqs to trigger lambda section of pipeline

    def SpawnEC2():
        genome = event['Records'][0]["dynamodb"]["NewImage"]["Genome"]["S"]
        jobID = event['Records'][0]["dynamodb"]["NewImage"]["JobID"]["S"]
        sequence = event['Records'][0]['dynamodb']["NewImage"]["Sequence"]["S"]

        # message = event['message']
        init_script = f"""#!/bin/bash
        export BUCKET="macktest"
        source /ec2Code/bin/activate
        python /ec2Code/ec2_ncbi.py {genome} {sequence} {jobID}
        shutdown -h now
                    """
        instance = ec2.run_instances( # create ec2 for when accession will take too long to download
            ImageId=AMI,
            InstanceType=INSTANCE_TYPE,
            KeyName=KEY_NAME,
            MaxCount=1,
            MinCount=1,
            InstanceInitiatedShutdownBehavior='terminate', 
            UserData=init_script
        )
        instance_id = instance['Instances'][0]['InstanceId']
        print(instance_id)

    def get_fna_size_accessions():
        api_instance = ncbi.datasets.GenomeApi(ncbi.datasets.ApiClient())
        
        genome = event['Records'][0]["dynamodb"]["NewImage"]["Genome"]["S"]
        
        print(genome)
        
        assembly_accessions = genome.split() ## needs to be a full accession.version

        genome_summary = api_instance.assembly_descriptors_by_accessions(assembly_accessions, page_size=1)

        type(genome_summary)

        print(f"Number of assemblies: {genome_summary.total_count}")

        # print(genome_summary)
        metaDataFile = 0 # this measurement is most accurate
        try:
            for a in genome_summary.assemblies[0].assembly.annotation_metadata.file:
                metaDataFile = metaDataFile + int(a.estimated_size)
        except:
            print("MetaData Length is missing data")

        
        ChromosomeLength = 0 # This measurement is needed if metaData is not present
        try:
            for a in genome_summary.assemblies[0].assembly.chromosomes:
                ChromosomeLength = ChromosomeLength + int(a.length)
        except:
            print("Chromosome Length is missing data")
        
        print(f" The sum of the files is {metaDataFile}")
        print(f" The sum of the chromosome length is {ChromosomeLength}")

        # add code to spawn either ec2 or lambda function
        
        if metaDataFile > 0: 
            metaDataFile = metaDataFile / 1048576
            if metaDataFile < int(cutoffValues[0][0]):
                #SpawnLambda()
                print("LAMBDA")
            else:
                #SpawnEC2
                print("EC2")
        else:
            ChromosomeLength = ChromosomeLength / 1048576
            if ChromosomeLength < float(cutoffValues[1][0]):
                #SpawnLambda()
                print("LAMBDA")
            else:
                print("EC2")
                #SpawnEC2()
        SpawnEC2()


    get_fna_size_accessions()
