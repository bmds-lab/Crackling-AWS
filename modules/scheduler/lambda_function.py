import ncbi.datasets
import json
import os
import boto3

try:
    from common_funcs import *
except:
    sys.path.insert(0, '/config/common_funcs/python')
    from common_funcs import *

# AMI = os.environ['AMI']
# INSTANCE_TYPE = os.environ['INSTANCE_TYPE']
# KEY_NAME = os.environ['KEY_NAME']
# SUBNET_ID = os.environ['SUBNET_ID']
# REGION = os.environ['REGION']
QUEUE = os.environ['QUEUE']

# eventJSON = '{"accessions": ["GCF_001433935.1","GCF_002211085.1","GCA_003073215.1","GCF_901001135.1","GCA_900231445.1"]}'

# event = json.loads(eventJSON)

# ec2 = boto3.client('ec2', region_name=REGION)

def handler(event, context):
    print(event)
    genome = event['Records'][0]["dynamodb"]["NewImage"]["Genome"]["S"]
    sequence = event['Records'][0]["dynamodb"]["NewImage"]["Sequence"]["S"]
    jobid = event['Records'][0]["dynamodb"]["NewImage"]["JobID"]["S"]
    dictionary ={ 
        "Genome": genome, 
        "Sequence": sequence, 
        "JobID": jobid
        } 
    # print(genome)
    # print(sequence)
    # print(jobid)
    # print(dictionary)
    json_object = json.dumps(dictionary)
    print(json_object)
    sendSQS(QUEUE,json_object)

#     def get_fna_size_accessions():
#         api_instance = ncbi.datasets.GenomeApi(ncbi.datasets.ApiClient())

#         for accession in event["accessions"]:
#             assembly_accessions = accession.split() ## needs to be a full accession.version

#             genome_summary = api_instance.assembly_descriptors_by_accessions(assembly_accessions, page_size=1)

#             type(genome_summary)

#             print(f"Number of assemblies: {genome_summary.total_count}")

#             # print(genome_summary)
#             sum = 0
#             try:
#                 for a in genome_summary.assemblies[0].assembly.annotation_metadata.file:
#                     sum = sum + int(a.estimated_size)
#             except:
#                 print("genome is missing data")

            
#             sum1 = 0
#             try:
#                 for a in genome_summary.assemblies[0].assembly.chromosomes:
#                     sum1 = sum1 + int(a.length)
#             except:
#                 print("genome is missing data")
            
#             print(f" The sum of the files is {sum}")
#             print(f" The sum of the chromosome length is {sum1}")

#             # add code to spawn either ec2 or lambda function

#     def genome_summary():
#         genome_summary = api_instance.assembly_descriptors_by_taxon(
#             taxon='9989', ## Rodents taxid
#             page_size=1000,
#             filters_assembly_source='refseq')

#         print(f'Number of assemblies: {genome_summary.total_count}')

#     # message = event['message']
#     init_script = """#!/bin/bash
#     python3 /EC2code/ec2_ncbi.py GCA_000214275.3 macktest AKIAVPURMR73I35VLPXG 6xA8q1B8H01lhG+0LgDi0bIMK9fy6QOW07Z+2r1b
#     shutdown -h now
#                 """

#     instance = ec2.run_instances(
#         ImageId=AMI,
#         InstanceType=INSTANCE_TYPE,
#         KeyName=KEY_NAME,
#         MaxCount=1,
#         MinCount=1,
#         InstanceInitiatedShutdownBehavior='terminate', 
#         UserData=init_script
#     )

#     instance_id = instance['Instances'][0]['InstanceId']
    
#     print(instance_id)

#     # return instance_id


#     # get_fna_size_accessions()

# # handler(event,None)
