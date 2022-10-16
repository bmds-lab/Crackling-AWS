import ncbi.datasets, os, json, boto3

from common_funcs import *

try:
    import ncbi.datasets
except ImportError:
    print('ncbi.datasets module not found. To install, run `pip install ncbi-datasets-pylib`.')

AMI = os.environ['AMI']
BUCKET = os.environ['BUCKET']
INSTANCE_TYPE = os.environ['INSTANCE_TYPE']
REGION = os.environ['REGION']
QUEUE = os.environ['QUEUE']
EC2_ARN = os.environ['EC2_ARN']
EC2_CUTOFF = int(os.environ['EC2_CUTOFF'])

def SpawnLambda(dictionary):
    print("Spinning up lambdas for download, bowtie & isslCreation")
    json_object = json.dumps(dictionary)
    print(json_object)
    sendSQS(QUEUE,json_object)

def SpawnEC2(genome,jobid,sequence):
    print("Spinning up EC2 for download, bowtie & isslCreation")
    ec2 = boto3.client('ec2', region_name=REGION)

    init_script = f"""#!/bin/bash
    sudo mkfs -t xfs /dev/nvme1n1
    sudo mkdir /tmp2
    sudo mount /dev/nvme1n1 /tmp2
    sudo cp -R /tmp/* /tmp2
    sudo umount /tmp2
    sudo mount /dev/nvme1n1 /tmp
    sudo chmod 777 /tmp
    export BUCKET="{BUCKET}"
    source /ec2Code/.venv/bin/activate
    python /ec2Code/ec2_ncbi.py {genome} {sequence} {jobid} || shutdown -h -t 30
    shutdown -h -t 30
                """
    
    # create ec2 for when accession will take too long to download
    instance = ec2.run_instances( 
        ImageId=AMI,
        InstanceType=INSTANCE_TYPE,
        MaxCount=1,
        MinCount=1,
        InstanceInitiatedShutdownBehavior='terminate', 
        UserData=init_script,
        IamInstanceProfile={
            'Arn': EC2_ARN  
        }   
    )
    instance_id = instance['Instances'][0]['InstanceId']
    response = ec2.monitor_instances(InstanceIds=[instance_id])
    print(f"instance_id: {instance_id}")
    print(f"EC2 Monitoring response: {response}")

def get_fna_size_accessions(genome):
    api_instance = ncbi.datasets.GenomeApi(ncbi.datasets.ApiClient())
    
    assembly_accessions = genome.split() ## needs to be a full accession.version

    genome_summary = api_instance.assembly_descriptors_by_accessions(assembly_accessions, page_size=1)

    print(f"Number of assemblies: {genome_summary.total_count}")

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

    return metaDataFile, ChromosomeLength


def lambda_handler(event, context):
    genome = event['Records'][0]["dynamodb"]["NewImage"]["Genome"]["S"]
    jobid = event['Records'][0]["dynamodb"]["NewImage"]["JobID"]["S"]
    sequence = event['Records'][0]['dynamodb']["NewImage"]["Sequence"]["S"]
    print("EC2_CUTOFF",EC2_CUTOFF)
    dictionary ={ 
        "Genome": genome, 
        "Sequence": sequence, 
        "JobID": jobid
    }

    # get genome file size
    metaDataFile, ChromosomeLength = get_fna_size_accessions(genome)

    # add code to spawn either ec2 or lambda function based on 
    # genome file size
    if metaDataFile > 0: 
        metaDataFile = metaDataFile / 1048576
        if (metaDataFile == 0) or (metaDataFile > EC2_CUTOFF):
            SpawnEC2(genome,jobid,sequence)
        else:
            SpawnLambda(dictionary)
    else:
        ChromosomeLength = ChromosomeLength / 1048576
        if (ChromosomeLength == 0) or (ChromosomeLength > EC2_CUTOFF):
            SpawnEC2(genome,jobid,sequence)
        else:
            SpawnLambda(dictionary)