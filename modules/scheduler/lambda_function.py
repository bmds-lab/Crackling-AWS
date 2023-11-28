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


def SpawnLambda(dictionary):
    print("Spinning up lambdas for download, bowtie & isslCreation")
    json_object = json.dumps(dictionary)
    print(json_object)
    sendSQS(QUEUE,json_object)


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
    dictionary ={ 
        "Genome": genome, 
        "Sequence": sequence, 
        "JobID": jobid
    }

    # get genome file size
    metaDataFile, ChromosomeLength = get_fna_size_accessions(genome)

    # genome file size
    if metaDataFile > 0: 
        metaDataFile = metaDataFile / 1048576

        SpawnLambda(dictionary)
    else:
        ChromosomeLength = ChromosomeLength / 1048576
       
        SpawnLambda(dictionary)