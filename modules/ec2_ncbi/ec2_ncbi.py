import sys, os, argparse
from pathlib import Path

# Add paths for where scripts/files are on ec2
sys.path.insert(0, os.path.join(os.path.abspath(os.sep),'ec2Code','modules'))
sys.path.insert(0, os.path.join(os.path.abspath(os.sep),'ec2Code','src','crackling','utils'))
sys.path.insert(0, os.path.join(os.path.abspath(os.sep),'ec2Code','src'))
sys.path.insert(0, os.path.join(os.path.abspath(os.sep),'ec2Code'))

# import code as modules
from common_funcs import *
import bt2Lambda
import issl_creation
import lambda_downloader

def cliArguments():
    # CLI helper
    parser = argparse.ArgumentParser(description="prepareFromNcbiAccession Utility for Crackling.",
                                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    #required argument
    parser.add_argument("accession", help="specify accession")
    parser.add_argument("sequence", help="specify accession")
    parser.add_argument("jobid", help="specify accession")
    return vars(parser.parse_args())

if __name__== "__main__":
    # return commandline arguments
    args = cliArguments()

    # Create s3_client
    access_point_arn = os.environ['ACCESS_POINT_ARN']
    s3_client = boto3.client('s3', endpoint_url=access_point_arn)

    # Create event and context 
    event, context = main(args['accession'],args['sequence'],args['jobid'])
    
    #Download accession
    print("Run: accession downloading code.")
    tmp_dir = lambda_downloader.ec2_start(s3_client, event, context)

    #bt2
    print("Run: bowtie2 creation code.")
    bt2Lambda.ec2_start(s3_client, tmp_dir, event, context)

    #issl
    print("Run: issl creation code.")
    issl_creation.ec2_start(s3_client, tmp_dir, event, context)

    #close temp fasta file directory
    if os.path.exists(tmp_dir):
        print("Cleaning Up...")
        shutil.rmtree(tmp_dir)

    print("EC2 Successfully completed.")