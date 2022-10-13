import sys
import os
import argparse
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.abspath(os.sep),'ec2Code','modules'))
sys.path.insert(0, os.path.join(os.path.abspath(os.sep),'ec2Code','src','crackling','utils'))
sys.path.insert(0, os.path.join(os.path.abspath(os.sep),'ec2Code','src'))
sys.path.insert(0, os.path.join(os.path.abspath(os.sep),'ec2Code'))
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

    print(args)

    s3_client = boto3.client('s3')

    event, context = main(args['accession'],args['sequence'],args['jobid'])
    
    #Download accession
    tmp_dir = lambda_downloader.ec2_start(s3_client, event, context)

    #bt2
    bt2Lambda.ec2_start(s3_client,tmp_dir, event, context)

    #issl
    issl_creation.ec2_start(s3_client,tmp_dir, event, context)


    if os.path.exists(tmp_dir):
        shutil.rmtree(tmp_dir)

    print("ye boi")