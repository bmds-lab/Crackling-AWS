# Modules
These modules are the code that is run both in the lambda functions of the normal stack and in the EC2 used for larger genomes.

## Downloader
This module uses the [NCBI Datasets](https://github.com/ncbi/datasets) python module to download genomes from the NCBI and associated databases. The requested genome accession to download is downloaded as a zip file to `/tmp`, then each FASTA file is extracted and uploaded to an s3 bucket to be used by the isslCreation and Bowtie2 modules. This module will also check S3 to confirm if the files already exist before downloading
This module requires the "CommonFuncs", "Ncbi" and "Lib" layers to function as expected.


## isslCreation
the isslCreation module uses parts of the [Crackling standalone codebase](https://github.com/bmds-lab/Crackling) to create both a "extractofftargets" and a ISSL index file required for the issl/offtarget-scoring module. Firstly, the "extractOfftargets.py" utility from Crackling standalone, which has been modified to run on a lambda function, is used to create an offtargets file. This offtargets file is needed for the input of the "isslCreateIndex" binary that was compiled from the "isslCreateIndex.cpp" source file in Crackling standalone, which creates the ".issl" index file.

Once the above code has been run successfully, the resulting files are uploaded to the genome S3 bucket, then the s3_success() function is run to create a `issl.notif` file which is required for the s3check module.

This module requires the "CommonFuncs", "IsslCreation" and "Lib" layers to function as expected.

When running on an ec2, the temporary directory where all the fasta files are stored will be used instead of downloading the file from the s3 bucket. Additionally, if running on an EC2, the unmodified, faster version of the the "extractOfftargets.py" utility is used. SQS sending is also disabled for this module when in EC2 mode.

## Notifier
The notiifier module sends an email to the user (optionally) when their job is complete. To do this, the system interacts (in the consensus and ISSL modules) with the task tracking DynamoDB table to check if all tasks are complete. If they are, the notifier is spawned and uses SES to email a user

