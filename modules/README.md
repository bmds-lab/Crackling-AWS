# Modules
These modules are the code that is run both in the lambda functions of the normal stack and in the EC2 used for larger genomes.

## ec2_ncbi
This python script is to run the Downloader, Bowtie2 and isslCreation modules on an ec2 instance from one script. This module also handles the running of lambda orientated code in an ec2 situation, passing data between the modules as the SQS method used to communicate between these modules as lambda functions makes no sense on an ec2 where all the code is running on the same machine.

## Lambda Scheduler

Since the scheduler is to dynamically decide between triggering a lambda or spawn an ec2 instance, IAM roles have to be created to allow the lambda function to spawn ec2 instances and s3 bucket rights.  This is achieved by creating a new iam_role and adding policies that give wild card permissions to both ec2 and s3.  This newly created role is then added to a "cfnInstanceProfile" and added as an environment variable for the user lambda to use when spawning an ec2.  

The "AMI" environment variable is the id of the AMI created previously in the ec2Stack.

The "INSTANCE_TYPE" is the type of EC2 instance that will be spun up.  At the moment the default is an r5ad.2xlarge, this is due to it being the cheapest option for high memory.  It may potentially be beneficial to switch to compute optimsied to reduce the computation time at the expense of more expensive memory.  

The "EC2_CUTOFF" is a variable that represents the total estimated length of the file download (MB) that is needed for the lambda to schedule an ec2 instance instead of a lambda function.  Feel free to adjust this variable if you feel this cutoff is too conservative.

"The Queue" variable is just a reference to the sqs queue that is needed when writing the sqs to trigger the subsequent lambda function.

To enable the lambda to give these iam roles, the lambda itself needs these permissions as well.  You can achieve by using the "add_to_principal_policy" to your lambda.Function variable.  You need to give "ec2:RunInstances", "s3:*" and "iam:*" rights to the lambda function.

Lastly, you need to grant the lambda function access to s3Genome, ddbJobs and sqs download.

## Downloader
This module uses the [NCBI Datasets](https://github.com/ncbi/datasets) python module to download genomes from the NCBI and associated databases. The requested genome accession to download is downloaded as a zip file to `/tmp`, then each FASTA file is extracted and uploaded to an s3 bucket to be used by the isslCreation and Bowtie2 modules. If this code is run on an EC2 instance via the ec2_ncbi.py function, the fasta files will also be saved to a temporary directory for the isslCreation and Bowtie2 modules so they don't redownload the fasta files from s3. SQS sending is also disabled for this module when in EC2 mode.

This module requires the "CommonFuncs", "Ncbi" and "Lib" layers to function as expected.

## bowtie2
The bowtie2 module uses the [Bowtie 2 tool](https://bowtie-bio.sourceforge.net/bowtie2/index.shtml) to create fast and memory-efficient aligned sequence indexes used for analysis of the genome by [Crackling standalone](https://github.com/bmds-lab/Crackling) which is then stored in the genome S3 bucket. 

Once the bowtie 2 tool has been run successfully, the resulting files are uploaded to the genome S3 bucket, then the s3_success() function is run to create a  file which is required for the s3check module.

This module requires the "Bt2Lib", "Bt2Bin", "CommonFuncs" and "Lib" layers to function as expected.

It is worth noting that "LD_LIBRARY_PATH" used intentionally puts the Bt2Lib folder "libs" before any other library directory as it contains a more recent version of the "libstdc++.so.6" than the "lib" layer has which is required for other modules, so it is used instead of the default version. If this package is not included, the module will fail to run the bowtie2 tool.

When running on an ec2, the temporary directory where all the fasta files are stored will be used instead of downloading the file from the s3 bucket. SQS sending is also disabled for this module when in EC2 mode.

## isslCreation
the isslCreation module uses parts of the [Crackling standalone codebase](https://github.com/bmds-lab/Crackling) to create both a "extractofftargets" and a ISSL index file required for the issl/offtarget-scoring module. Firstly, the "extractOfftargets.py" utility from Crackling standalone, which has been modified to run on a lambda function, is used to create an offtargets file. This offtargets file is needed for the input of the "isslCreateIndex" binary that was compiled from the "isslCreateIndex.cpp" source file in Crackling standalone, which creates the ".issl" index file.

Once the above code has been run successfully, the resulting files are uploaded to the genome S3 bucket, then the s3_success() function is run to create a `issl.notif` file which is required for the s3check module.

This module requires the "CommonFuncs", "IsslCreation" and "Lib" layers to function as expected.

When running on an ec2, the temporary directory where all the fasta files are stored will be used instead of downloading the file from the s3 bucket. Additionally, if running on an EC2, the unmodified, faster version of the the "extractOfftargets.py" utility is used. SQS sending is also disabled for this module when in EC2 mode.


## s3Check
Where SQS Queues are used to trigger one or more lambdas from a single lambda, in essence creating new threads, the s3Check acts as a thread join in a typical multi-threaded program, waiting for both the bowtie2 and isslCreation *threads* to finish execution before continuing. This module is triggered via the genome storage S3 bucket, more specifically on files that have the ".notif" (short for notification) suffix or file extension for a given accession. If only one of either the `bt2.notif` or `issl.notif` files are present, the lambda will just terminate execute, but if both files are present, the contents contained in the files is read (necessary data from JobsDB table; both ".notif" files contain identical data), the files are deleted and replaced with a "success" file, then the file content is sent as the body of an SQS message to trigger the next lambda in the cloud formation which is the targetScan module. 

This module requires the "CommonFuncs" layer to function as expected.