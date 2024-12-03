# Modules
These modules are the code that is run both in the lambda functions of the normal stack and in the EC2 used for larger genomes.

## Downloader
This module uses the [NCBI Datasets](https://github.com/ncbi/datasets) python module to download genomes from the NCBI and associated databases. The requested genome accession to download is downloaded as a zip file to `/tmp`, then each FASTA file is extracted and uploaded to an s3 bucket to be used by the isslCreation and Bowtie2 modules. This module will also check S3 to confirm if the files already exist before downloading
This module requires the "CommonFuncs", "Ncbi" and "Lib" layers to function as expected.


## isslCreation
the isslCreation module uses parts of the [Crackling standalone codebase](https://github.com/bmds-lab/Crackling) to create both a "extractofftargets" and a ISSL index file required for the issl/offtarget-scoring module. Firstly, the "extractOfftargets.py" utility from Crackling standalone, which has been modified to run on a lambda function, is used to create an offtargets file. This offtargets file is needed for the input of the "isslCreateIndex" binary that was compiled from the "isslCreateIndex.cpp" source file in Crackling standalone, which creates the ".issl" index file.

Once the above code has been run successfully, the resulting files are uploaded to the genome S3 bucket. SQS is then used to initiate the TargetScan function

This module requires the "CommonFuncs", "IsslCreation" and "Lib" layers to function as expected.

## TargetScan
This function is in charge of extracting target sequences (23-length long) from the initial DNA query by splitting it. Each target sequence is scored by both on-target (consensus) and off-target (issl) and it uses two queues to initiate each scoring function.

## issl
This is a scoring function for "off-target" in CRISPR-Cas9. The function consumes a batch from ISSL_SQS (input) which contains the genome accession, sequence and target guide. The max size of the batch consists of 10 records due to memory as well as storage constraint limitations. More importantly, this function scales out by running multiple instances of itself with different sqs batches (achieving parallelism).

This function determines if the each genome accession in the batch can downloaded into the ephemeral storage of lambda (10GB), determining which genomes to keep. The ones which exceed available space are sent back into the queue, to be picked by a future lambda instance. The result/scores (ouputs) are sent to DynamoDB for storage to be queried by the website.

This lambda function depends on the ISSL file created in isslCreation to be used as input for scoring. The genome accession is used to sort and structure differing jobs in a batch.

## consensus
This is a scoring function for "on-target" in CRISPR-Cas9. The function uses three existing libraries like CHOPCHOPm sgRNAScorer2.0, mm10db to determine its appropriateness. The function consumes a batch from CONSENSUS_SQS (input) which contains the same information as issl function. Compared to issl, the max size of the batch is 100 records due to less intensive procedures required. Similarly to issl, this function exhibits parallelism achieved via the sqs batches.
The function is very quick to run ~2 seconds at most. Similarly, the scores are sent to DynamoDB for access by website query. The jobid is used to sort and structure differing jobs in a batch. 
