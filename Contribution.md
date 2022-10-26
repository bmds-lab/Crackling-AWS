# Contribution File

This file is intended to show the contributions that were made.

## Nick Meurant

I mostly worked on the EC2 aspect of this project, I implemented the EC2Stack and was responsible for creating the schedulder lambda function.  I spent quite alot of time benchmarking and running statistical analysis to create an accurate estimation for the needed RAM from initial file size.  I also spent alot of time working with Mackenzie porting his lambda function code so that it could correctly work within an EC2 environment.

## Mackenzie Wilkins

I developed the following modules/lambda functions:
- downloader
- bowtie2
- isslCreation
- s3Check.

I also modified the targetScan, issl and createJob modules/lambda functions as part of integrating the above, developed lambda functions into the cloud architecture.

I created the *ec2_nbci* module with help from Nick Meurant, to be able to run the downloader, bowtie2 and isslCreation modules on an EC2 instance.

I created the following lambda layers to satisfy dependencies for lambda functions:
- bt2Bin
- bt2Lib
- commonFuncs (python module with common functions used by lambdas)
- isslCreation
  - Including a modified sequential version of *extractOfftargets.py* from [Crackling standalone](https://github.com/bmds-lab/Crackling) to run on a lambda
- ncbi (not in git repo, must be downloaded via instructions in layers/README.md).

*lib* lambda layer was also updated with more recent and additional "shared objects" library files.

Assisted Nick with EC2 part of the project when required.