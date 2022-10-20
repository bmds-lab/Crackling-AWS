
# Welcome to your CDK Python project!

This is a blank project for Python development with CDK.

The `cdk.json` file tells the CDK Toolkit how to execute your app.

This project is set up like a standard Python project.  The initialization
process also creates a virtualenv within this project, stored under the `.venv`
directory.  To create the virtualenv it assumes that there is a `python3`
(or `python` for Windows) executable in your path with access to the `venv`
package. If for any reason the automatic creation of the virtualenv fails,
you can create the virtualenv manually.

To manually create a virtualenv on MacOS and Linux:

```
$ python3 -m venv .venv
```

After the init process completes and the virtualenv is created, you can use the following
step to activate your virtualenv.

```
$ source .venv/bin/activate
```

If you are a Windows platform, you would activate the virtualenv like this:

```
% .venv\Scripts\activate.bat
```

Once the virtualenv is activated, you can install the required dependencies.

```
$ pip install -r requirements.txt
```

At this point you can now synthesize the CloudFormation template for this code.

```
$ cdk synth
```

To add additional dependencies, for example other CDK libraries, just add
them to your `setup.py` file and rerun the `pip install -r requirements.txt`
command.

## Useful commands

 * `cdk ls`          list all stacks in the app
 * `cdk synth`       emits the synthesized CloudFormation template
 * `cdk deploy`      deploy this stack to your default AWS account/region
 * `cdk diff`        compare deployed stack with current state
 * `cdk docs`        open CDK documentation

Enjoy!


# CDK STACK INFO

There are two seperate stacks that are used to create the Crackling pipeline.  

## EC2STACK

The first stack located in "ec2App.py" is a stack that is created purely for the creation of an ec2 instance with everything needed pre-installed.  The CracklingStackEC2Nick runs on the default vpc.  The EC2 allows inbound ssh from any ip address.

Since the files are being initilialised by the cdk, cloud formation needs to be installed on the ubuntu ami (ubuntu doesnt have this by default), this is achieved by adding commands into the user data of the linux ami before assigning it to an ec2 (MUST BE DONE BEFORE CREATING THE EC2_INSTANCE)

The linux AMI ssm parameter is configured to always retrieve the most up to date ubuntu 20.04 x86 version.

The ec2 needs bt2Lambda.py, common_funcs.py, issl_creation.py, lambda_downloader.py to be located in the "/ec2Code/modules" directoy and ec2_ncbi.py to be in the "/ec2Code" directory.

The isslcreateIndex must be sent over as well, ensure that this is base65 encoded to allow it to be sent through the template

There is a bash script called "init.sh", this file should be located in the root directory of the instance.  This init.sh is ran by a shell command that calls it through using bash.  Please ensure that the EOL characters in the init.sh file are unix based and not dos based (can also used dos2unix during the init shell commands).  DOS based EOL characters will cause the cdk stack to not be initialised.

The ec2 instance can be generated on a t2.Nano, since spawning the ec2 is only used to generate an AMI and does not need a lot of resources, and minimizes cost, especially if the EC2 is forgotten about and not deleted *AFTER* AMI creation (steps below).

### EC2Stack Deployment/Usage Steps

To successfully deploy the EC2Stack and create the AMI, complete the following steps (assuming above setup steps for cdk install/setup have been completed):

1. cd to `Crackling-AWS/aws` directory
2. modify `cdk.json` line 2 to read `"app": "python3 ec2App.py"` (instead of `"app": "python3 app.py"`)
3. run `cdk deploy` to create stack and EC2
4. goto EC2 management console and find EC2 instance named "EC2forAMI"
5. wait for ec2 instance to be fully deployed/verified to have been deployed and running with 2/2 status checks
6. Create AMI via right-click menu (more detailed steps found [here](https://docs.aws.amazon.com/toolkit-for-visual-studio/latest/user-guide/tkv-create-ami-from-instance.html))
7. Wait roughly 20 minutes for AMI to be created
8. Copy AMI ID
9. Terminate EC2 *AFTER* AMI creation has finished
10. Replace lambdaScheduler's old "AMI" environment variable value with new AMI ID, found in `Crackling-AWS/aws/app.py`
11. Run `cdk destroy` to delete stack as no longer needed
12. Revert Step 2 (change line back to "app.py").

By default make the ec2 AMI 8gb EBS which is fine as CracklingStack will make use of EC2 instances with additional attached storage (R5ad) for temporary data.

Repeat the above steps if the any of the following modules are modified, to have the ec2 instance reflect modifications:
- bowtie2
- downloader
- ec2_ncbi
- isslCreation

## CracklingStack

The CracklingStack is the main stack and is responsible for deploying the frontend, api endpoints, s3 buckets and all the various lambda functions that are used.

The Crackling-AWS architecture is segmented into lambda functions to calculate results against a genome. These lambda functions will interact with other AWS services such as s3 buckets, dynamo db tables or to sqs queues that allow other lambda functions to be triggered progress through the cloud formation.

Simple Queue Service is used in the formation to trigger the next lambda/s after the first one has finished, sending necessary information from one AWService to the next. Dynamo DB tables and streams are used in a similar way as SQS, to push events from lambda to the next, as well as store data from runs in a database form.

The crackling frontend is saved and deployed into an s3 or Simple Storage Service bucket, as well as another s3 bucket for storage of the genome files downloaded and generated by the stack.

Since the lambda functions need to use external libraries, python packages and files etc which aren't installed to the typical lambda instance, these files are uploaded as *lambda layers* which can then be attached to each lambda as needed at runtime. 

For more information on either the lambda function code or lambda layers, go to the `../modules` or `../layers` directories for additional readme files.












