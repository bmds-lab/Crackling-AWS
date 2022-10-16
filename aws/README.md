
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

# EC2STACK

The first stack located in "ec2App.py" is a stack that is created purely for the creation of an ec2 instance with everything needed pre-installed.  The CracklingStackEC2Nick runs on the default vpc.  The EC2 allows inbound ssh from any ip address.

Since the files are being initilialised by the cdk, cloud formation needs to be installed on the ubuntu ami (ubuntu doesnt have this by default), this is achieved by adding commands into the user data of the linux ami before assigning it to an ec2 (MUST BE DONE BEFORE CREATING THE EC2_INSTANCE)

The linux AMI ssm parameter is configured to always retrieve the most up to date ubuntu 20.04 x86 version.

The ec2 needs bt2Lambda.py, common_funcs.py, issl_creation.py, lambda_downloader.py to be located in the "/ec2Code/modules" directoy and ec2_ncbi.py to be in the "/ec2Code" directory.

The isslcreateIndex must be sent over as well, ensure that this is base65 encoded to allow it to be sent through the template

There is a bash script called "init.sh", this file should be located in the root directory of the instance.  This init.sh is ran by a shell command that calls it through using bash.  Please ensure that the EOL characters in the init.sh file are unix based and not dos based (can also used dos2unix during the init shell commands).  DOS based EOL characters will cause the cdk stack to not be initialised.

The ec2 instance can be generated on a t2.Micro, since spawning the ec2 is only used to generate an AMI.

Once everything has been ran and the ec2 is verified to have been deployed and running with 2/2 status checks you will have to manually create an AMI of the newly created ec2 instance.

By default make the ec2 AMI 8gb EBS (this EBS will be extended when calling it from the scheduler).

Once the image is created successfully, delete the ec2 and paste in the ID of the AMI into the "lambdaScheduler.environment.AMI section present in app.py".  Sadly this process couldnt be made automatic since a new ec2/AMI doesnt need to be created everytime the app.py stack is redeployed.

# CracklingStack

The CrackligStack is the main stack and is responsible for deploying the frontend, api endpoints, s3 buckets and all the various lambda functions that are used.

The crackline is now split into small lambda subsections that push info to either s3 buckets or to dynamo db tables that will trigger another lambda function.

The crackling frontend is saved and deployed into an s3 bucket








