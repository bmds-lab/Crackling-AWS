# Lambda layers
These lambda layers are the code that is run both in the lambda functions of the normal stack and in the EC2 used for larger genomes.

## bt2Bin
contains all bowtie2 *binary* files (some files are scripts). If you need to update this layer for a new version of bowtie2, the following will need to be done.

```
# run following to get directory the bowtie2 file is in
whereis bowtie2

# run the following to list of all bowtie2 related file names (assuming bowtie is in "/usr/bin" directory)
ls /usr/bin/ | grep bowtie2

# copy all listed files to bowtie2 layer folder. Example for one copy command below
cp /usr/bin/bowtie2-align-l layers/bt2Bin/bin
```

## bt2lib
This layer contains a more recent version of the "libstdc++.so.6" than the "lib" layer has which is required for other modules. The /opt/libs folder should be the first library directory in the "LD_LIBRARY_PATH" environment variable for applicable lambda functions it's library file is used instead of the default version. If this package is not included, the module will fail to run the bowtie2 tool. 

## CommonFuncs
This layer is a custom python module with functions that are used across many lambda functions. This layer allows changes to functions to be consistent across functions, make it easier to interact with other Amazon web services such as SQS or S3 as well as run lambda modules on EC2 instances via recreating the event and context objects used for lambda_handler functions.

S3 write locking (pseudo-mutex) is also implemented to stop multiple files writing to the csv log files at the same time. These functions can be further expanded

## isslCreation
Firstly, this layer contains a modified version of the "extractOfftargets.py" utility from [Crackling standalone](https://github.com/bmds-lab/Crackling). Lambda functions don't properly support the python mutliprocessing module which the original extractOfftargets utility made use of, therefore the "startMutliprocessing" function of the original version was converted to "startSequentalprocessing" which has updated the function to not use a mutliprocessing pool and instead use maps where appropriate. Though this version of the code is slower than the orginial mutliprocessing approach, it is required to work at all on lambdas and required less development time than completely rewriting the utility in C++ (for example) to enable parallelization. Other helper python scripts that "extractOfftargets.py" uses are also present in this layer.

The "isslCreateIndex" binary was compiled from the "isslCreateIndex.cpp" source file in Crackling standalone, which creates the ".issl" index file.

## ncbi Layer

To get the "ncbi-datasets-pylib" python package for this layer, code similar to the following needs to be run to install the package into the correct layer folder. This layer is required for the Scheduler and Downloader code.

```
mkdir layers/ncbi/python
py -3 -m pip install --target layers/ncbi/python -r layers/ncbi_reqs.txt
```