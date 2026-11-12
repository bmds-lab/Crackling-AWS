# Lambda layers

This directory contains layers that are included in the AWS Lambda run-time environment.

We cannot provide all layers, due to distribution restrictions in the licences of each open source component. You will need to obtain these yourself.

The layers are lsited below, starting with those that you need to build yourself.

## `ncbi`

**You will need to build this layer.**

The list of assets needed in this layer are provided in `requirements_ncbi.txt`. 

To get the "ncbi-datasets-pylib" python package for this layer, code similar to the following needs to be run to install the package into the correct layer folder.

```
mkdir -p layers/ncbi/python
py -3 -m pip install --target layers/ncbi/python -r layers/requirements_ncbi.txt
```

## `lib`

**You will need to build this layer.**

Collect all shared objects needed by compiled binaries.

See here: https://www.commandlinefu.com/commands/view/10238/copy-all-shared-libraries-for-a-binary-to-directory

Working in the root directory of the repo, run:

```bash
ldd layers/isslMapper/mapper | grep "=> /" | awk '{print $3}' | xargs -I '{}' cp -v '{}' layers/sharedObjects
```

then

```bash
ldd layers/rnaFold/rnaFold/RNAfold | grep "=> /" | awk '{print $3}' | xargs -I '{}' cp -v '{}' layers/sharedObjects
```

## `commonFuncs`

This layer has been curated by the authors of this repository.

This layer is a custom python module with functions that are used across many lambda functions. This layer allows changes to functions to be consistent across functions, make it easier to interact with other Amazon web services such as SQS or S3 as well as run lambda modules on EC2 instances via recreating the event and context objects used for lambda_handler functions.

S3 write locking (pseudo-mutex) is also implemented to stop multiple files writing to the csv log files at the same time. These functions can be further expanded

## `isslCreation`

This layer has been curated by the authors of this repository.

This layer contains a modified version of the "extractOfftargets.py" utility from [Crackling standalone](https://github.com/bmds-lab/Crackling). Lambda functions don't properly support the python mutliprocessing module which the original extractOfftargets utility made use of, therefore the "startMutliprocessing" function of the original version was converted to "startSequentalprocessing" which has updated the function to not use a mutliprocessing pool and instead use maps where appropriate. Though this version of the code is slower than the orginial mutliprocessing approach, it is required to work at all on lambdas and required less development time than completely rewriting the utility in C++ (for example) to enable parallelization. Other helper python scripts that "extractOfftargets.py" uses are also present in this layer.

The "isslCreateIndex" binary was compiled from the "isslCreateIndex.cpp" source file in Crackling standalone, which creates the ".issl" index file.

## `isslMapper`

This layer has been curated by the authors of this repository.

This layer contains a precompiled binary. More information in the README file within the directory.

## `rnaFold`

This layer has been curated by the authors of this repository.

This layer has the compiled binary of RNAfold from the ViennaRNA package.

```bash
$ layers/rnaFold/rnaFold/RNAfold --version
RNAfold 2.4.14
```

This has been made available in our repository as the licence does not specify conditions preventing redistribution, but importantly, we need to acknowledge the hugely valuable contributions of the Institute for Theoretical Chemistry of the University of Vienna.

See here: https://github.com/ViennaRNA/ViennaRNA/blob/master/COPYING

## `sgrnascorer2model`

This layer has been curated by the authors of this repository.

We have provided a pre-trained sgRNA Scorer 2.0 model. This model was published in ACS Synthetic Biology, 2017.

There is no attached licence, yet we acknowledge the important contributions that the authors have made to the CRISPR community by providing the data and source code to train the model.

> Chari, R., Yeo, N. C., Chavez, A., & Church, G. M. (2017). sgRNA Scorer 2.0: a species-independent model to predict CRISPR/Cas9 activity. ACS Synthetic Biology, 6(5), 902-904.

The source code to train the model can be downloaded here:
- https://frederick.cancer.gov/resources/repositories/sgrnascorer
- https://frederick.cancer.gov/downloads/sgRNAScorer.2.0.zip

