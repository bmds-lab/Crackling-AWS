# Crackling AWS

This is a cloud-based implementation of the Crackling pipeline, for design of efficient and specific CRISPR Cas9 guides. This implementation utilises serverless technologies made available by Amazon Web Services (AWS).



Jacob Bradford<sup>1</sup>, Timothy Chappell<sup>1</sup>, Brendan Hosking<sup>2</sup>, Laurence Wilson<sup>2</sup>, Dimitri Perrin<sup>1</sup>
<sup>1</sup> Queensland University of Technology, Brisbane, Australia 
<sup>2</sup> Commonwealth Scientific and Industrial Research Organisation (CSIRO), Sydney, Australia 



## Preamble

The standalone implementation is available on GitHub, [here](https://github.com/bmds-lab/Crackling).

Please refer to our paper when using Crackling:

> **Faster and better CRISPR guide RNA design with the Crackling method**
>
> Jacob Bradford, Timothy Chappell, Dimitri Perrin
>
> bioRxiv 2020.02.14.950261; doi: https://doi.org/10.1101/2020.02.14.950261

> The design of CRISPR-Cas9 guide RNAs is not trivial, and is a computationally demanding task. Design tools need to identify target  sequences that will maximise the likelihood of obtaining the desired  cut, whilst minimising off-target risk. There is a need for a tool that  can meet both objectives while remaining practical to use on large  genomes.
>
> Here, we present Crackling, a new method that is more suitable for meeting these objectives. We test its performance on  12 genomes and on data from validation studies. Crackling maximises  guide efficiency by combining multiple scoring approaches. On  experimental data, the guides it selects are better than those selected  by others. It also incorporates Inverted Signature Slice Lists (ISSL)  for faster off-target scoring. ISSL provides a gain of an order of  magnitude in speed, while preserving the same level of accuracy.  Overall, this makes Crackling a faster and better method to design guide RNAs at scale.
>
> Crackling is available at https://github.com/bmds-lab/Crackling under the Berkeley Software Distribution (BSD) 3-Clause license.



## Architecture



## Deployment

Collect all shared objects needed by compiled binaries.

See here: https://www.commandlinefu.com/commands/view/10238/copy-all-shared-libraries-for-a-binary-to-directory

```
ldd layers/isslScoreOfftargets/isslScoreOfftargets | grep "=> /" | awk '{print $3}' | xargs -I '{}' cp -v '{}' layers/sharedObjects

ldd layers/rnaFold/rnaFold/RNAfold | grep "=> /" | awk '{print $3}' | xargs -I '{}' cp -v '{}' layers/sharedObjects
```

### Consensus

```
py -3 -m pip install --target layers/consensusPy38Pkgs/python -r modules/consensus/requirements.txt
```




The consensus module has Python dependencies that need to be installed.

They need to be installed and packaged locally before deploying to AWS:

```
cd modules/consensus

tar.exe -czf deployment.package.zip package/*
```

Read more in the AWS documentation: https://docs.aws.amazon.com/lambda/latest/dg/python-package.html#python-package-dependencies

On Windows (with Windows Python Launcher installed):

```
cd modules/consensus

py -3 -m venv .venv

.venv\Scripts\activate.bat

pip install --target ./package joblib sklearn

pip freeze > requirements.txt

pip install -r requirements.txt
```

If you make changes to the dependencies, make sure the `requirements.txt` file is updated:

```
cd modules/consensus

pip freeze > requirements.txt
```

## Development



## References

