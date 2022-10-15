#!/bin/bash

sudo apt-get update

dir=/ec2Code

cd /

sudo mkkdir ${dir}

export DEBIAN_FRONTEND=noninteractive

echo "aptget install required files"

sudo apt-get install bowtie2 libtbb-dev unzip python3-pip python3.8-venv -y

cd ${dir}

echo "setting up venv"
echo "setting up venv"
echo "setting up venv"
echo "setting up venv"
echo "setting up venv"
echo "setting up venv"
echo "setting up venv"
echo "setting up venv"
echo "setting up venv"

ve() {
    local py=${1:-python3}
    local venv="${2:-${dir}/.venv}"

    local bin="${venv}/bin/activate"

    # If not already in virtualenv
    # $VIRTUAL_ENV is being set from $venv/bin/activate script
      if [ -z "${VIRTUAL_ENV}" ]; then
        if [ ! -d ${venv} ]; then
            echo "Creating and activating virtual environment ${venv}"
            ${py} -m venv ${venv} --system-site-package
            echo "export PYTHON=${py}" >> ${bin}    # overwrite ${python} on .zshenv
            source ${bin}
            echo "Upgrading pip"
            ${py} -m pip install --upgrade pip

            ${py} -m pip install --upgrade setuptools

            ${py} -m pip install boto3

            ${py} -m pip install ncbi-datasets-pylib

            ${py} -m pip install joblib
        else
            echo "Virtual environment  ${venv} already exists, activating..."
            source ${bin}
        fi
    else
        echo "Already in a virtual environment!"
    fi
}

ve


sudo chmod 777 -R ${dir}

sudo wget -O ${dir}/2.zip https://github.com/bmds-lab/Crackling/archive/refs/heads/dev/fix-max-open-files-offtarget-extraction.zip 
sudo unzip -o ${dir}/2.zip -d ${dir}
cd ${dir}/Crackling-dev-fix-max-open-files-offtarget-extraction
sudo g++ -o ../isslCreateIndex src/ISSL/isslCreateIndex.cpp  -fopenmp -std=c++11 -I src/ISSL/include
cd ${dir}
sudo rm -r ${dir}/src/
sudo mv ${dir}/Crackling-dev-fix-max-open-files-offtarget-extraction/src ${dir}
sudo rm ${dir}/2.zip
sudo rm -r ${dir}/Crackling-dev-fix-max-open-files-offtarget-extraction
