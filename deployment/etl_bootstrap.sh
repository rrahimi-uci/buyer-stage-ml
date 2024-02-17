#!/bin/bash

set -x
set -e

export PYSPARK_PYTHON=/usr/bin/python3.6
export PYSPARK_DRIVER_PYTHON=/usr/bin/python3.6
python --version

CODE_SRC=$1

echo "Code Source: $CODE_SRC"
mkdir -p /mnt/buyer_stage_ml
aws s3 cp $CODE_SRC /mnt/buyer_stage_ml/buyer_stage_ml.zip

cd /mnt/buyer_stage_ml
unzip -o buyer_stage_ml.zip
rm -rf buyer_stage_ml.zip

echo "/mnt/buyer_stage_ml"
ls -al /mnt/buyer_stage_ml
ls -al /mnt/buyer_stage_ml/src_etl

export PYTHONPATH=$PYTHONPATH:/mnt/buyer_stage_ml/src_etl

sudo pip-3.6 install --upgrade setuptools
sudo pip-3.6 install wheel
sudo pip-3.6 install --upgrade -r requirements.txt

cd /mnt/buyer_stage_ml/src_etl
zip -r buyer_stage_ml.zip *

sudo chmod a+x /mnt/buyer_stage_ml/deployment/etl_emr_spark_submit.sh

python --version

echo "DONE"