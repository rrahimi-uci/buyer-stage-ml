#!/bin/bash

echo "Spark Submit: *******************"

spark-submit --deploy-mode cluster \
--conf spark.scheduler.mode=FAIR \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=python36 \
--conf spark.executorEnv.PYSPARK_PYTHON=python36 \
--py-files /mnt/buyer_stage_ml/src_etl/buyer_stage_ml.zip \
/mnt/buyer_stage_ml/src_etl/main.py \
"${1}" \
"${2}" \
"${3}" \
"${4}" 

PID=$! # catch the last PID, here from the above command
echo "PID of the spark-submit job: $PID"
wait $PID # wait for the above command