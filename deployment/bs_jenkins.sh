#!/bin/bash
ACCOUNT="${ACCOUNT_NAME}-${ACCOUNT_ENV}"

STACK_NAME="${STACKTYPE}-${ACCOUNT}-${BRANCH}"

echo "Setup Account Number Hash..."
declare -A ACCOUNT_NUMBERS

ACCOUNT_NUMBERS["moverdc-dev"]="425555124585"
ACCOUNT_NUMBERS["moverdc-qa"]="337683724535"
ACCOUNT_NUMBERS["moverdc-prod"]="747559966630"

ACCOUNT_NUMBERS["dataeng-dev"]="289154003759"
ACCOUNT_NUMBERS["dataeng-qa"]="609158398538"
ACCOUNT_NUMBERS["dataeng-prod"]="057425096214"

echo "Getting AWS Account Number For Environment..."
ACCOUNT_NUMBER=${ACCOUNT_NUMBERS[${ACCOUNT}]};
echo "$ACCOUNT_NUMBER"

declare -A TAG_SET
TAG_SET["owner"]="dltechnologyconsumerprofile@move.com"
TAG_SET["product"]="consumer_profile"
TAG_SET["component"]="buyer-stage-ml"
TAG_SET["environment"]="${ACCOUNT_ENV}"
TAG_SET["classification"]="internal"

echo "Getting AWS Credentials..."
aws sts assume-role --role-arn=arn:aws:iam::${ACCOUNT_NUMBER}:role/User \
                    --role-session-name="userRole" \
                    --region=us-west-2 > ${WORKSPACE}/credentials.txt
export AWS_SECRET_ACCESS_KEY=`grep SecretAccessKey credentials.txt |cut -d"\"" -f4`
export AWS_SESSION_TOKEN=`grep SessionToken credentials.txt |cut -d"\"" -f4`
export AWS_ACCESS_KEY_ID=`grep AccessKeyId credentials.txt |cut -d"\"" -f4`
export AWS_SECURITY_TOKEN=$AWS_SESSION_TOKEN

cd ${WORKSPACE}
echo " The workspace is ${WORKSPACE}"
OUTPUTFILE=output.txt

if [ ${STACKTYPE} == "buyer-stage-etl-pipeline" ]; then
  TEMPLATE=file://${WORKSPACE}/deployment/bs_etl_pipeline_cloudformation_template.yml
fi

if [ ${STACKTYPE} == "buyer-stage-ml-pipeline" ]; then
  TEMPLATE=file://${WORKSPACE}/deployment/bs_ml_pipeline_cloudformation_template.yaml
fi

zip_lambdas(){

cd ${WORKSPACE}
echo " The workspace is ${WORKSPACE}"

cd src_ml
# make a directory for saving lambdas zip source codes
mkdir -p lambda_functions_zip;

# zipping lambdas source code
cat << EOF > bs_zip_lambda_codes.py
#!/usr/bin/python
# This module zips lambdas
from shutil import make_archive


lambda_functions_list = ['bs_lambda_check_automl_status', 'bs_lambda_check_batch_transformation_status',
                         'bs_lambda_check_if_daily_data_available', 'bs_lambda_check_if_model_available',
                         'bs_lambda_check_if_training_data_available', 'bs_lambda_create_and_save_model',
                         'bs_lambda_send_sns_notification', 'bs_lambda_start_automl',
                         'bs_lambda_start_batch_stransformation',
                         'bs_lambda_check_uploading_to_dynamo_db_status',
                         'bs_lambda_start_uploading_to_dynamo_db']

lambda_source_dir = 'lambda_functions/'
lambda_source_zip = 'lambda_functions_zip/'

def zip_lambdas():
    for lambdas in lambda_functions_list:
        make_archive(lambda_source_zip + lambdas, 'zip', lambda_source_dir + lambdas)

if __name__ == "__main__":
    zip_lambdas()

EOF

chmod 755 bs_zip_lambda_codes.py

./bs_zip_lambda_codes.py

}

deploy_on_s3_ml() {

    cd ${WORKSPACE}
    echo " The workspace is ${WORKSPACE}"
    echo "Ensuring S3 Bucket and Pushing S3..."
    aws s3 mb s3://rdc-buyer-stage-ml-${ACCOUNT_ENV} --region us-west-2 || true
    aws s3api put-bucket-tagging --bucket rdc-buyer-stage-ml-${ACCOUNT_ENV}\
        --tagging "TagSet=[{Key=owner,Value=${TAG_SET[owner]}}, {Key=product,Value=${TAG_SET[product]}}, {Key=component,Value=${TAG_SET[component]}}, {Key=environment,Value=${TAG_SET[environment]}}, {Key=classification,Value=${TAG_SET[classification]}} ]"
    #copy glue codes, lambda codes, initial training set,
    aws s3 cp src_ml/bs_push2ddb_glue.py s3://rdc-buyer-stage-ml-${ACCOUNT_ENV}/ml-pipeline-codes/
    aws s3 cp src_ml/lambda_functions_zip s3://rdc-buyer-stage-ml-${ACCOUNT_ENV}/ml-pipeline-codes/ --recursive
    aws s3 cp sample_data/buyer_stage_tr_val_set.csv s3://rdc-buyer-stage-ml-${ACCOUNT_ENV}/train/csv/
    aws s3 cp sample_data/buyer_stage_test_set.csv s3://rdc-buyer-stage-ml-${ACCOUNT_ENV}/test/with-target/
}

update_Lambda_codes_ml(){

    cd ${WORKSPACE}
    echo " The workspace is ${WORKSPACE}"
    echo "updating lambdas codes ... "
    aws s3 cp src_ml/lambda_functions_zip s3://rdc-buyer-stage-ml-${ACCOUNT_ENV}/ml-pipeline-codes/ --recursive

    # Declare lambda function names to be updated
    declare -a StringArray=("bs_lambda_check_automl_status" \
                            "bs_lambda_check_batch_transformation_status" \
                            "bs_lambda_check_if_daily_data_available" \
                            "bs_lambda_check_if_model_available" \
                            "bs_lambda_check_if_training_data_available" \
                            "bs_lambda_check_uploading_to_dynamo_db_status"\
                            "bs_lambda_create_and_save_model"\
                            "bs_lambda_send_sns_notification"\
                            "bs_lambda_start_automl"\
                            "bs_lambda_start_batch_stransformation"\
                            "bs_lambda_start_uploading_to_dynamo_db"
                            )

    # Iterate through lambda functions and update the code
    for item in ${StringArray[@]}; do
       echo "Updating Lambda..." ${item}
        aws lambda update-function-code \
        --function-name  ${item} \
        --s3-bucket rdc-buyer-stage-ml-${ACCOUNT_ENV}\
        --s3-key ml-pipeline-codes/${item}.zip\
        --region us-west-2
    done
}

echo ${PROJECT}
TAGS_LIST="Key=owner,Value=${TAG_SET['owner']} Key=product,Value=${TAG_SET['product']} Key=component,Value=${TAG_SET['component']} Key=environment,Value=${TAG_SET['environment']}  Key=classification,Value=${TAG_SET['classification']}"

if [ ${STACKTYPE} == "buyer-stage-ml-pipeline" ]; then
    if [ ${ACTION} == "create-stack" ] || [ ${ACTION} == "update-stack" ]; then
       zip_lambdas
       deploy_on_s3_ml
       aws cloudformation ${ACTION} --stack-name buyer-stage-ml-pipeline-stack\
        --template-body ${TEMPLATE} \
        --capabilities CAPABILITY_NAMED_IAM \
        --parameters ParameterKey=Env,ParameterValue=${ACCOUNT_ENV}\
          ParameterKey=S3BucketName,ParameterValue=rdc-buyer-stage-ml-${ACCOUNT_ENV}\
          ParameterKey=DynamoDbTableName,ParameterValue=buyer-stage-ml-pipeline-results-${ACCOUNT_ENV}\
        --tags ${TAGS_LIST}\
        --region us-west-2 2>&1
    elif [ ${ACTION} == "update-lambdas" ]; then
       zip_lambdas
       update_Lambda_codes_ml
    elif [ ${ACTION} == "start-ml-pipeline" ]; then
       aws stepfunctions start-execution\
           --state-machine-arn arn:aws:states:us-west-2:${ACCOUNT_NUMBER}:stateMachine:BuyerStageMLPipeline\
           --region us-west-2
    elif [ ${ACTION} == "delete-ml-model" ]; then
       aws s3 rm s3://rdc-buyer-stage-ml-${ACCOUNT_ENV}/best-model/buyer_stage.pkl
    elif [ ${ACTION} == "delete-stack" ]; then
       aws cloudformation ${ACTION} --stack-name buyer-stage-ml-pipeline-stack --region us-west-2 2>&1
    else
       echo "No Operation for buyer-stage-ml-pipeline ... "
    fi
fi

if [ ${ACTION} == "create-stack" ] && [ ${STACKTYPE} == "buyer-stage-etl-pipeline" ]; then
  echo ${ACTION}
  echo ${STACKTYPE}
  zip -r buyer_stage_ml.zip *
  aws s3 sync . $CODE_S3_PATH/${BRANCH}_${ACCOUNT}/

  echo "Params:"
  echo ${ACCOUNT}
  echo ${STEPS_TO_RUN}

  aws cloudformation ${ACTION} --stack-name ${STACK_NAME} --parameters \
  ParameterKey=CodeSource,ParameterValue="$CODE_S3_PATH/${BRANCH}_${ACCOUNT}/buyer_stage_ml.zip" \
  ParameterKey=BootstrapSource,ParameterValue="$CODE_S3_PATH/${BRANCH}_${ACCOUNT}/deployment/etl_bootstrap.sh" \
  ParameterKey=Env,ParameterValue=${ACCOUNT_ENV} \
  ParameterKey=SrcDataBucket,ParameterValue=${SRC_DATA_BUCKET} \
  ParameterKey=DstDataBucket,ParameterValue=${DST_DATA_BUCKET} \
  ParameterKey=StepsToRun,ParameterValue=${STEPS_TO_RUN} \
  --template-body ${TEMPLATE} \
  --capabilities CAPABILITY_IAM \
  --tags ${TAGS_LIST} \
  --region us-west-2 2>&1 | tee output.txt
fi

if [ ${ACTION} == "delete-stack" ] && [ ${STACKTYPE} == "buyer-stage-etl-pipeline" ]; then
  echo "Deleting ${STACK_NAME}"
  aws cloudformation ${ACTION} --stack-name ${STACK_NAME} --region us-west-2
  aws s3 rm --recursive $CODE_S3_PATH/${BRANCH}_${ACCOUNT}/
  aws s3 rm $CODE_S3_PATH/${BRANCH}_${ACCOUNT}
fi