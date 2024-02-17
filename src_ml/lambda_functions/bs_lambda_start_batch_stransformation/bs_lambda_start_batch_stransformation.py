import logging
import os
import pickle
import sys
from datetime import datetime, timedelta
from time import gmtime, strftime

import boto3

logger = logging.getLogger('buyer_stage')
logging.getLogger().addHandler(logging.StreamHandler(sys.stderr))
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger.setLevel(logging.INFO)

env = os.environ['ENV']
bucket = os.environ['BUCKET']
best_model_prefix = os.environ['BEST_MODEL_PREFIX']
number_of_days_lag = os.environ['NUMBER_OF_DAYS_LAG']
instance_type = os.environ['INSTANCE_TYPE']
instance_count = os.environ['INSTANCE_COUNT']
max_concurrent_transforms = os.environ['MAX_CONCURRENT_TRANSFORMS']
max_payload_in_mb = os.environ['MAX_PAYLOAD_IN_MB']
batch_strategy = os.environ['BATCH_STRATEGY']
inference_prefix = os.environ['INFERENCE_PREFIX']
input_prefix = os.environ['INPUT_PREFIX']

sagmaker_handler = boto3.client('sagemaker')


def lambda_handler(event, context):
    """
        Description: start batch buyer state batch transformation
        @rtype : dict
        @return : response
    """
    try:
        logger.info("Start Batch Transformation Job")
        logger.info("The content of event is : --->" + str(event))

        # This should get model name from pickle file
        model = unpickle_file_on_s3(bucket, best_model_prefix)
        model_name = model['optimal_model_name']

        timestamp_suffix = strftime('%y-%m-%d-%H-%M-%S', gmtime())
        batch_job_name = 'buyer-stage-batch-trans-' + timestamp_suffix

        input_data_on_s3 = get_input_data_path_on_s3(int(number_of_days_lag))
        inference_output_path_on_s3 = get_inference_data_path_on_s3(int(number_of_days_lag))
        logger.info(inference_output_path_on_s3)

        transform_input = {
            'DataSource': {
                'S3DataSource': {
                    'S3DataType': 'S3Prefix',
                    'S3Uri': input_data_on_s3
                }
            },
            'ContentType': 'text/csv',
            'CompressionType': 'None',
            'SplitType': 'Line'
        }

        transform_output = {
            'Accept': 'text/csv',
            'AssembleWith': 'Line',
            'S3OutputPath': inference_output_path_on_s3
        }

        transform_resources = {
            'InstanceType': '{}'.format(instance_type),
            'InstanceCount': int(instance_count)
        }

        data_processing = {
            'InputFilter': '$[1:]',
            'JoinSource': 'Input',
            'OutputFilter': "$[0,-1]"
        }

        sagmaker_handler.create_transform_job(TransformJobName=batch_job_name,
                                              ModelName=model_name,
                                              MaxConcurrentTransforms=int(max_concurrent_transforms),
                                              MaxPayloadInMB=int(max_payload_in_mb),
                                              BatchStrategy=batch_strategy,
                                              TransformInput=transform_input,
                                              TransformOutput=transform_output,
                                              TransformResources=transform_resources,
                                              DataProcessing=data_processing,
                                              Tags=[
                                                  {
                                                      'Key': 'owner',
                                                      'Value': 'dltechnologyconsumerprofile@move.com'
                                                  },
                                                  {
                                                      'Key': 'product',
                                                      'Value': 'consumer_profile'
                                                  },
                                                  {
                                                      'Key': 'component',
                                                      'Value': 'buyer-stage-ml'
                                                  },
                                                  {
                                                      'Key': 'environment',
                                                      'Value': env
                                                  },
                                                  {
                                                      'Key': 'classification',
                                                      'Value': 'internal'
                                                  },
                                              ]
                                              )

        response = {'statusCode': 200,
                    'status': 'Started',
                    'batch_job_name': batch_job_name,
                    'inference_output_path_on_s3' : inference_output_path_on_s3,
                    'from': 'bs_lambda_start_batch_stransformation'}
        return response

    except Exception as message:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logger.error(
            "filename = %s - function = bs_lambda_start_batch_stransformation in line_number = %s - hint = %s " % (
                f_name, exc_tb.tb_lineno, message.__str__()))

        response = {'statusCode': 200,
                    'status': 'Exception',
                    'exception-message': message.__str__(),
                    'from': 'bs_lambda_start_batch_stransformation'}
        return response


def unpickle_file_on_s3(s3bucket, prefix):
    try:
        s3 = boto3.resource('s3')
        best_model_package = pickle.loads(s3.Bucket(s3bucket).Object(prefix).get()['Body'].read())
        return best_model_package

    except Exception as message:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logger.error(
            "filename = %s - function = pickle_file_on_s3 in line_number = %s - hint = %s " % (
                f_name, exc_tb.tb_lineno, message.__str__()))

        response = {'statusCode': 200,
                    'status': 'Exception',
                    'exception-message': message.__str__(),
                    'from': 'bs_lambda_create_and_save_model'}
        return response


def get_inference_data_path_on_s3(n_days_ago=1):
    try:
        date_N_days_ago = datetime.now() - timedelta(days=n_days_ago)
        timestamp_suffix = date_N_days_ago.strftime('%Y/%m/%d/')
        s3_path = "s3://{}/{}".format(bucket, inference_prefix + timestamp_suffix)
        return s3_path
    except Exception as message:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logger.error(
            "filename = %s - function = get_today_inference_data_path_on_s3 in line_number = %s - hint = %s " % (
                f_name, exc_tb.tb_lineno, message.__str__()))
        return None


def get_input_data_path_on_s3(n_days_ago=1):
    try:
        date_N_days_ago = datetime.now() - timedelta(days=n_days_ago)
        timestamp_suffix = date_N_days_ago.strftime('%Y/%m/%d/')
        s3_path = "s3://{}/{}".format(bucket,
                                      input_prefix + timestamp_suffix + 'buyer_stage_input.csv')
        return s3_path
    except Exception as message:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logger.error(
            "filename = %s - function = get_today_inference_data_path_on_s3 in line_number = %s - hint = %s " % (
                f_name, exc_tb.tb_lineno, message.__str__()))
        return None

