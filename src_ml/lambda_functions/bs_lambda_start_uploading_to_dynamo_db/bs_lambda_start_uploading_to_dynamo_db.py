import logging
import os
import sys
from datetime import datetime, timedelta
from time import gmtime, strftime

import boto3

logger = logging.getLogger('buyer_stage')
logging.getLogger().addHandler(logging.StreamHandler(sys.stderr))
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger.setLevel(logging.INFO)

env = os.environ['ENV']
glue_role_arn = os.environ['GLUE_ROLE_ARN']
max_retries = os.environ['MAX_RETRIES']
number_of_days_lag = os.environ['NUMBER_OF_DAYS_LAG']
bucket = os.environ['BUCKET']
source_codes_prefix = os.environ['SOURCE_CODES_PREFIX']
glue_worker_type = os.environ['GLUE_WORKER_TYPE']
glue_number_of_workers = os.environ['GLUE_NUMBER_OF_WORKERS']
glue_timeout = os.environ['GLUE_TIMEOUT']
region = os.environ['REGION']
number_of_partitions = os.environ['NUMBER_OF_PARTITIONS']
dynamo_db_table = os.environ['DYNAMO_DB_TABLE']
dynamo_ttl_day = os.environ['DYNAMO_TTL_DAY']
inference_prefix = os.environ['INFERENCE_PREFIX']


glue_client = boto3.client('glue')


def lambda_handler(event, context):
    """
        Description: start batch buyer state batch transformation
        @rtype : dict
        @return : response
    """
    try:
        logger.info("Start Uploading to Dynamo Db job")
        logger.info("The content of event is : --->" + str(event))

        timestamp_suffix = strftime('%y-%m-%d-%H-%M-%S', gmtime())
        glue_job_name = 'buyer-stage-ml-pipeline-pushddb-' + timestamp_suffix
        # delete job if exists before and refresh the code
        # glue_client.delete_job(JobName=glue_job_name)
        retry = max_retries

        logger.info('Push dynamo db Glue Job: ' + glue_job_name)
        input_data_on_s3_to_pushddb_by_glue = get_inference_data_path_on_s3(
            int(number_of_days_lag)) + 'buyer_stage_input.csv.out'
        logger.info("The input data for push to dynamodb is " + input_data_on_s3_to_pushddb_by_glue)

        buyer_stage_job = glue_client.create_job(
            Name=glue_job_name,
            Description='To push buyer stage ML pipeline results to Dynamo db',
            Role=glue_role_arn,
            ExecutionProperty={
                'MaxConcurrentRuns': 10
            },
            Command={
                'Name': 'glueetl',
                'ScriptLocation': 's3://{}/{}'.format(bucket,
                                                      source_codes_prefix + 'bs_push2ddb_glue.py'),
                'PythonVersion': '3'
            },
            DefaultArguments={
                '--job-language': 'python'
            },
            GlueVersion='1.0',
            MaxRetries=int(retry),
            WorkerType=glue_worker_type,
            NumberOfWorkers=int(glue_number_of_workers),
            Timeout=int(glue_timeout),
            Tags={
                'owner': 'dltechnologyconsumerprofile@move.com',
                'product': 'consumer_profile',
                'component': 'buyer-stage-ml',
                'environment': env,
                'classification': 'internal'
            }
        )

        glue_response = glue_client.start_job_run(
            JobName=buyer_stage_job['Name'],
            Arguments={
                '--ENV': env,
                '--JOB_NAME': glue_job_name,
                '--REGION': region,
                '--S3_SOURCE': input_data_on_s3_to_pushddb_by_glue,
                '--NUMBER_OF_PARTITIONS': str(number_of_partitions),
                '--DYNAMO_DB_TABLE': dynamo_db_table,
                '--NUMBER_OF_DAYS_TTL': str(dynamo_ttl_day)
            })

        status = glue_client.get_job_run(JobName=buyer_stage_job['Name'],
                                         RunId=glue_response['JobRunId'])

        response = {'statusCode': 200,
                    'status': status['JobRun']['JobRunState'],
                    'glue_job_name': buyer_stage_job['Name'],
                    'glue_job_run_id': glue_response['JobRunId'],
                    'retry': retry,
                    'from': 'bs_lambda_start_uploading_to_dynamo_db'}

        return response

    except Exception as message:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logger.error(
            "filename = %s - function = bs_lambda_start_uploading_to_dynamo_db in line_number = %s - hint = %s " % (
                f_name, exc_tb.tb_lineno, message.__str__()))

        response = {'statusCode': 200,
                    'status': 'Exception',
                    'exception-message': message.__str__(),
                    'from': 'bs_lambda_start_uploading_to_dynamo_db'}
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
