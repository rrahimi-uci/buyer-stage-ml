import logging
import os
import sys

import boto3

logger = logging.getLogger('buyer_stage')
logging.getLogger().addHandler(logging.StreamHandler(sys.stderr))
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger.setLevel(logging.INFO)

env = os.environ['ENV']

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
        job_name = event['Input']['glue_job_name']
        run_id = event['Input']['glue_job_run_id']
        retry = int(event['Input']['retry'])

        status = glue_client.get_job_run(JobName=job_name, RunId=run_id)

        if int(retry) > 0 and status['JobRun']['JobRunState'] == 'FAILED':
            retry = retry - 1
            status_choice = 'STARTING'
        else:
            status_choice = status['JobRun']['JobRunState']

        response = {'statusCode': 200,
                    'status': status_choice,
                    'glue_job_name': job_name,
                    'glue_job_run_id': run_id,
                    'retry': retry,
                    'from': 'bs_lambda_check_uploading_to_dynamo_db_status'}

        return response

    except Exception as message:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logger.error(
            "filename = %s - function = bs_lambda_check_uploading_to_dynamo_db_status in line_number = %s - hint = %s " % (
                f_name, exc_tb.tb_lineno, message.__str__()))

        response = {'statusCode': 200,
                    'status': 'Exception',
                    'exception-message': message.__str__(),
                    'from': 'bs_lambda_check_uploading_to_dynamo_db_status'}
        return response
