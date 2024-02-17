import boto3
import os
import logging
import sys

logger = logging.getLogger('buyer_stage')
logging.getLogger().addHandler(logging.StreamHandler(sys.stderr))
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger.setLevel(logging.INFO)

env = os.environ['ENV']

sagmaker_handler = boto3.client('sagemaker')


def lambda_handler(event, context):
    """
        Description: Check the status of SageMaker AutoML
        @type: dict
        @param: event['Input']['automl_job_name'], event['Input']['status']
        @rtype : dict
        @return : response
    """
    try:
        logger.info("Check AutoMl Status")
        logger.info("The content of event is ---> " + str(event))

        # Get AutoMl Job Status
        automl_job_name = event['Input']['automl_job_name']

        # Track SageMaker autopilot process
        describe_response = sagmaker_handler.describe_auto_ml_job(AutoMLJobName=automl_job_name)
        logger.info(describe_response['AutoMLJobStatus'] + " - " + describe_response['AutoMLJobSecondaryStatus'])

        status = describe_response['AutoMLJobStatus']
        logger.info("AutoML job status is : " + str(status))

        response = {'statusCode': 200,
                    'status': status,
                    'automl_job_name': automl_job_name,
                    'from': 'bs_lambda_check_automl_status'}
        return response

    except Exception as message:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logger.error(
            "filename = %s - function = bs_lambda_check_automl_status in line_number = %s - hint = %s " % (
                f_name, exc_tb.tb_lineno, message.__str__()))

        response = {'statusCode': 200,
                    'status': 'Exception',
                    'exception-message': message.__str__(),
                    'from': 'bs_lambda_check_automl_status function'}
        return response
