import logging
import os
import sys

import boto3

logger = logging.getLogger('buyer_stage')
logging.getLogger().addHandler(logging.StreamHandler(sys.stderr))
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger.setLevel(logging.INFO)

env = os.environ['ENV']
bucket = os.environ['BUCKET']
best_model_prefix = os.environ['BEST_MODEL_PREFIX']

s3 = boto3.client('s3')


def lambda_handler(event, context):
    """
        Description: Check if the model available on s3 specific location
        @rtype : dict
        @return : response
    """
    try:
        logger.info("Check to see if model is available for buyer-stage inference pipeline.")
        logger.info("the content of event is :" + str(event))
        model_availability_status = check_object_exists_on_s3(bucket, best_model_prefix)
        if model_availability_status is True:
            model_path = "s3://{}/{}".format(bucket, best_model_prefix)
            logger.info("model path on s3 is : " + model_path)
            response = {'statusCode': 200,
                        'model_path_on_s3': model_path,
                        'status': 'Available',
                        'from': 'bs_lambda_check_if_model_available'}
        else:
            response = {'statusCode': 200,
                        'model_path_on_s3': '',
                        'status': 'Not-Available',
                        'from': 'bs_lambda_check_if_model_available'}
        return response

    except Exception as message:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logger.error(
            "filename = %s - function = bs_lambda_check_if_model_available in line_number = %s - hint = %s " % (
                f_name, exc_tb.tb_lineno, message.__str__()))

        response = {'statusCode': 200, 'model_path_on_s3': '', 'status': 'Exception',
                    'exception-message': message.__str__(), 'from': 'bs_lambda_check_if_model_available'}
        return response


def check_object_exists_on_s3(bucket, key):
    """ Checks to see if the input filename exists as object in S3
    """
    try:
        response = s3.head_object(
            Bucket=bucket,
            Key=key
        )
    except Exception as message:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logger.info(
            "filename = %s - function = check_object_exists_on_s3 in line_number = %s - hint = %s " % (
                f_name, exc_tb.tb_lineno, message.__str__()))
        return False
    return True
