import logging
import os
import sys
from datetime import datetime, timedelta

import boto3

logger = logging.getLogger('buyer_stage')
logging.getLogger().addHandler(logging.StreamHandler(sys.stderr))
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger.setLevel(logging.INFO)

env = os.environ['ENV']
bucket = os.environ['BUCKET']
number_of_days_lag = os.environ['NUMBER_OF_DAYS_LAG']
input_prefix = os.environ['INPUT_PREFIX']


s3 = boto3.client('s3')


def lambda_handler(event, context):
    """
        Description: Check if model available
        @rtype : dict
        @return : response
    """
    try:
        logger.info("Check to see if new data is available for buyer-stage inference pipeline.")

        # check data N days ago
        data_prefix = get_input_prefix_on_s3(int(number_of_days_lag))
        logger.info("The prefix is : " + data_prefix)

        data_availability_status = check_object_exists_on_s3(bucket,
                                                             data_prefix + 'buyer_stage_input.csv')

        if data_availability_status is True:
            data_path_on_s3 = "s3://{}/{}".format(bucket,
                                                  data_prefix + 'buyer_stage_input.csv')
            logger.info("The data path on s3 is : " + data_path_on_s3)
            response = {'statusCode': 200,
                        'data_path_on_s3': data_path_on_s3,
                        'status': 'Available',
                        'from': 'bs_lambda_check_if_daily_data_available'}
        else:
            response = {'statusCode': 200,
                        'data_path_on_s3': '',
                        'status': 'Not-Available',
                        'from': 'bs_lambda_check_if_daily_data_available'}
        return response

    except Exception as message:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logger.error(
            "filename = %s - function = bs_lambda_check_if_daily_data_available in line_number = %s - hint = %s " % (
                f_name, exc_tb.tb_lineno, message.__str__()))

        response = {'statusCode': 200, 'data_path_on_s3': '', 'status': 'Exception',
                    'exception-message': message.__str__(), 'from': 'bs_lambda_check_if_daily_data_available'}
        return response


def get_input_prefix_on_s3(days=1):
    try:
        date_N_days_ago = datetime.now() - timedelta(days=days)
        timestamp_suffix = date_N_days_ago.strftime('%Y/%m/%d/')
        s3_prefix = "{}{}".format(input_prefix, timestamp_suffix)
        return s3_prefix
    except Exception as message:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logger.error(
            "filename = %s - function = get_input_prefix_on_s3 in line_number = %s - hint = %s " % (
                f_name, exc_tb.tb_lineno, message.__str__()))
        return None


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
