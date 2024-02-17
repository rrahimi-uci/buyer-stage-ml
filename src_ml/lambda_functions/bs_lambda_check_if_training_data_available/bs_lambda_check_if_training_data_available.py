import boto3
import os
import logging
import sys

logger = logging.getLogger('buyer_stage')
logging.getLogger().addHandler(logging.StreamHandler(sys.stderr))
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger.setLevel(logging.INFO)

env = os.environ['ENV']
bucket = os.environ['BUCKET']
train_prefix = os.environ['TRAIN_PREFIX']


s3 = boto3.client('s3')


def lambda_handler(event, context):
    """
        Description: Check if training data available.
        @rtype : dict
        @return : response
    """
    try:
        logger.info("Check to see if training data is available for buyer-stage inference pipeline.")
        logger.info("The content of event is : --->" + str(event))
        training_data_availability_status = check_object_exists_on_s3(bucket, train_prefix)
        if training_data_availability_status is True:
            path_on_s3 = "s3://{}/{}".format(bucket, train_prefix)
            response = {'statusCode': 200,
                        'training_data_path_on_s3': path_on_s3,
                        'status': 'Available',
                        'from': 'bs_lambda_check_if_training_data_available'}
        else:
            response = {'statusCode': 200,
                        'training_data_path_on_s3': '',
                        'status': 'Not-Available',
                        'from': 'bs_lambda_check_if_training_data_available'}
        return response

    except Exception as message:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logger.error(
            "filename = %s - function = bs_lambda_check_if_training_data_available in line_number = %s - hint = %s " % (
                f_name, exc_tb.tb_lineno, message.__str__()))

        response = {'statusCode': 200, 'training_data_path_on_s3': '', 'status': 'Exception',
                    'exception-message': message.__str__(), 'from': 'bs_lambda_check_if_training_data_available'}
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
        logger.info("filename = %s - function = check_object_exists_on_s3 in line_number = %s - hint = %s "
        % (f_name, exc_tb.tb_lineno, message.__str__()))
        return False
    return True
