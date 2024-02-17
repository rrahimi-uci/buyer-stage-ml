import boto3
import os
import logging
import sys

logger = logging.getLogger('buyer_stage')
logging.getLogger().addHandler(logging.StreamHandler(sys.stderr))
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger.setLevel(logging.INFO)

env = os.environ['ENV']
sns_arn = os.environ['SNS_ARN']

sns_client = boto3.client('sns')


def lambda_handler(event, context):
    """
        Description: Send sns Notification if failure happened in pipeline.
        @rtype : dict
        @return : response
    """
    try:
        logger.info("Start doing SNS notification to BuyerStageMLPipeline topic :: ")
        logger.info("the content of event is ---> :" + str(event))
        # adding environment to the message
        message = '{}:: Got unexpected status {} from module {} in Buyer Stage ML Pipeline !'.format(
            env, event['Input']['status'], event['Input']['from'])
        response = sns_client.publish(
            TopicArn=sns_arn,
            Message=message)

        return response

    except Exception as message:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logger.error(
            "filename = %s - function = bs_lambda_start_batch_stransformation in line_number = %s - hint = %s " % (
                f_name, exc_tb.tb_lineno, message.__str__()))

        response = {'statusCode': 200, 'status': 'Exception', 'exception-message': message.__str__(),
                    'from': 'bs_lambda_send_sns_notification'}
        return response
