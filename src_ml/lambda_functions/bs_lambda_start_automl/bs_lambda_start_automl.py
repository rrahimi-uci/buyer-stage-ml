import boto3
import os
import logging
import sys
from time import gmtime, strftime

logger = logging.getLogger('buyer_stage')
logging.getLogger().addHandler(logging.StreamHandler(sys.stderr))
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger.setLevel(logging.INFO)

env = os.environ['ENV']
automl_role = os.environ['AUTOML_ROLE']
region = os.environ['REGION']
bucket = os.environ['BUCKET']
train_prefix = os.environ['TRAIN_PREFIX']
automl_prefix = os.environ['AUTOML_PREFIX']
automl_max_candidate = os.environ['AUTOML_MAX_CANDIDATE']
automl_max_runtime = os.environ['AUTOML_MAX_RUNTIME']


sagmaker_handler = boto3.Session().client(service_name='sagemaker', region_name=region)


def lambda_handler(event, context):
    """
        Description: Start automl Job
        @rtype : dict
        @return : response
    """
    try:
        logger.info("Start Automl Job")
        logger.info("The content of event is : ---> " + str(event))
        target_value = 'buyer_stage'

        input_data_config = [{
            'DataSource': {
                'S3DataSource': {
                    'S3DataType': 'S3Prefix',
                    'S3Uri': 's3://{}/{}'.format(bucket, train_prefix)
                }
            },
            'TargetAttributeName': '{}'.format(target_value)
        }
        ]

        output_data_config = {
            'S3OutputPath': 's3://{}/{}'.format(bucket, automl_prefix)
        }

        problem_type = '{}'.format('MulticlassClassification')

        # We use F1macro as our optimization criteria
        auto_ml_job_objective = {'MetricName': '{}'.format('F1macro')}

        auto_ml_job_config = {
            'CompletionCriteria': {
                'MaxCandidates': int(automl_max_candidate),
                'MaxAutoMLJobRuntimeInSeconds': int(automl_max_runtime)
            }}

        # Launch AutoML Job
        timestamp_suffix = strftime('%y-%m-%d-%H-%M-%S', gmtime())
        automl_job_name = 'buyer-stage-auto-ml-' + timestamp_suffix
        logger.info('Start AutoML for Job Name: ' + automl_job_name)

        sagmaker_handler.create_auto_ml_job(AutoMLJobName=automl_job_name,
                                            InputDataConfig=input_data_config,
                                            OutputDataConfig=output_data_config,
                                            AutoMLJobObjective=auto_ml_job_objective,
                                            AutoMLJobConfig=auto_ml_job_config,
                                            ProblemType=problem_type,
                                            RoleArn=automl_role,
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
                    'automl_job_name': automl_job_name,
                    'from': 'bs_lambda_start_automl'}
        return response

    except Exception as message:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logger.error(
            "filename = %s - function = bs_lambda_start_automl in line_number = %s - hint = %s " % (
                f_name, exc_tb.tb_lineno, message.__str__()))

        response = {'statusCode': 200,
                    'status': 'Exception',
                    'exception-message': message.__str__(),
                    'from': 'bs_lambda_start_automl'}
        return response
