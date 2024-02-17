import logging
import os
import pickle
import sys
from time import gmtime, strftime

import boto3

logger = logging.getLogger('buyer_stage')
logging.getLogger().addHandler(logging.StreamHandler(sys.stderr))
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger.setLevel(logging.INFO)

env = os.environ['ENV']
bucket = os.environ['BUCKET']
best_model_prefix = os.environ['BEST_MODEL_PREFIX']
automl_role = os.environ['AUTOML_ROLE']

sagmaker_handler = boto3.client('sagemaker')


def lambda_handler(event, context):
    """
        Description: create and save model
        @rtype : dict
        @return : response
    """
    try:
        logger.info("Create and save best model")
        logger.info("The content of event is + " + str(event))

        # Get AutoMl Job Name
        automl_job_name = event['Input']['automl_job_name']

        # Return Best Candidate information
        best_candidate = sagmaker_handler.describe_auto_ml_job(AutoMLJobName=automl_job_name)['BestCandidate']
        best_candidate_name = best_candidate['CandidateName']
        best_candidate_performance = best_candidate['FinalAutoMLJobObjectiveMetric']['Value']

        logger.info("Best Candidate Name for AutoML job {} is {}".format(automl_job_name, best_candidate_name))
        logger.info("Best Candidate Performance is : " + str(best_candidate_performance))

        optimal_model_name = "buyer-stage-optimal-model-" + strftime('%d-%H-%M-%S', gmtime())

        model = sagmaker_handler.create_model(ModelName=optimal_model_name,
                                              Containers=best_candidate['InferenceContainers'],
                                              ExecutionRoleArn=automl_role,
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

        best_model_package = {'best_candidate': best_candidate, 'optimal_model_name': optimal_model_name}
        pickle_file_on_s3(best_model_package, bucket, best_model_prefix)

        status = 'Completed'
        response = {'statusCode': 200,
                    'status': str(status),
                    'model_name': optimal_model_name,
                    'from': 'bs_lambda_create_and_save_model'}

        return response

    except Exception as message:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logger.error(
            "filename = %s - function = bs_lambda_create_and_save_model in line_number = %s - hint = %s " % (
                f_name, exc_tb.tb_lineno, message.__str__()))

        response = {'statusCode': 200,
                    'status': 'Exception',
                    'exception-message': message.__str__(),
                    'from': 'bs_lambda_create_and_save_model'}
        return response


def pickle_file_on_s3(best_model_package, bucket, best_model_prefix):
    try:
        pickle_byte_obj = pickle.dumps(best_model_package)
        s3_resource = boto3.resource('s3')
        s3_resource.Object(bucket, best_model_prefix).put(Body=pickle_byte_obj)

        return True

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
