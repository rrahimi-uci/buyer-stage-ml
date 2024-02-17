from logger import logger
import boto3
import json
from time import gmtime, strftime, sleep
from s3_helper import *
import sys
import os

class BuyerStageMl(object):
    env = 'dev-sagemaker-notebook'
    region = None
    bucket = None
    role = None
    sagmaker_handler = None
    param_dict = None
    auto_ml_job_name = None
    auto_ml_best_model = None
    auto_ml_best_model_name = None
    batch_transfer_job_name = None

    def __init__(self, env):
        self.env = env
        # Loading config file for config parameters
        with open('config.json', 'r') as fp:
            self.param_dict = json.load(fp)

        self.region = self.param_dict[self.env]['region']
        self.bucket = self.param_dict[self.env]['bucket']
        self.role = self.param_dict[self.env]['sagemaker_role_arn']
        self.sagmaker_handler = boto3.Session().client(service_name='sagemaker', region_name=self.region)

    def get_today_input_data_path_on_s3(self):
        try:
            timestamp_suffix = strftime('%Y/%m/%d/', gmtime())
            s3_path = "s3://{}/{}".format(self.param_dict[self.env]['bucket'],
                                          self.param_dict[self.env]['input_prefix'] + timestamp_suffix)
            return s3_path
        except Exception as message:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.error(
                "filename = %s - function = get_today_input_data_path_on_s3 in line_number = %s - hint = %s " % (
                    f_name, exc_tb.tb_lineno, message.__str__()))
            return None

    def get_today_inference_data_path_on_s3(self):
        try:
            timestamp_suffix = strftime('%Y/%m/%d/', gmtime())
            s3_path = "s3://{}/{}".format(self.param_dict[self.env]['bucket'],
                                          self.param_dict[self.env]['inference_prefix'] + timestamp_suffix)
            return s3_path
        except Exception as message:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.error(
                "filename = %s - function = get_today_inference_data_path_on_s3 in line_number = %s - hint = %s " % (
                    f_name, exc_tb.tb_lineno, message.__str__()))
            return None

    def start_auto_ml(self, target_value, opt_metric, problem_type, max_candidate, max_runtime):
        try:
            logger.info("Starting Auto Ml Job")

            input_data_config = [{
                'DataSource': {
                    'S3DataSource': {
                        'S3DataType': 'S3Prefix',
                        'S3Uri': 's3://{}/{}'.format(self.bucket, self.param_dict[self.env]['train_prefix'])
                    }
                },
                'TargetAttributeName': '{}'.format(target_value)
            }
            ]

            output_data_config = {
                'S3OutputPath': 's3://{}/{}'.format(self.bucket, self.param_dict[self.env]['automl_prefix'])
            }

            problem_type = '{}'.format(problem_type)

            # We use F1macro as our optimization criteria
            auto_ml_job_objective = {'MetricName': '{}'.format(opt_metric)}

            auto_ml_job_config = {
                'CompletionCriteria': {
                    'MaxCandidates': max_candidate,
                    'MaxAutoMLJobRuntimeInSeconds': max_runtime
                }}

            # Launch AutoML Job
            timestamp_suffix = strftime('%d-%H-%M-%S', gmtime())
            auto_ml_job_name = 'bs-auto-ml-' + timestamp_suffix
            logger.info('Start AutoML for Job Name: ' + auto_ml_job_name)

            self.sagmaker_handler.create_auto_ml_job(AutoMLJobName=auto_ml_job_name,
                                       InputDataConfig=input_data_config,
                                       OutputDataConfig=output_data_config,
                                       AutoMLJobObjective=auto_ml_job_objective,
                                       AutoMLJobConfig=auto_ml_job_config,
                                       ProblemType=problem_type,
                                       RoleArn=self.role)
            
            self.auto_ml_job_name = auto_ml_job_name
            return auto_ml_job_name
        
        except Exception as message:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.error("filename = %s - function = start_auto_ml in line_number = %s - hint = %s " % (
                f_name, exc_tb.tb_lineno, message.__str__()))
            return None
        
    def get_auto_ml_status(self, auto_ml_job_name):
        try:
            # Track SageMaker autopilot process
            logger.info("Getting Auto Ml Job Status")
            describe_response = self.sagmaker_handler.describe_auto_ml_job(AutoMLJobName=auto_ml_job_name)
            logger.info(describe_response['AutoMLJobStatus'] + " - " + describe_response['AutoMLJobSecondaryStatus'])
            job_run_status = describe_response['AutoMLJobStatus']

            while job_run_status not in ('Failed', 'Completed', 'Stopped'):
                describe_response = self.sagmaker_handler.describe_auto_ml_job(AutoMLJobName=auto_ml_job_name)
                job_run_status = describe_response['AutoMLJobStatus']
                logger.info(
                    describe_response['AutoMLJobStatus'] + " - " + describe_response['AutoMLJobSecondaryStatus'])
                sleep(60)
                
            return self.sagmaker_handler.describe_auto_ml_job(AutoMLJobName=auto_ml_job_name)['AutoMLJobStatus']
        
        except Exception as message:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.error("filename = %s - function = get_auto_ml_status in line_number = %s - hint = %s " % (
                f_name, exc_tb.tb_lineno, message.__str__()))
            return None
        
    def create_model(self, model_name, model_container):
        try:
            model = self.sagmaker_handler.create_model(ModelName=model_name, Containers=model_container, ExecutionRoleArn=self.role)
            return model
        except Exception as message:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.error("filename = %s - function = create_model in line_number = %s - hint = %s " % (
                f_name, exc_tb.tb_lineno, message.__str__()))
            return None
        
    def save_best_candidate(self, auto_ml_job_name ):
        try:
            logger.info("Save Best Candidate from AutoMl Job")
            # Return Best Candidate information
            best_candidate = self.sagmaker_handler.describe_auto_ml_job(AutoMLJobName=auto_ml_job_name)['BestCandidate']
            best_candidate_name = best_candidate['CandidateName']
            best_candidate_performance = best_candidate['FinalAutoMLJobObjectiveMetric']['Value']

            logger.info("Best Candidate Name for job {} is {}".format(auto_ml_job_name, best_candidate_name))
            logger.info("Best Candidate Performance is : " + str(best_candidate_performance))

            self.auto_ml_best_model = best_candidate
            optimal_model_name = "bs-optimal-model-" + strftime('%d-%H-%M-%S', gmtime())  
            self.auto_ml_best_model_name = optimal_model_name
            
            self.create_model(optimal_model_name, best_candidate['InferenceContainers'])
             
            best_model_path_s3 = "s3://{}/{}".format(self.bucket, self.param_dict[self.env]['best_model_prefix'])
            best_model_package = {'best_candidate': best_candidate, 'optimal_model_name':optimal_model_name}
            
            S3Operations.pickle_file_s3(best_model_package, best_model_path_s3)
            return optimal_model_name  
        
        except Exception as message:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.error("filename = %s - function = save_best_candidate in line_number = %s - hint = %s " % (
                f_name, exc_tb.tb_lineno, message.__str__()))
            return None

    def start_batch_transformation(self, 
                             model_name,
                             input_data_on_s3,
                             inference_data_on_s3,
                             instance_type='ml.m4.xlarge',
                             instance_count=1):
        try:
            logger.info("Start of batch transformation Job")
            timestamp_suffix = strftime('%d-%H-%M-%S', gmtime())
            transform_job_name = 'bs-batch-trans-' + timestamp_suffix

            transform_input = {
                'DataSource': {
                    'S3DataSource': {
                        'S3DataType': 'S3Prefix',
                        'S3Uri': input_data_on_s3
                    }
                },
                'ContentType': 'text/csv',
                'CompressionType': 'Gzip',
                'SplitType': 'Line'
            }

            transform_output = {
                'Accept': 'text/csv',
                'AssembleWith': 'Line',
                'S3OutputPath': inference_data_on_s3,
            }

            transform_resources = {
                'InstanceType': '{}'.format(instance_type),
                'InstanceCount': instance_count
            }

            data_processing = {
                'InputFilter': '$[1:]',
                'JoinSource': 'Input',
                'OutputFilter': "$[0,-1]"
            }

            self.sagmaker_handler.create_transform_job(TransformJobName=transform_job_name,
                                         ModelName=model_name,
                                         MaxConcurrentTransforms=self.param_dict[self.env]['max_concurrent_transforms'],
                                         MaxPayloadInMB=self.param_dict[self.env]['max_payload_in_mb'],
                                         BatchStrategy=self.param_dict[self.env]['batch_strategy'],
                                         TransformInput=transform_input,
                                         TransformOutput=transform_output,
                                         TransformResources=transform_resources,
                                         DataProcessing=data_processing)           
            
            self.batch_transfer_job_name = transform_job_name
            return transform_job_name

        except Exception as message:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.error("filename = %s - function = start_batch_transformation in line_number = %s - hint = %s " % (
                f_name, exc_tb.tb_lineno, message.__str__()))
            return None
        
    def get_batch_transformation_status(self, transform_job_name):
        try:
            logger.info("Start of batch transformation status check!")
            describe_response = self.sagmaker_handler.describe_transform_job(TransformJobName=transform_job_name)
            job_run_status = describe_response['TransformJobStatus']
            logger.info(job_run_status)

            while job_run_status not in ('Completed', 'Stopped'):
                describe_response = self.sagmaker_handler.describe_transform_job(TransformJobName=transform_job_name)
                job_run_status = describe_response['TransformJobStatus']
                logger.info(job_run_status)
                sleep(60)

            return job_run_status
        except Exception as message:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.error("filename = %s - function = get_batch_transformation_status in line_number = %s - hint = %s " % (
                f_name, exc_tb.tb_lineno, message.__str__()))
            return None
            
    def run_auto_ml_best_model_on_test_data(self, instance_type='ml.m4.xlarge', instance_count=1):
        try:
            input_path_on_s3 = "s3://{}/{}".format(self.param_dict[self.env]['bucket'],
                                                   self.param_dict[self.env]['test_prefix_no_target'])
            # Clean up prevoius results
            S3Operations.s3_clean_up(self.param_dict[self.env]['bucket'],
                                    self.param_dict[self.env]['inference_prefix_auto_ml_test'])

            output_path_on_s3 = "s3://{}/{}".format(self.param_dict[self.env]['bucket'],
                                                    self.param_dict[self.env]['inference_prefix_auto_ml_test'])
            transform_job_name = self.start_batch_transformation(
                model_name=self.auto_ml_best_model_name,
                input_data_on_s3=input_path_on_s3,
                inference_data_on_s3=output_path_on_s3,
                instance_type=instance_type,
                instance_count=instance_count)
            
            self.get_batch_transformation_status(transform_job_name)
            return
        
        except Exception as message:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.error(
                "filename = %s - function = run_auto_ml_best_model_on_test_data in line_number = %s - hint = %s " % (
                    f_name, exc_tb.tb_lineno, message.__str__()))
            return None
