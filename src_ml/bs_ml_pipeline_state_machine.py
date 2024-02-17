import json
import logging
import os
import sys

import stepfunctions
from stepfunctions.steps import *
from stepfunctions.workflow import Workflow

logger = logging.getLogger('buyer_stage')
logging.getLogger().addHandler(logging.StreamHandler(sys.stderr))
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger.setLevel(logging.INFO)

stepfunctions.set_stream_logger(level=logging.INFO)

with open('config.json', 'r') as fp:
    param_dict = json.load(fp)


def creat_ml_pipeline_state_machine():
    try:
        SendSnSNotification = 'bs_lambda_send_sns_notification'
        CheckDailyDataAvaiability = 'bs_lambda_check_if_daily_data_available'
        CheckModelAvailability = 'bs_lambda_check_if_model_available'
        CheckTrainingDataAvailability = 'bs_lambda_check_if_training_data_available'
        StartAutoMl = 'bs_lambda_start_automl'
        CheckAutoMlStatus = 'bs_lambda_check_automl_status'
        CreateAndSaveModel = 'bs_lambda_create_and_save_model'
        StartBatchTransformation = 'bs_lambda_start_batch_stransformation'
        CheckBatchTransformationStatus = 'bs_lambda_check_batch_transformation_status'

        StartUploadingToDynamoDB = 'bs_lambda_start_uploading_to_dynamo_db'
        CheckUploadingToDynamoDBStatus = 'bs_lambda_check_uploading_to_dynamo_db_status'

        success_step = Succeed('Done')

        # Defining Lambda Functions
        # SendSnSNotification = 'bs_lambda_send_sns_notification'
        send_sns_notification_lambda_step = LambdaStep('SendSnSNotification',
                                                       parameters={"FunctionName": SendSnSNotification,
                                                                   'Payload': {"Input.$": "$.Payload"}})

        send_sns_notification_lambda_step.add_retry(Retry(
            error_equals=["States.TaskFailed"],
            interval_seconds=30,
            max_attempts=3,
            backoff_rate=4.0
        ))

        send_sns_notification_lambda_step.add_catch(Catch(
            error_equals=["States.TaskFailed"],
            next_step=Fail("SNSFailed")
        ))

        # CheckDailyDataAvaiability =  'bs_lambda_check_if_daily_data_available'
        check_if_daily_data_available_lambda_step = LambdaStep('CheckDailyDataAvaiability',
                                                               parameters=
                                                               {"FunctionName": CheckDailyDataAvaiability})

        check_if_daily_data_available_lambda_step.add_retry(Retry(
            error_equals=["States.TaskFailed"],
            interval_seconds=30,
            max_attempts=3,
            backoff_rate=4.0
        ))

        # CheckModelAvailability = 'bs_lambda_check_if_model_available'
        check_if_model_available_lambda_step = LambdaStep('CheckModelAvailability',
                                                          parameters=
                                                          {"FunctionName": CheckModelAvailability,
                                                           'Payload': {"Input.$": "$.Payload"}})

        check_if_model_available_lambda_step.add_retry(Retry(
            error_equals=["States.TaskFailed"],
            interval_seconds=30,
            max_attempts=3,
            backoff_rate=4.0
        ))

        # CheckTrainingDataAvailability = 'bs_lambda_check_if_training_data_available'
        check_if_training_data_available_lambda_step = LambdaStep('CheckTrainingDataAvailability',
                                                                  parameters=
                                                                  {"FunctionName": CheckTrainingDataAvailability,
                                                                   'Payload': {"Input.$": "$.Payload"}})

        check_if_training_data_available_lambda_step.add_retry(Retry(
            error_equals=["States.TaskFailed"],
            interval_seconds=30,
            max_attempts=3,
            backoff_rate=4.0
        ))

        # StartAutoMl = 'bs_lambda_start_automl'
        start_automl_lambda_step = LambdaStep('StartAutoMl',
                                              parameters=
                                              {"FunctionName": StartAutoMl,
                                               'Payload': {"Input.$": "$.Payload"}})

        start_automl_lambda_step.add_retry(Retry(
            error_equals=["States.TaskFailed"],
            interval_seconds=30,
            max_attempts=3,
            backoff_rate=4.0
        ))

        # CheckAutoMlStatus = 'bs_lambda_check_automl_status'
        check_automl_status_lambda_step = LambdaStep('CheckAutoMlStatus',
                                                     parameters=
                                                     {"FunctionName": CheckAutoMlStatus,
                                                      'Payload': {"Input.$": "$.Payload"}})

        check_automl_status_lambda_step.add_retry(Retry(
            error_equals=["States.TaskFailed"],
            interval_seconds=30,
            max_attempts=3,
            backoff_rate=4.0
        ))

        # CreateAndSaveModel = 'bs_lambda_create_and_save_model'
        create_and_save_model_lambda_step = LambdaStep('CreateAndSaveModel',
                                                       parameters=
                                                       {"FunctionName": CreateAndSaveModel,
                                                        'Payload': {"Input.$": "$.Payload"}})

        create_and_save_model_lambda_step.add_retry(Retry(
            error_equals=["States.TaskFailed"],
            interval_seconds=30,
            max_attempts=3,
            backoff_rate=4.0
        ))

        # StartBatchTransformation = 'bs_lambda_start_batch_stransformation'
        start_batch_transformation_lambda_step = LambdaStep('StartBatchTransformation',
                                                            parameters=
                                                            {"FunctionName": StartBatchTransformation,
                                                             'Payload': {"Input.$": "$.Payload"}})

        start_batch_transformation_lambda_step.add_retry(Retry(
            error_equals=["States.TaskFailed"],
            interval_seconds=30,
            max_attempts=3,
            backoff_rate=4.0
        ))

        # CheckBatchTransformationStatus =  'bs_lambda_check_batch_transformation_status'
        check_batch_transformation_status_lambda_step = LambdaStep('CheckBatchTransformationStatus',
                                                                   parameters=
                                                                   {"FunctionName": CheckBatchTransformationStatus,
                                                                    'Payload': {"Input.$": "$.Payload"}})

        check_batch_transformation_status_lambda_step.add_retry(Retry(
            error_equals=["States.TaskFailed"],
            interval_seconds=30,
            max_attempts=3,
            backoff_rate=4.0
        ))

        # StartUploadingToDynamoDB = 'bs_lambda_start_uploading_to_dynamo_db'
        start_uploading_to_dynamo_db_lambda_step = LambdaStep('StartUploadingToDynamoDB',
                                                              parameters=
                                                              {"FunctionName": StartUploadingToDynamoDB,
                                                               'Payload': {"Input.$": "$.Payload"}})

        start_uploading_to_dynamo_db_lambda_step.add_retry(Retry(
            error_equals=["States.TaskFailed"],
            interval_seconds=60,
            max_attempts=3,
            backoff_rate=4.0
        ))

        # CheckUploadingToDynamoDBStatus = 'bs_lambda_check_uploading_to_dynamo_db_status'
        check_uploading_to_dynamo_db_status_lambda_step = LambdaStep('CheckUploadingToDynamoDBStatus',
                                                                     parameters=
                                                                     {"FunctionName": CheckUploadingToDynamoDBStatus,
                                                                      'Payload': {"Input.$": "$.Payload"}})

        check_uploading_to_dynamo_db_status_lambda_step.add_retry(Retry(
            error_equals=["States.TaskFailed"],
            interval_seconds=60,
            max_attempts=3,
            backoff_rate=4.0
        ))

        # Define Wait States
        batch_transformation_wait_state = Wait(state_id="WaitForBatchTransformation", seconds=300)
        automl_wait_state = Wait(state_id="WaitForAutoML", seconds=300)
        uploading_to_dynamodb_wait_state = Wait(state_id="WaitForUploadingToDynamoDbGlueJob", seconds=300)

        # Define Choice States
        check_if_daily_data_available_choice_step = Choice('CheckDailyDataAvaiabilityResult')
        data_availble = ChoiceRule.StringEquals(
            check_if_daily_data_available_lambda_step.output()['Payload']['status'],
            'Available')
        data_not_available = ChoiceRule.StringEquals(
            check_if_daily_data_available_lambda_step.output()['Payload']['status'],
            'Not-Available')
        data_availble_exception = ChoiceRule.StringEquals(
            check_if_daily_data_available_lambda_step.output()['Payload']['status'],
            'Exception')
        check_if_daily_data_available_choice_step.add_choice(rule=data_availble,
                                                             next_step=check_if_model_available_lambda_step)
        check_if_daily_data_available_choice_step.add_choice(rule=data_not_available,
                                                             next_step=send_sns_notification_lambda_step)
        check_if_daily_data_available_choice_step.add_choice(rule=data_availble_exception,
                                                             next_step=send_sns_notification_lambda_step)

        check_if_model_available_choice_step = Choice('CheckModelAvailabilityResult')
        model_available = ChoiceRule.StringEquals(check_if_model_available_lambda_step.output()['Payload']['status'],
                                                  'Available')
        model_not_available = ChoiceRule.StringEquals(
            check_if_model_available_lambda_step.output()['Payload']['status'],
            'Not-Available')
        model_available_exception = ChoiceRule.StringEquals(
            check_if_model_available_lambda_step.output()['Payload']['status'],
            'Exception')
        check_if_model_available_choice_step.add_choice(rule=model_available,
                                                        next_step=start_batch_transformation_lambda_step)
        check_if_model_available_choice_step.add_choice(rule=model_not_available,
                                                        next_step=check_if_training_data_available_lambda_step)
        check_if_model_available_choice_step.add_choice(rule=model_available_exception,
                                                        next_step=send_sns_notification_lambda_step)

        check_if_training_data_available_choice_step = Choice('CheckTrainingDataAvailabilityResult')
        training_data_available = ChoiceRule.StringEquals(
            check_if_training_data_available_lambda_step.output()['Payload']['status'], 'Available')
        training_data_not_available = ChoiceRule.StringEquals(
            check_if_training_data_available_lambda_step.output()['Payload']['status'], 'Not-Available')
        training_data_available_exception = ChoiceRule.StringEquals(
            check_if_training_data_available_lambda_step.output()['Payload']['status'], 'Exception')
        check_if_training_data_available_choice_step.add_choice(rule=training_data_available,
                                                                next_step=start_automl_lambda_step)
        check_if_training_data_available_choice_step.add_choice(rule=training_data_not_available,
                                                                next_step=send_sns_notification_lambda_step)
        check_if_training_data_available_choice_step.add_choice(rule=training_data_available_exception,
                                                                next_step=send_sns_notification_lambda_step)

        check_batch_transformation_status_choice_step = Choice('CheckBatchTransformationStatusResult')
        batch_completed = ChoiceRule.StringEquals(
            check_batch_transformation_status_lambda_step.output()['Payload']['status'],
            'Completed')
        batch_stopped = ChoiceRule.StringEquals(
            check_batch_transformation_status_lambda_step.output()['Payload']['status'],
            'Stopped')
        batch_inprogress = ChoiceRule.StringEquals(
            check_batch_transformation_status_lambda_step.output()['Payload']['status'],
            'InProgress')
        batch_failed = ChoiceRule.StringEquals(
            check_batch_transformation_status_lambda_step.output()['Payload']['status'],
            'Failed')
        batch_exception = ChoiceRule.StringEquals(
            check_batch_transformation_status_lambda_step.output()['Payload']['status'],
            'Exception')
        check_batch_transformation_status_choice_step.add_choice(rule=batch_completed,
                                                                 next_step=start_uploading_to_dynamo_db_lambda_step)
        check_batch_transformation_status_choice_step.add_choice(rule=batch_stopped,
                                                                 next_step=send_sns_notification_lambda_step)
        check_batch_transformation_status_choice_step.add_choice(rule=batch_inprogress,
                                                                 next_step=batch_transformation_wait_state)
        check_batch_transformation_status_choice_step.add_choice(rule=batch_failed,
                                                                 next_step=send_sns_notification_lambda_step)
        check_batch_transformation_status_choice_step.add_choice(rule=batch_exception,
                                                                 next_step=send_sns_notification_lambda_step)

        check_automl_status_choice_step = Choice('CheckAutoMlStatusResult')
        automl_completed = ChoiceRule.StringEquals(check_automl_status_lambda_step.output()['Payload']['status'],
                                                   'Completed')
        automl_stopped = ChoiceRule.StringEquals(check_automl_status_lambda_step.output()['Payload']['status'],
                                                 'Stopped')
        automl_inprogress = ChoiceRule.StringEquals(check_automl_status_lambda_step.output()['Payload']['status'],
                                                    'InProgress')
        automl_failed = ChoiceRule.StringEquals(check_automl_status_lambda_step.output()['Payload']['status'],
                                                'Failed')
        automl_exception = ChoiceRule.StringEquals(check_automl_status_lambda_step.output()['Payload']['status'],
                                                   'Exception')
        check_automl_status_choice_step.add_choice(rule=automl_completed,
                                                   next_step=create_and_save_model_lambda_step)
        check_automl_status_choice_step.add_choice(rule=automl_stopped,
                                                   next_step=send_sns_notification_lambda_step)
        check_automl_status_choice_step.add_choice(rule=automl_inprogress,
                                                   next_step=automl_wait_state)
        check_automl_status_choice_step.add_choice(rule=automl_failed,
                                                   next_step=send_sns_notification_lambda_step)
        check_automl_status_choice_step.add_choice(rule=automl_exception,
                                                   next_step=send_sns_notification_lambda_step)

        check_uploading_to_dynamodb_status_choice_step = Choice('CheckUploadingToDynamoDbResult')
        glue_starting = ChoiceRule.StringEquals(
            check_uploading_to_dynamodb_status_choice_step.output()['Payload']['status'],
            'STARTING')
        glue_running = ChoiceRule.StringEquals(
            check_uploading_to_dynamodb_status_choice_step.output()['Payload']['status'],
            'RUNNING')
        glue_stopping = ChoiceRule.StringEquals(
            check_uploading_to_dynamodb_status_choice_step.output()['Payload']['status'],
            'STOPPING')
        glue_stopped = ChoiceRule.StringEquals(
            check_uploading_to_dynamodb_status_choice_step.output()['Payload']['status'],
            'STOPPED')
        glue_succeeded = ChoiceRule.StringEquals(
            check_uploading_to_dynamodb_status_choice_step.output()['Payload']['status'],
            'SUCCEEDED')
        glue_failed = ChoiceRule.StringEquals(
            check_uploading_to_dynamodb_status_choice_step.output()['Payload']['status'],
            'FAILED')
        glue_timeout = ChoiceRule.StringEquals(
            check_uploading_to_dynamodb_status_choice_step.output()['Payload']['status'],
            'TIMEOUT')
        glue_exception = ChoiceRule.StringEquals(
            check_uploading_to_dynamodb_status_choice_step.output()['Payload']['status'],
            'Exception')
        check_uploading_to_dynamodb_status_choice_step.add_choice(rule=glue_starting,
                                                                  next_step=uploading_to_dynamodb_wait_state)
        check_uploading_to_dynamodb_status_choice_step.add_choice(rule=glue_running,
                                                                  next_step=uploading_to_dynamodb_wait_state)
        check_uploading_to_dynamodb_status_choice_step.add_choice(rule=glue_stopping,
                                                                  next_step=uploading_to_dynamodb_wait_state)
        check_uploading_to_dynamodb_status_choice_step.add_choice(rule=glue_stopped,
                                                                  next_step=send_sns_notification_lambda_step)
        check_uploading_to_dynamodb_status_choice_step.add_choice(rule=glue_succeeded, next_step=success_step)
        check_uploading_to_dynamodb_status_choice_step.add_choice(rule=glue_failed,
                                                                  next_step=send_sns_notification_lambda_step)
        check_uploading_to_dynamodb_status_choice_step.add_choice(rule=glue_timeout,
                                                                  next_step=send_sns_notification_lambda_step)
        check_uploading_to_dynamodb_status_choice_step.add_choice(rule=glue_exception,
                                                                  next_step=send_sns_notification_lambda_step)

        # chaining the steps
        buyer_stage_ml_pipeline = Chain(
            [check_if_daily_data_available_lambda_step, check_if_daily_data_available_choice_step])
        Chain([check_if_model_available_lambda_step, check_if_model_available_choice_step])
        Chain([check_if_training_data_available_lambda_step, check_if_training_data_available_choice_step])
        Chain([check_batch_transformation_status_lambda_step, check_batch_transformation_status_choice_step])
        Chain([check_automl_status_lambda_step, check_automl_status_choice_step])
        Chain([check_uploading_to_dynamo_db_status_lambda_step, check_uploading_to_dynamodb_status_choice_step])
        Chain([start_uploading_to_dynamo_db_lambda_step, check_uploading_to_dynamo_db_status_lambda_step])
        Chain([create_and_save_model_lambda_step, start_batch_transformation_lambda_step])
        Chain([start_batch_transformation_lambda_step, check_batch_transformation_status_lambda_step])
        Chain([start_automl_lambda_step, check_automl_status_lambda_step])
        Chain([batch_transformation_wait_state, check_batch_transformation_status_lambda_step])
        Chain([automl_wait_state, check_automl_status_lambda_step])
        Chain([uploading_to_dynamodb_wait_state, check_uploading_to_dynamo_db_status_lambda_step])

        # Next, we define the workflow
        buyer_stage_ml_pipeline_workflow = Workflow(
            name="BuyerStageMLPipeline",
            definition=buyer_stage_ml_pipeline,
            role=param_dict['dev-sagemaker-notebook']['stepfunctions_role_arn']
        )
        return buyer_stage_ml_pipeline_workflow
    except Exception as message:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logger.error(
            "filename = %s - function = bs_lambda_check_automl_status in line_number = %s - hint = %s " % (
                f_name, exc_tb.tb_lineno, message.__str__()))
        return None


def save_buyer_stage_ml_pipeline_state_machine_json(file_name):
    buyer_stage_ml_state_machine = creat_ml_pipeline_state_machine()
    res = buyer_stage_ml_state_machine.definition.to_json(pretty=True)
    with open(file_name, "w+") as text_file:
        text_file.write(res)


def save_buyer_stage_ml_pipeline_state_machine_cloud_formation(file_name):
    buyer_stage_ml_state_machine = creat_ml_pipeline_state_machine()
    res = buyer_stage_ml_state_machine.get_cloudformation_template()
    with open(file_name, "w+") as text_file:
        text_file.write(res)


if __name__ == "__main__":
    buyer_stage_ml_pipeline_state_machine_json = '../deployment/bs_ml_pipeline_state_machine.json'
    buyer_stage_ml_pipeline_state_machine_cloud_formation_yml = '../deployment/bs_ml_pipeline_state_machine_cloud_formation.yaml'
    save_buyer_stage_ml_pipeline_state_machine_cloud_formation(buyer_stage_ml_pipeline_state_machine_cloud_formation_yml)
