import sagemaker
import matplotlib
import matplotlib.pyplot as plt
import boto3
import logging
import sys
import os
import pandas as pd
import numpy as np
import json
from sagemaker import get_execution_role
from time import gmtime, strftime, sleep
import s3fs
s3 = s3fs.S3FileSystem()
from sklearn.metrics import confusion_matrix

logger = logging.getLogger()
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
fhandler = logging.FileHandler(filename ='logs/buyer_intent.log', mode='a')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fhandler.setFormatter(formatter)
logger.addHandler(fhandler)
logger.setLevel(logging.INFO)

# Loading config file for config parameters 
with open('config.json', 'r') as fp:
     param_dict = json.load(fp)

region = boto3.Session().region_name

# This part is hard coded in this setion for dev enviroment
bucket = param_dict['dev']['bucket']
prefix = param_dict['dev']['prefix']

session = sagemaker.Session(default_bucket=bucket)

role = get_execution_role()
sm = boto3.Session().client(service_name='sagemaker',region_name=region)

# Clean up the bucket and related prefix


def s3_clean_up(bucket, prefix):
    try:
        s3 = boto3.resource('s3')
        bk = s3.Bucket(bucket)
        lst = bk.objects.filter(Prefix=prefix).delete()
        return True
    except Exception as message:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logger.error("filename = %s - function = s3_clean_up in line_number = %s - hint = %s " % (
            f_name, exc_tb.tb_lineno, message.__str__()))
        return False

# Upload the local file to s3 bucket/prefix


def upload_to_s3(session, file_name, bucket, prefix):
    try:
        data_s3_path = session.upload_data( path= file_name, bucket = bucket, key_prefix = prefix)
        logger.info('file {} uploaded to :{} '.format(file_name, data_s3_path))
        return data_s3_path 
    except Exception as message:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logger.error("filename = %s - function = upload_to_s3 in line_number = %s - hint = %s " % (
            f_name, exc_tb.tb_lineno, message.__str__()))
        return None


def load_data_from_s3(s3_file_path, dest_name):
    try:
        fs = s3fs.S3FileSystem()
        with fs.open(s3_file_path, "rb") as f1:
            with open(dest_name, "wb") as f2:
                  f2.write(f1.read())
        return True
    except Exception as message:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logger.error("filename = %s - function = load_data_from_s3 in line_number = %s - hint = %s " % (
            f_name, exc_tb.tb_lineno, message.__str__()))
        return None
    
# This function will be used to generate 3 diffrent data set for 3 diffrenet label set that we got
# from qualtrics


def make_and_clean_up_buyer_intent_data_set(data_set, q1_csv_file_name, q2_csv_file_name, q3_csv_file_name, proc = 'train' ):
    try :
        df = pd.read_csv(data_set)
        #remove member id for traing purpose
        if proc == 'train':
            df = df.drop(columns=['member_id','member_id_001','member_id_007','member_id_030','member_id_060','member_id_090'])
        else :
            df = df.drop(columns=['member_id_001','member_id_007','member_id_030','member_id_060','member_id_090'])
        
        df = df.drop(columns=['snapshot_date_mst_yyyymmdd','snapshot_date_mst_yyyymmdd_001','snapshot_date_mst_yyyymmdd_007','snapshot_date_mst_yyyymmdd_030',
                                  'snapshot_date_mst_yyyymmdd_060', 'snapshot_date_mst_yyyymmdd_090'])
        df.replace(np.nan, 0, inplace=True)
        
        df_q1 = df.drop(columns = ['stage_label_2', 'stage_label_3'])
        df_q2 = df.drop(columns = ['stage_label_1', 'stage_label_3'])
        df_q3 = df.drop(columns = ['stage_label_1', 'stage_label_2'])
        
        # Important ! remove index and have header for TRAINING File
        df_q1.to_csv(q1_csv_file_name, index=False, header=True)
        df_q2.to_csv(q2_csv_file_name, index=False, header=True)
        df_q3.to_csv(q3_csv_file_name, index=False, header=True)
        
        return True
    except Exception as message:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logger.error("filename = %s - function = make_and_clean_up_buyer_intent_data_set in line_number = %s - hint = %s " % (
            f_name, exc_tb.tb_lineno, message.__str__()))
        return None      

# This function is only used to upload TEST data set for inferencing


def load_test_data_on_s3(test_file, label):
    try:
        df = pd.read_csv(test_file)
        test_data_no_target = df.drop(columns=[label])
        head, test_file_name = os.path.split(test_file)
        
        # Remember to remove INDEX and HEADER for test data set
        test_data_no_target.to_csv(head +'/'+ 'no_trg_'+ test_file_name, index = False, header = False)
        test_data_s3_path = session.upload_data(path = head +'/'+ 'no_trg_'+ test_file_name, key_prefix=prefix + "/test")
        logger.info('Test data uploaded to: ' + test_data_s3_path)
        #os.remove('no_trg_' + test_file_name)
        return test_data_s3_path
    
    except Exception as message:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logger.error("filename = %s - function = load_test_data_on_s3 in line_number = %s - hint = %s " % (f_name, exc_tb.tb_lineno, message.__str__()))
        return None

# This function finds optimal model based on training data set


def find_optimal_model(session, bucket, prefix, train_file_name, target_value, opt_metric, problem_type, max_candidate, max_runtime, job_suffix):
    try:
        # Upload file Train and Test File to S3
        train_data_s3_path = upload_to_s3(session, train_file_name, bucket, prefix + "/train")
        logger.info('Train data uploaded to: ' + train_data_s3_path)

        #Set up Automl Job Configuration
        input_data_config = [{
          'DataSource': {
            'S3DataSource': {
              'S3DataType': 'S3Prefix',
              'S3Uri': 's3://{}/{}/train'.format(bucket,prefix)
            }
          },
          'TargetAttributeName': '{}'.format(target_value)
            }
          ]

        output_data_config = {
        'S3OutputPath': 's3://{}/{}/output'.format(bucket,prefix)
            }
        
        # Ourcase is multiclassclassification 
        ProblemType ='{}'.format(problem_type)

        # We use F1macro as our optimization criteria
        AutoMLJobObjective = {'MetricName': '{}'.format(opt_metric)}
        
        AutoMLJobConfig={
        'CompletionCriteria': {
            'MaxCandidates': max_candidate,
            'MaxAutoMLJobRuntimeInSeconds': max_runtime
        }}

        # Launch AutoML Job
        timestamp_suffix = strftime('%d-%H-%M-%S', gmtime())
        auto_ml_job_name = 'automl-bi-' + job_suffix +'-'+ timestamp_suffix
        logger.info('Start AutoML for Job Name: ' + auto_ml_job_name)

        sm.create_auto_ml_job(AutoMLJobName = auto_ml_job_name, 
                          InputDataConfig = input_data_config, 
                          OutputDataConfig = output_data_config,
                          AutoMLJobObjective = AutoMLJobObjective,
                          AutoMLJobConfig = AutoMLJobConfig,
                          ProblemType = ProblemType,
                          RoleArn=role)

        #Track SageMaker autopilot process
        describe_response = sm.describe_auto_ml_job(AutoMLJobName = auto_ml_job_name)
        logger.info(describe_response['AutoMLJobStatus'] + " - " + describe_response['AutoMLJobSecondaryStatus'])
        job_run_status = describe_response['AutoMLJobStatus']

        while job_run_status not in ('Failed', 'Completed', 'Stopped'):
            describe_response = sm.describe_auto_ml_job(AutoMLJobName = auto_ml_job_name)
            job_run_status = describe_response['AutoMLJobStatus'] 
            logger.info(describe_response['AutoMLJobStatus'] + " - " + describe_response['AutoMLJobSecondaryStatus'])
            sleep(180)

        # Return Best Candidate information  
        best_candidate = sm.describe_auto_ml_job(AutoMLJobName = auto_ml_job_name)['BestCandidate']
        best_candidate_name = best_candidate['CandidateName']
        best_candidate_performance = best_candidate['FinalAutoMLJobObjectiveMetric']['Value'] 
        
        logger.info("Best Candidate Name for job {} is {}".format(auto_ml_job_name, best_candidate_name))
        logger.info("Best Candidate Performance is : " + str(best_candidate['FinalAutoMLJobObjectiveMetric']['Value']))

        return (best_candidate, best_candidate_name, best_candidate_performance )
    except Exception as message:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logger.error("filename = %s - function = find_optimal_model in line_number = %s - hint = %s " % (f_name, exc_tb.tb_lineno, message.__str__()))
        return None
    
# RUN Model on s3 Data set in Batch mode


def inference_model(sm, role, model_name, candidate_container, transform_job_name, input_data_on_s3, output_bucket, output_prefix, instance_type ='ml.m5.xlarge', instance_count = 1):
    try:
        logger.info('candidate container is : {}'.format(str(candidate_container)))
        model = sm.create_model(Containers = candidate_container, ModelName = model_name, ExecutionRoleArn=role)
        logger.info('Model ARN corresponding to the candidate is : {}'.format(model['ModelArn']))

        transform_input = {
                'DataSource': {
                    'S3DataSource': {
                        'S3DataType': 'S3Prefix',
                        'S3Uri': '{}'.format(input_data_on_s3)
                    }
                },
                'ContentType': 'text/csv',
                'CompressionType': 'None',
                'SplitType': 'Line'
            }

        transform_output = {
                'Accept':'text/csv',
                'AssembleWith':'Line',
                'S3OutputPath': 's3://{}/{}/inference-results'.format(output_bucket,output_prefix),
            }

        transform_resources = {
                'InstanceType': '{}'.format(instance_type),
                'InstanceCount': instance_count
            }
        
        data_processing = {
            'InputFilter': '$[1:]', 
            'JoinSource': 'Input',
            'OutputFilter':"$[0,-1]"
         }

        sm.create_transform_job(TransformJobName = transform_job_name, 
                                ModelName = model_name,
                                MaxConcurrentTransforms=10,
                                MaxPayloadInMB=10,
                                BatchStrategy='MultiRecord',
                                TransformInput = transform_input, 
                                TransformOutput = transform_output, 
                                TransformResources = transform_resources,
                                DataProcessing = data_processing)


        describe_response = sm.describe_transform_job(TransformJobName = transform_job_name)
        job_run_status = describe_response['TransformJobStatus']
        logger.info(job_run_status)

        while job_run_status not in ('Completed', 'Stopped'):
            describe_response = sm.describe_transform_job(TransformJobName = transform_job_name)
            job_run_status = describe_response['TransformJobStatus']
            logger.info(job_run_status)
            sleep(180)
    
    except Exception as message:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logger.error("filename = %s - function = inference_model in line_number = %s - hint = %s " % (
            f_name, exc_tb.tb_lineno, message.__str__()))
        return None 
    

def compute_precision_recall(test_csv, predicted_csv, label):
    try:
        df_test = pd.read_csv(test_csv)
        real = df_test[label].to_list()
        
        df_predicted = pd.read_csv(predicted_csv, header = None, names=["member_id", "stage"])
        predicts = df_predicted["stage"].to_list()

        #with open(predicted_csv, 'r') as f:
        #    predicts = f.read().splitlines()

        cm = confusion_matrix(real, predicts, labels = ["Dreamer", "Casual Explorer", "Active Searcher", "Ready to Transact"])
        recall = np.diag(cm) / np.sum(cm, axis = 1)
        precision = np.diag(cm) / np.sum(cm, axis = 0)

        return(cm, precision, recall)
    
    except Exception as message:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logger.error("filename = %s - function = compute_precision_recall in line_number = %s - hint = %s " % (
            f_name, exc_tb.tb_lineno, message.__str__()))
        return None
    


def show_buyer_stage_precision_recall(pre, rec, labels):
    x = np.arange(len(labels))  # the label locations
    #x = labels  # the label locations
    width = 0.35  # the width of the bars

    fig, ax = plt.subplots()
    rects1 = ax.bar(x-width/2, pre, width, label='Precision')
    rects2 = ax.bar(x+width/2, rec, width, label='Recall')

    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel('Performance Value')
    ax.set_title('Prediction/Recall based on ONLY UP to 30 days Activity of the User (Best Label: Stage_label_1)')
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.legend()


    def autolabel(rects):
        """Attach a text label above each bar in *rects*, displaying its height."""
        for rect in rects:
            height = rect.get_height()
            ax.annotate('{}'.format(height),
                        xy=(rect.get_x() + rect.get_width() / 2, height),
                        xytext=(0, 3),  # 3 points vertical offset
                        textcoords="offset points",
                        ha='center', va='bottom')


    autolabel(rects1)
    autolabel(rects2)

    fig.tight_layout()
    fig.set_size_inches(16, 8)

    plt.show()