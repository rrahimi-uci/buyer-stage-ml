# **Buyer Stage Prediction**

The main goal of buyer stage project is to predict user stage in home buying 
experience journey. By understanding user stage we could provide more personalized 
services for them.

Following table shows the definition of the buyer **STAGE**.

| **Stage**        | **Definition**     |
| ------------- |:-------------:|
| **Dreamer**   | Does not know The timeline of buying **or** around/more than 1 year|
| **Casual Searcher**      | Within next 7-12 months      |
| **Active Searcher** |  Within next 2-6 months     |
| **Ready To Transact**        | Within next month      |

## Structure of the Codes

The code structure is as follows

* #### deployment: This folder contains related Cloudformation for project deployment on AWS :
    * **bs_ml_pipeline_cloudformation_template**: AWS resources to set up ml-pipeline. 
    * **bs_jenkins**: Jenkins file to run ml and etl pipelines.
    * **bs_etl_pipeline_cloudformation_template**: AWS resources to set up etl-pipeline.
    * **etl_bootstrap**: Bootstrap for EMR. Install on the master and worker nodes the required Python libraries listed in requirements.txt.
    * **etl_emr_spark_submit**: Submit spark job for the etl pipeline.
    * **etl_emr_spark_test**: Submit unit and data validation tests for the etl pipeline.

* #### notebooks: This folder contains important notebooks related to the project for model design/test:
    * **study**: This folder contains reports and graphs related to our study and exploring data in different phases of the project.
    * **model_dataset_generation**: This folder contains notebooks related to data set generation. Mainly used CTAS for this purpose.
        If it does not work well we may use Spark-based notebooks.
    * **model_train_validate_deploy**: This is the folder contains model training validating and deployment on SageMaker environment.

* #### sample_data: This folder contains sample data for local train/validate/test files related to the project.

* #### src_etl: utilizing AWS EMR, collects raw consumer behavior data from the datalake, run ETL, and writes the transformed data into a S3 bucket in rdc for machine learning model training as well as inferencing
    * **config**: 
        * **constants**: constant variables needed to run each step in the ETL pipeline
        * **settings**: parameter mappings needed for each step in the ETL pipeline
    * **steps**: 
        * **process_data_from_dataeng_s3.py**: loads the data from the datalake S3 onto the worker nodes for processing, persists the data into rdc S3, and extract a portion of the raw data for testing purposes
    * **main.py**: entry point for EMR when running the ETL pipeline
    * **process_data_from_dataeng_s3.py**: a number of functions that does the ETL: extract needed features, remove outliers and nulls, one-hot-encode qualitative features, order the columns and remove header
    * **test.py**: unit tests that checks whether result from each function inside `process_data_from_dataeng_s3.py` is as expected; data validation tests that checks whether end product is same as running the original Athena queries on the raw data

* #### src_ml: Related source codes like lambda functions for buyer stage ML pipeline.
    * **lambda_functions**: 
        * **bs_lambda_send_sns_notification**: Sends SnS Notification if error happens in ml pipeline.
        * **bs_lambda_check_if_daily_data_available**: Checks if daily data is available for buyer stage prediction.
        * **bs_lambda_check_if_model_available**: Checks if buyer stage model available.
        * **bs_lambda_check_if_training_data_available**: Checks if training data available to train model.
        * **bs_lambda_check_uploading_to_dynamo_db_status**: Checks if data uploaded to DynamoDb
        * **bs_lambda_start_automl**: Starts SageMaker AutoMl Job.
        * **bs_lambda_check_automl_status**: Checks AutoMl job status.
        * **bs_lambda_create_and_save_model**: Saves and creates model from AutoMl result for batch transformation.
        * **bs_lambda_start_batch_stransformation**: Starts batch transformation job for buyer stage.
        * **bs_lambda_check_batch_transformation_status**: Checks the status of batch transformation.
        * **bs_lambda_start_uploading_to_dynamo_db**: Starts uploading data into dynamo db.
    * **bs_ml_pipeline_state_machine**: 
        * Generates state machine for buyer_stage ML pipeline.
    * **bs_push2ddb_glue**: 
        * Pushes inferenced data into Dynamo DB using glueetl.  

* #### tests: 
    * Contains the related tests for each designed module. It is notebook based and 
      standard python unittests.
    
## **Buyer Stage General Architecture**

The following picture shows the high-level architecture of the buyer stage as two separate stacks, ETL and ML.


![ML State Diagram](/buyer-stage-architecture.png)

## **Buyer Stage ML Pipeline State Machine**

We are mainly using **AWS** **S3**, **SNS**, **CloudWatch**, **EventBridge**, **Lambda**, **SageMaker**, **DynamoDb** and **Glue**  
Following picture shows the related step function states for buyer stage ML pipeline:


![ML State Diagram](/buyer-satge-ml-pipeline.png)

## **Buyer Stage ML Pipeline Deployment using Cloudformation Graph**

To deploy buyer stage ml pipeline, we used cloudformation. The following picture shows the resources used in the related 
template:


![ML State Diagram](/buyer-stage-cloudformation-template.png)


## **Getting Started**

#### ETL Pipeline: 
1. Inside Jenkins, search for **buyer-stage-pipelines**
![buyer-stage-etl-jenkins](/buy-stage-etl-jenkins.png)

2. To start the ETL pipeline, input the settings:
    * `ACCOUNT_NAME`: dataeng 
    * `ACCOUNT_ENV`: dev
    * `ACTION`: 
        * `create-stack`: to create a cloudformation stack
    * `BRANCH`: master, or any branch under development
    * `PROJECT`: buyer-stage-pipelines
    * `STACKTYPE`: buyer-stage-etl-pipeline
    * `CODE_S3_PATH`: s3://dataeng-buyer-stage-ml-dev/etl-pipeline-code
    * `SRC_DATA_BUCKET`: s3://move-dataeng-bd-prod
    * `DST_DATA_BUCKET`: s3://rdc-buyer-stage-ml-dev
    * `STEPS_TO_RUN`: all

3. Upon clicking `Build`, the Jenkins job will spin up a cloudformation template using the `bs_etl_pipeline_cloudformation_template.yml`. Currently, the settings are:
    * Account: dataeng_dev
    * Run, in order, 
        1. deployment/etl_bootstrap.sh
        2. src_etl/main.py
        3. src_etl/steps/prcess_data_from_dataeng_s3.py
        4. src_etl/prcess_data_from_dataeng_s3.py
        5. src_etl/test.py
    * Master instance: 1 * m4.xlarge (spot pricing)
    * Core instance: 2 * r4.16xlarge (spot pricing)
    * Schedule: session runs every 8hrs (60min duration maximum for each session)
    * Data source: two-day old data from 
        * s3://move-dataeng-bd-prod/cnsp/consumer_analytical_profile_member_id_summary_t001
        * s3://move-dataeng-bd-prod/cnsp/consumer_analytical_profile_member_id_summary_t007
        * s3://move-dataeng-bd-prod/cnsp/consumer_analytical_profile_member_id_summary_t030
        * s3://move-dataeng-bd-prod/cnsp/consumer_analytical_profile_member_id_summary_t060
        * s3://move-dataeng-bd-prod/cnsp/consumer_analytical_profile_member_id_summary_t090
    * SNS subscriptions: 
        * reza.rahimi@move.com 
        * david.kong@move.com

4. The cloudformation template will spin up an EMR instance. Check under EMR to make sure it started without errors

5. The EMR instance will run the `STEPS_TO_RUN` specified in the Jenkins setting. The default is `all`, which will run all the functions inside `src_etl/process_data_from_dataeng_s3.py` on the raw data and eventually write to the `DST_DATA_BUCKET`. This process should take less than an hour

6. After writing the transformed data to `DST_DATA_BUCKET`, the EMR instance will go on to run the unit and data validation tests in `src_etl/test.py`. The output of the test results can be found in the EMR log (std_out.gz)

7. To shut down the EMR instance and delete all the resources associated with the ETL pipeline, go back to Jenkins and run,
    * `ACCOUNT_NAME`: dataeng 
    * `ACCOUNT_ENV`: dev
    * `ACTION`: 
        * `delete-stack`: to delete a cloudformation stack
    * `BRANCH`: master, or any branch under development
    * `PROJECT`: buyer-stage-pipelines
    * `STACKTYPE`: buyer-stage-etl-pipeline
    * `CODE_S3_PATH`: s3://dataeng-buyer-stage-ml-dev/etl-pipeline-code
    * `SRC_DATA_BUCKET`: s3://move-dataeng-bd-prod
    * `DST_DATA_BUCKET`: s3://rdc-buyer-stage-ml-dev
    * `STEPS_TO_RUN`: all

#### ML Pipeline:

2. To start the ML pipeline, input the settings:
    * `ACCOUNT_NAME`: moverdc 
    * `ACCOUNT_ENV`: dev/qa/prod
    * `ACTION`: 
        * `create-stack`: to create a ml stack
        * `update-stack`: to update ml stack
        * `update-lambdas`: to update lambdas codes
        * `start-ml-pipeline`: to start ml pipelines
        * `delete-ml-model`: to delete ml model
        * `delete-stack`: to delete ml stack
    * `BRANCH`: master, or any branch under development
    * `PROJECT`: buyer-stage-pipelines
    * `STACKTYPE`: buyer-stage-ml-pipeline
   
## **Project Python Version**

Python 3.7
    