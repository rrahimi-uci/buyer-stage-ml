import datetime
import time
import boto3
from process_data_from_dataeng_s3 import run_process_data_from_dataeng_s3 
from boto3.s3.transfer import TransferConfig

s3 = boto3.client('s3')
s3_resource = boto3.resource("s3")

def load_df(spark,src_data_bucket,day_partitions,current_date,repartition_num):
    def load_day_partition(day_partition):
        return spark.read.parquet(f"{src_data_bucket}/cnsp/consumer_analytical_profile_member_id_summary_{day_partition}/snapshot_date_mst_yyyymmdd={current_date}")
    return {day_partition:load_day_partition(day_partition) for day_partition in day_partitions}

def persist_df_for_testing(dfs,sample_percentage,test_data_write_path):
    for day_partition,df in dfs.items():
        df_partial = df.sample(False,sample_percentage)
        df_partial.write.parquet(f'{test_data_write_path}/{day_partition}', mode='overwrite')

def persist_df(df,write_path):
    df\
        .coalesce(1)\
        .write\
        .format("com.databricks.spark.csv")\
        .mode("overwrite")\
        .option("header","false")\
        .save(write_path)

    time.sleep(120)
    bucket = write_path[5:].split('/')[0]
    prefix = "/".join(write_path[5:].split('/')[1:])
    res = s3.list_objects(Bucket=bucket,Prefix=prefix)    
    keys = [content['Key'] for content in res['Contents']]
    for key in keys:
        if "_SUCCESS" in key:
            print("Deleting _SUCCESS file")
            success_file = s3_resource.Object(bucket,key)
            success_file.delete()
        else:
            print("Rename file")
            s3.download_file(bucket,key,"buyer_stage_input.csv")
            time.sleep(120)
            s3.upload_file(
                "buyer_stage_input.csv",bucket,f'{prefix}/buyer_stage_input.csv',
                Config = TransferConfig(multipart_threshold=1024*25, max_concurrency=10,
                                        multipart_chunksize=1024*25, use_threads=True),
                ExtraArgs={'ACL': 'bucket-owner-full-control'}
            )
            obsolete_file = s3_resource.Object(bucket,key)
            obsolete_file.delete()
            
def process(spark,config):  

    if config['active']:
        print(f"Process data from dataeng S3 starts {datetime.datetime.now()}")

        dfs = load_df(spark,config['src_data_bucket'],config['day_partitions'],config['current_date'],config['repartition_num'])
        persist_df_for_testing(dfs,config['sample_percentage'],config['test_data_s3_path'])
        df = run_process_data_from_dataeng_s3(spark,config,dfs)
        persist_df(df,config['dst_data_s3_path'])

        print(f'Process data from dataeng S3 done {datetime.datetime.now()}')

    else:
        print("Skip process data from dataeng S3")