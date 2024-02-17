import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# Retrieve parameters for the Glue job.
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_SOURCE', 'S3_DEST', 'TRAIN_KEY', 'TEST_KEY', 'TEST_KEY_NO_TARGET'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Create a PySpark dataframe from the source table.
df = spark.read.load(args['S3_SOURCE'], format='csv', header=True)

columns_to_drop = ['member_id_001', 'member_id_007', 'member_id_030', 'member_id_060', 'member_id_090',
                   'snapshot_date_mst_yyyymmdd', 'snapshot_date_mst_yyyymmdd_001', 'snapshot_date_mst_yyyymmdd_007',
                   'snapshot_date_mst_yyyymmdd_030',
                   'snapshot_date_mst_yyyymmdd_060', 'snapshot_date_mst_yyyymmdd_090']
df = df.drop(*columns_to_drop)
df = df.na.fill(0)

# Split the dataframe in to training and validation dataframes.
train_df, test_df = df.randomSplit([.7, .3])

# remove member_id for training data frame
member_id = ['member_id']
train_df = train_df.drop(*member_id)

target=['buyer_stage']
test_df_no_target = test_df.drop(*target)

# Write both dataframes to the destination datastore.
train_path = args['S3_DEST'] + args['TRAIN_KEY']
test_path = args['S3_DEST'] + args['TEST_KEY']
test_path_no_target = args['S3_DEST'] + args['TEST_KEY_NO_TARGET']

train_df.write.save(train_path, format='csv', mode='overwrite', header=True)
test_df.write.save(test_path, format='csv', mode='overwrite', header=False)
test_df_no_target.write.save(test_path_no_target, format='csv', mode='overwrite', header=False)

# Complete the job.
job.commit()
