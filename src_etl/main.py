import importlib
from pyspark.sql import SparkSession
from config import settings

if __name__ == "__main__":
    
    spark = SparkSession.builder.appName("buyer-intent-etl-pipeline").getOrCreate()
    spark._jsc.hadoopConfiguration().set("fs.s3.canned.acl","BucketOwnerFullControl")

    for step in settings.pipeline_steps:
        print(f"RUNNING {step['name']}")

        step_module = importlib.import_module(f"steps.{step['name']}")
        step_module.process(spark, step['config'])

        print(f"FINISHED {step['name']}")