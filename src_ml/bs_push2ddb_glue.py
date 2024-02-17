from __future__ import print_function

import sys
import time
from datetime import timedelta

import boto3
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
logger = glueContext.get_logger()

# Retrieve parameters for the Glue job.
args = getResolvedOptions(sys.argv,
                          ['ENV', 'JOB_NAME', 'REGION', 'S3_SOURCE', 'NUMBER_OF_PARTITIONS',
                           'DYNAMO_DB_TABLE', 'NUMBER_OF_DAYS_TTL'])


def push2ddb(line):
    try:
        dynamo_db = boto3.resource('dynamodb', region_name=args['REGION'])
        dynamo_db_table = dynamo_db.Table(args['DYNAMO_DB_TABLE'])
        response = dynamo_db_table.put_item(
            Item={
                'member_id': str(line[0]),
                'value': str(line[1]),
                'ttl': int(time.time()) + int(timedelta(days=int(args['NUMBER_OF_DAYS_TTL'])).total_seconds())
            })
        return 1
    except Exception as message:
        return 0


def main():
    try:
        logger.info("Start Uploading Data into DynamoDB!")
        df = spark.read.load(args['S3_SOURCE'], format='csv', header=False)
        df = df.repartition(int(args['NUMBER_OF_PARTITIONS']))
        number_of_pushed_item = df.rdd.map(push2ddb).sum()
        logger.info("Number of pushed item is : " + str(number_of_pushed_item))
    except Exception as message:
        logger.error('error message is ' + message.__str__())
        return


if __name__ == "__main__":
    main()
