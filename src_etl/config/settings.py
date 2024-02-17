import sys
import datetime
from config.constants import *

# System Args order:
# 1 Env, 2 CurrentYear, 3 CurrentMonth, 4 CurrentDay, 5 SrcDataBucket, 6 DstDataBucket, 7 StepsToRun

env = 'dev'
src_data_bucket = 's3://move-dataeng-bd-prod'
dst_data_bucket = 's3://rdc-buyer-stage-ml-dev'
steps_to_run = 'all'

try:
    print("Sys Arguments:")
    for i in range(4):
        print(sys.argv[i+1])

    env = sys.argv[1]
    src_data_bucket = sys.argv[2]
    dst_data_bucket = sys.argv[3]
    steps_to_run = sys.argv[4]
except:
    print('Argument does not exist')

def should_run_step(step_name):
    if (len(steps_to_run) == 0) or (steps_to_run == "all"): 
        return True
    return step_name in steps_to_run

# Constants reformatted
current_date = datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(3), '%Y-%m-%d')
current_year,current_month,current_day = current_date.split("-")[0],current_date.split("-")[1],current_date.split("-")[2]
current_date = datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(3), '%Y%m%d')
day_partitions = day_partitions.split("-") # Ex. ['t001','t007','t030','t060','t090']

test_data_s3_path = "%s/%s/%s/%s/%s" % (
    dst_data_bucket,
    test_data_prefix,
    current_year,
    current_month,
    current_day
)

dst_data_s3_path = "%s/%s/%s/%s/%s" % (
    dst_data_bucket,
    dst_data_prefix,
    current_year,
    current_month,
    current_day
)

process_data_from_dataeng_s3 = {
    'name': "process_data_from_dataeng_s3",
    'config': {
        "active": should_run_step("process_data_from_dataeng_s3"),
        "src_data_bucket": src_data_bucket,
        "test_data_s3_path": test_data_s3_path,
        "repartition_num": repartition_num,
        "day_partitions": day_partitions,
        "sample_percentage": sample_percentage,
        "current_date": current_date,
        "cols_with_nulls": cols_with_nulls,
        'cols_with_outliers': cols_with_outliers,
        'cols_options_to_one_hot_encode': cols_options_to_one_hot_encode,
        'cols_to_drop': cols_to_drop,
        'expected_output_cols': expected_output_cols,
        "default_integer": default_integer,
        "default_dwell_threshhold": default_dwell_threshhold,
        "dst_data_s3_path": dst_data_s3_path
    }
}

pipeline_steps = [
    process_data_from_dataeng_s3
]

testing = {
    'config': {
        "test_data_s3_path": test_data_s3_path,
        "day_partitions": day_partitions,
        "current_date": current_date,
        "cols_with_nulls": cols_with_nulls,
        "default_integer": default_integer,
        'cols_with_outliers': cols_with_outliers,
        "default_dwell_threshhold": default_dwell_threshhold,
        'cols_options_to_one_hot_encode': cols_options_to_one_hot_encode,
        'cols_to_drop': cols_to_drop,
        'expected_output_cols': expected_output_cols
    }
}