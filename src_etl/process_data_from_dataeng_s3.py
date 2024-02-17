import pyspark.sql.functions as F
import time
import pandas as pd
from datetime import datetime

def append_new_columns(df,partition,current_date):
    print("append_new_columns")
    return df\
              .withColumnRenamed("member_id",f"member_id_{partition}")\
              .withColumnRenamed("median_listings_viewed",f"median_listings_viewed_{partition}")\
              .withColumnRenamed("average_listings_viewed",f"average_listings_viewed_{partition}")\
              .withColumn(f"snapshot_date_mst_yyyymmdd_{partition}",F.lit(current_date))

def eliminate_nulls(df,partition,cols_with_nulls,default_integer):
    print("eliminate_nulls")
    df_transformed = df
    for col in cols_with_nulls:
        df_transformed = df_transformed\
                            .fillna({col:default_integer})\
                            .withColumnRenamed(col,f"{col}_{partition}")
    return df_transformed

def remove_outliers(df,partition,cols_with_outliers,default_integer,default_dwell_threshhold):
    print("remove_outliers")
    df_transformed = df
    for col in cols_with_outliers:
        df_transformed = df_transformed\
                            .fillna({col:default_integer})\
                            .withColumn(f"{col}_{partition}",F.when(F.col(col)>default_dwell_threshhold,default_dwell_threshhold).otherwise(F.col(col)))\
                            .drop(col)
    return df_transformed

def one_hot_encode(df,partition,cols_options_to_one_hot_encode):
    print("one_hot_encode")
    df_transformed = df
    for col,options in cols_options_to_one_hot_encode.items():
        for option in options:
            option_reformatted = option.replace(" ","_")
            if (col == 'ldp_dominant_experience_type') and (option == 'mobile apps'):
                df_transformed = df_transformed\
                                .withColumn(f'ldp_dominant_experience_mobile_app_encoded_{partition}',
                                            (F.trim(F.col(col))==option).cast('byte'))
            else:
                df_transformed = df_transformed\
                                .withColumn(f'{col}_{option_reformatted}_{partition}',
                                            (F.trim(F.col(col))==option).cast('byte'))
        df_transformed = df_transformed.drop(col)
    return df_transformed

def drop_unnecessary_cols(df,cols_to_drop):
    print("drop_unnecessary_cols")
    return df.drop(*cols_to_drop)

def transform_dfs(dfs,current_date,cols_with_nulls,cols_with_outliers,cols_options_to_one_hot_encode,cols_to_drop,default_integer,default_dwell_threshhold):
    print("transform_dfs")
    for day_partition in dfs.keys():
        partition = day_partition[1:] # Ex. 001,007,030,060,090
        df_transformed = dfs[day_partition]
        df_transformed = append_new_columns(df_transformed,partition,current_date)
        df_transformed = eliminate_nulls(df_transformed,partition,cols_with_nulls,default_integer)
        df_transformed = remove_outliers(df_transformed,partition,cols_with_outliers,default_integer,default_dwell_threshhold)
        df_transformed = one_hot_encode(df_transformed,partition,cols_options_to_one_hot_encode)
        df_transformed = drop_unnecessary_cols(df_transformed,cols_to_drop)
        dfs[day_partition] = df_transformed
        print(f"Transformed {day_partition} data!")
        print("="*60)
    return dfs

def outer_join_dfs(dfs_transformed):
    print("outer_join_dfs")
    day_partitions = list(dfs_transformed.keys()) # t001,t007,t030,t060,t090
    dfs_joined = dfs_transformed[day_partitions[0]]
    for i in range(1,len(day_partitions)): # t007,t030,t060,t090
        dfs_joined = dfs_joined.join(dfs_transformed[day_partitions[i]],
                                     dfs_joined[f"member_id_{day_partitions[i-1][1:]}"] == dfs_transformed[day_partitions[i]][f"member_id_{day_partitions[i][1:]}"],
                                     how="outer")
    return dfs_joined

def coalesce_memberid_snapshotdate(dfs_joined,day_partitions):
    print("coalesce_memberid_snapshotdate")
    cols_to_coalesce = ['member_id','snapshot_date_mst_yyyymmdd']
    df = dfs_joined
    for col in cols_to_coalesce:
        df = df\
                .withColumn(col,F.coalesce(*[F.col(f"{col}_{day_partition[1:]}") for day_partition in day_partitions]))\
                .drop(*[f"{col}_{day_partition[1:]}" for day_partition in day_partitions])
    return df

def append_time_info(df,current_date,day_partitions):
    print("append_time_info")
    year = current_date[:4]
    month = current_date[4:6]
    day = current_date[6:]
    dow = datetime.strptime(f'{year} {month} {day}','%Y %m %d').weekday() + 1 # day of the week
    dow = 1 if dow == 7 else dow + 1 # To be consistent with SQL
    return df\
            .withColumn("snapshot_month",F.lit(int(month)))\
            .withColumn("snapshot_day_of_month",F.lit(int(day)))\
            .withColumn("snapshot_day_of_week",F.lit(int(dow)))\
            .drop('snapshot_date_mst_yyyymmdd')

def fill_null(df):
    print("fill_null")
    return df.na.fill(0)

def order_columns(df,column_ordered):
    print("order_columns")
    return df.select(column_ordered)

def run_process_data_from_dataeng_s3(spark,config,dfs):
    dfs_transformed = transform_dfs(dfs,
                                    config['current_date'],
                                    config['cols_with_nulls'],
                                    config['cols_with_outliers'],
                                    config['cols_options_to_one_hot_encode'],
                                    config['cols_to_drop'],
                                    config['default_integer'],
                                    config['default_dwell_threshhold'])
    dfs_joined = outer_join_dfs(dfs_transformed)
    df = coalesce_memberid_snapshotdate(dfs_joined,config['day_partitions'])
    df = append_time_info(df,config['current_date'],config['day_partitions'])
    df = fill_null(df)
    df = order_columns(df,config['expected_output_cols'])
    df.printSchema()
    return df