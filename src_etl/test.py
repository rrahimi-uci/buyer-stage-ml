import sys
import datetime
import unittest
import pandas as pd
import importlib
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import process_data_from_dataeng_s3 as pdas
from config.constants import (cols_with_nulls,default_integer,cols_with_outliers,default_dwell_threshhold,
                              cols_options_to_one_hot_encode,cols_to_drop,expected_output_cols,
                              test_data_prefix,sql_query_base,sql_query_training_set,num_smoke_tests,
                              day_partitions)

dst_data_bucket = 's3://rdc-buyer-stage-ml-dev'

try:
    print("Sys Arguments:")
    for i in range(1):
        print(sys.argv[i+1])
        
    dst_data_bucket = sys.argv[1]
except:
    print('Argument does not exist')

# Constants
current_date = datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(2), '%Y-%m-%d')
current_year,current_month,current_day = current_date.split("-")[0],current_date.split("-")[1],current_date.split("-")[2]
current_date = datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(2), '%Y%m%d')
day_partitions = day_partitions.split("-") # Ex. ['t001','t007','t030','t060','t090']
test_data_s3_path = "%s/%s/%s/%s/%s" % (
    dst_data_bucket,
    test_data_prefix,
    current_year,
    current_month,
    current_day
)

class ETLPipelineTest(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        """This method is run first"""
        print("setUpClass")
        print("="*60)

        """This method is run first"""
        global spark
        spark = SparkSession.builder.appName("buyer-intent-etl-pipeline-tests").getOrCreate()
        spark._jsc.hadoopConfiguration().set("fs.s3.canned.acl","BucketOwnerFullControl")
        
        # For unit test
        global dfs_after_append_new_columns,dfs_after_eliminate_nulls,dfs_after_remove_outliers,dfs_after_one_hot_encode,dfs_after_drop_unnecessary_cols
        dfs_after_append_new_columns,dfs_after_eliminate_nulls,dfs_after_remove_outliers,dfs_after_one_hot_encode,dfs_after_drop_unnecessary_cols = {},{},{},{},{}
        for day_partition in day_partitions:
            partition = day_partition[1:] # Ex. 001,007,030,060,090
            dfs_day_partition = spark.read.parquet(f"{test_data_s3_path}/{day_partition}")
            dfs_day_partition\
                .withColumn('snapshot_date_mst_yyyymmdd',F.lit(current_date))\
                .createGlobalTempView(day_partition)
            
            dfs_after_append_new_columns[day_partition] = pdas.append_new_columns(dfs_day_partition,partition,current_date)
            dfs_after_eliminate_nulls[day_partition] = pdas.eliminate_nulls(dfs_after_append_new_columns[day_partition],partition,cols_with_nulls,default_integer)
            dfs_after_remove_outliers[day_partition] = pdas.remove_outliers(dfs_after_eliminate_nulls[day_partition],partition,cols_with_outliers,default_integer,default_dwell_threshhold)
            dfs_after_one_hot_encode[day_partition] = pdas.one_hot_encode(dfs_after_remove_outliers[day_partition],partition,cols_options_to_one_hot_encode)
            dfs_after_drop_unnecessary_cols[day_partition] = pdas.drop_unnecessary_cols(dfs_after_one_hot_encode[day_partition],cols_to_drop)

        global df_after_outer_join_dfs,df_after_coalesce_memberid_snapshotdate,df_after_append_time_info,df_after_fill_null,df_after_order_columns
        df_after_outer_join_dfs = pdas.outer_join_dfs(dfs_after_drop_unnecessary_cols)
        df_after_coalesce_memberid_snapshotdate = pdas.coalesce_memberid_snapshotdate(df_after_outer_join_dfs,day_partitions)
        df_after_append_time_info = pdas.append_time_info(df_after_coalesce_memberid_snapshotdate,current_date,day_partitions)
        df_after_fill_null = pdas.fill_null(df_after_append_time_info)
        df_after_order_columns = pdas.order_columns(df_after_fill_null,expected_output_cols)

        # For smoke test
        joined_df = spark.sql(sql_query_base.replace("DAY_PARTITION",day_partitions[0]).replace("PARTITION",day_partitions[0][1:]))
        for i in range(1,len(day_partitions)):
            df = spark.sql(sql_query_base.replace("DAY_PARTITION",day_partitions[i]).replace("PARTITION",day_partitions[i][1:]))
            joined_df = joined_df.join(df,
                                       joined_df[f"member_id_{day_partitions[i-1][1:]}"] == df[f"member_id_{day_partitions[i][1:]}"],
                                       how="outer")
        joined_df.createGlobalTempView('joined_df')

        global expected_results,actual_results
        expected_results = spark\
            .sql(sql_query_training_set)\
            .select(expected_output_cols)\
            .na.fill(0)
        actual_results = df_after_order_columns

    @classmethod
    def tearDownClass(cls):
        print("tearDownClass")
        print("="*60)

        """This method is run last"""
        spark.stop()

    def test_append_new_columns(self):
        print("test_append_new_columns")
        print("="*60)

        for day_partition,df in dfs_after_append_new_columns.items():
            partition = day_partition[1:]

            self.assertIn(f"member_id_{partition}",dfs_after_append_new_columns[day_partition].columns,
                          f"column member_id_{partition} should exist")
            self.assertNotIn(f"member_id",dfs_after_append_new_columns[day_partition].columns,
                             f"column member_id should not exist")
            self.assertIn(f"snapshot_date_mst_yyyymmdd_{partition}",dfs_after_append_new_columns[day_partition].columns,
                          f"column snapshot_date_mst_yyyymmdd_{partition} should exist")

        print("Passed test_append_new_columns\n")

    def test_eliminate_nulls(self):
        print("test_eliminate_nulls")
        print("="*60)

        for day_partition,df in dfs_after_eliminate_nulls.items():
            partition = day_partition[1:]

            for col in cols_with_nulls:

                self.assertNotIn(f"{col}",dfs_after_eliminate_nulls[day_partition].columns,
                                 f"column {col} should not exist")
                self.assertIn(f"{col}_{partition}",dfs_after_eliminate_nulls[day_partition].columns,
                              f"column {col}_{partition} should exist")
                self.assertEqual(dfs_after_eliminate_nulls[day_partition].where(F.col(f'{col}_{partition}').isNull()).count(),0,
                                 f"column {col}_{partition} should have 0 nulls")

        print("Passed test_eliminate_nulls\n")

    def test_remove_outliers(self):
        print("test_remove_outliers")
        print("="*60)

        for day_partition,df in dfs_after_remove_outliers.items():
            partition = day_partition[1:] 
            
            for col in cols_with_outliers:

                self.assertNotIn(f"{col}",dfs_after_remove_outliers[day_partition].columns,
                                 f"column {col} should not exist")
                self.assertIn(f"{col}_{partition}",dfs_after_remove_outliers[day_partition].columns,
                              f"column {col}_{partition} should exist")
                self.assertEqual(dfs_after_remove_outliers[day_partition].where(F.col(f'{col}_{partition}').isNull()).count(),0,
                                 f"column {col}_{partition} should have 0 nulls")
                self.assertEqual(dfs_after_remove_outliers[day_partition].filter(F.col(f"{col}_{partition}")>default_dwell_threshhold).count(),0,
                                 f"column {col}_{partition} should not have values larger than outlier threshold {default_dwell_threshhold}")

        print("Passed test_remove_outliers\n")

    def test_one_hot_encode(self):
        print("test_one_hot_encode")
        print("="*60)

        for day_partition,df in dfs_after_one_hot_encode.items():
            partition = day_partition[1:] 

            for col,options in cols_options_to_one_hot_encode.items():
                self.assertNotIn(f"{col}",dfs_after_one_hot_encode[day_partition].columns,
                                 f"column {col} should not exist")

                for option in options:
                    option_reformatted = option.replace(" ","_")
                    if (col == 'ldp_dominant_experience_type') and (option == 'mobile apps'):
                        self.assertEqual(dfs_after_one_hot_encode[day_partition].filter((F.col(f'ldp_dominant_experience_mobile_app_encoded_{partition}')==1) | (F.col(f'ldp_dominant_experience_mobile_app_encoded_{partition}')==0)).count(),dfs_after_one_hot_encode[day_partition].count(),
                                         f"column ldp_dominant_experience_mobile_app_encoded_{partition} should have values of either 1 or 0")
                    else:
                        self.assertEqual(dfs_after_one_hot_encode[day_partition].filter((F.col(f'{col}_{option_reformatted}_{partition}')==1) | (F.col(f'{col}_{option_reformatted}_{partition}')==0)).count(),dfs_after_one_hot_encode[day_partition].count(),
                                         f"column {col}_{option_reformatted}_{partition} should have values of either 1 or 0")

        print("Passed test_remove_outliers\n")

    def test_drop_unnecessary_cols(self):
        print("test_drop_unnecessary_cols")
        print("="*60)

        for day_partition,df in dfs_after_drop_unnecessary_cols.items():
            partition = day_partition[1:] 

            for col in cols_to_drop:
                self.assertNotIn(f"{col}",dfs_after_drop_unnecessary_cols[day_partition].columns,
                                 f"column {col} should not exist")

        print("Passed test_drop_unnecessary_cols\n")

    def test_outer_join_dfs(self):
        print("test_outer_join_dfs")
        print("="*60)
        
        distinct_non_null_member_ids = 0
        for day_partition in day_partitions:
            partition = day_partition[1:]
            distinct_non_null_member_ids += df_after_outer_join_dfs.filter(F.col(f"member_id_{partition}").isNotNull()).distinct().count()
        
        self.assertGreaterEqual(distinct_non_null_member_ids,df_after_outer_join_dfs.count(),
                                f"should include all distinct member_ids")

        print("Passed test_outer_join_dfs\n")

    def test_coalesce_memberid_snapshotdate(self):
        print("test_coalesce_memberid_snapshotdate")
        print("="*60)

        for day_partition in day_partitions:
            partition = day_partition[1:]
            
            self.assertNotIn(f"snapshot_date_mst_yyyymmdd_{partition}",df_after_coalesce_memberid_snapshotdate.columns,
                             f"column snapshot_date_mst_yyyymmdd_{partition} should not exist")
            self.assertNotIn(f"member_id_{partition}",df_after_coalesce_memberid_snapshotdate.columns,
                             f"column member_id_{partition} should not exist")

        self.assertIn(f"snapshot_date_mst_yyyymmdd",df_after_coalesce_memberid_snapshotdate.columns,
                        f"column snapshot_date_mst_yyyymmdd should exist")
        self.assertIn(f"member_id",df_after_coalesce_memberid_snapshotdate.columns,
                        f"column member_id should exist")
        self.assertEqual(df_after_coalesce_memberid_snapshotdate.filter(F.col('snapshot_date_mst_yyyymmdd').isNull()).count(),0,
                         f"column snapshot_date_mst_yyyymmdd should not contain any null values")
        self.assertEqual(df_after_coalesce_memberid_snapshotdate.filter(F.col('member_id').isNull()).count(),0,
                         f"column member_id should not contain any null values")

        print("Passed test_coalesce_memberid_snapshotdate\n")

    def test_append_time_info(self):
        print("test_append_time_info")
        print("="*60)

        self.assertNotIn(f"snapshot_date_mst_yyyymmdd",df_after_append_time_info.columns,
                         f"column snapshot_date_mst_yyyymmdd should not exist")
        self.assertIn(f"snapshot_month",df_after_append_time_info.columns,
                      f"column snapshot_month should exist")
        self.assertIn(f"snapshot_day_of_month",df_after_append_time_info.columns,
                      f"column snapshot_day_of_month should exist")
        self.assertIn(f"snapshot_day_of_week",df_after_append_time_info.columns,
                      f"column snapshot_day_of_week should exist")

        print("Passed test_append_time_info\n")

    def test_fill_null(self):
        print("test_fill_null")
        print("="*60)

        for col in df_after_fill_null.columns:
            
            self.assertEqual(df_after_fill_null.filter(F.col(col).isNull()).count(),0,
                             f"column {col} should not contain any null values")

        print("Passed test_fill_null\n")

    def test_order_columns(self):
        print("test_order_columns")
        print("="*60)

        self.assertEqual(df_after_order_columns.columns,expected_output_cols,
                         f"columns should contain the same elements and in an expected order")
                         
        print("Passed test_order_columns\n")

    def test_data_validation(self):
        print("test_data_validation")
        print("="*60)

        for _ in range(num_smoke_tests):
            expected_pd = expected_results.sample(0.1).limit(1).toPandas()
            member_id = expected_pd['member_id'].values[0]
            actual_pd = actual_results.filter(F.col('member_id')==member_id).toPandas()

            self.assertEqual(expected_pd.values.tolist()[0],actual_pd.values.tolist()[0],
                             f"For member_id {member_id}, expected {expected_pd.values.tolist()[0]} but got {actual_pd.values.tolist()[0]}")

            for col in expected_pd.columns:
                
                expected_val = expected_pd[col].values[0]
                actual_val = actual_pd[col].values[0]

                self.assertEqual(expected_val,actual_val,
                                 f"Column {col} - expected value {expected_val}, actual value {actual_val}")

        print("Passed test_data_validation\n")

if __name__ == '__main__':

    unittest.main(argv=['first-arg-is-ignored'],exit=False)