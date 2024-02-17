CREATE EXTERNAL TABLE IF NOT EXISTS {db_name}.qualtrics_survey (
      `member_id` string, 
      `start_date` string, 
      `end_date` string, 
      `stage_label_1` string, 
      `stage_label_2` string, 
      `stage_label_3` string)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION '{s3_path_qualtrics_survey}'
    TBLPROPERTIES ("skip.header.line.count"="1");