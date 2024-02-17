create TABLE {db_name}.labeled_data_set_{table_generation_date}
    with (
    external_location = '{s3_staging_path}/training_sets/labeled_data_set_{table_generation_date}',
    format='PARQUET'
    ) AS

select {db_name}.{data_set_table}.*,
       {db_name}.{qualtrics_survey}.stage_label_1, 
       {db_name}.{qualtrics_survey}.stage_label_2, 
       {db_name}.{qualtrics_survey}.stage_label_3
    from {db_name}.{data_set_table}
    INNER JOIN {db_name}.{qualtrics_survey}
    ON 
    {db_name}.{data_set_table}.member_id = {db_name}.{qualtrics_survey}.member_id AND
    abs(date_diff('day', date_parse({db_name}.{data_set_table}.snapshot_date_mst_yyyymmdd, '%Y%m%d'), date_parse({db_name}.{qualtrics_survey}.start_date, '%Y%m%d'))) <= 2;