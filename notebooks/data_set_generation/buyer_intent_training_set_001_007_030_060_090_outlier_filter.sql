create TABLE {db_name}.buyer_intent_training_set_001_007_030_060_090_{table_generation_date}
    with (
    external_location = '{s3_path_training_sets}/buyer_intent_training_set_001_007_030_060_090_{table_generation_date}',
    format='PARQUET'
    ) AS

    with

    T001 as (
            select
                -- IMPORTANT KEYS TO JOIN ON
                t001.member_id as member_id_001,
                t001.snapshot_date_mst_yyyymmdd as snapshot_date_mst_yyyymmdd_001,
                -- IMPORTANT BEHAVIOURAL SIGNAL 001
                -- COALESCE(t001.ldp_dominant_segment, '{default_string}') AS ldp_dominant_segment_001,
                -- One hot encoding
                (case when trim(t001.ldp_dominant_segment) = 'for sale' then  1 else 0 end) as ldp_dominant_segment_for_sale_001,
                (case when trim(t001.ldp_dominant_segment) = 'not for sale' then 1 else 0 end) as ldp_dominant_segment_not_for_sale_001,
                (case when trim(t001.ldp_dominant_segment) = 'for rent' then  1 else 0 end) as ldp_dominant_segment_for_rent_001,
                (case when trim(t001.ldp_dominant_segment) = 'no_segment' then  1 else 0 end) as ldp_dominant_segment_no_segment_001,
                -- COALESCE(t001.ldp_dominant_zip, '{default_string}') AS ldp_dominant_zip_001,
                coalesce(t001.total_ldp_page_views, {default_integer}) AS total_ldp_page_views_001,
                coalesce(t001.median_ldp_page_views, {default_integer}) AS median_ldp_page_views_001,
                coalesce(t001.average_ldp_page_views, {default_double}) AS average_ldp_page_views_001,
                --------------------------------------- Remove Outliers----------------------------------------------
                (case when coalesce(t001.average_ldp_dwell_time_seconds, {default_double})>{default_dwell_threshhold} THEN {default_dwell_threshhold} ELSE COALESCE(t001.average_ldp_dwell_time_seconds, {default_double}) END) AS average_ldp_dwell_time_seconds_001,
                (case when coalesce(t001.total_ldp_dwell_time_seconds, {default_double})>{default_dwell_threshhold} THEN {default_dwell_threshhold} ELSE COALESCE(t001.total_ldp_dwell_time_seconds, {default_double}) END) AS total_ldp_dwell_time_seconds_001,
                (case when coalesce(t001.median_ldp_dwell_time_seconds, {default_integer})>{default_dwell_threshhold} THEN {default_dwell_threshhold} ELSE COALESCE(t001.median_ldp_dwell_time_seconds, {default_integer}) END) AS median_ldp_dwell_time_seconds_001,
                -----------------------------------------------------------------------------------------------------
                coalesce(t001.total_distinct_listings_viewed,  {default_integer}) AS total_distinct_listings_viewed_001,
                coalesce(t001.total_listings_viewed, {default_integer}) AS total_listings_viewed_001,
                coalesce(t001.median_distinct_listings_viewed,  {default_integer}) AS median_distinct_listings_viewed_001,
                coalesce(t001.average_distinct_listings_viewed, {default_double}) AS average_distinct_listings_viewed_001,
                coalesce(t001.median_listings_viewed, {default_integer}) AS median_listings_viewed_001,
                coalesce(t001.average_listings_viewed, {default_double}) AS average_listings_viewed_001,
                --COALESCE(t001.ldp_dominant_experience_type, '{default_string}') AS ldp_dominant_experience_type_001,
                -- One hot encoding
                (case when trim(t001.ldp_dominant_experience_type) = 'web' then  1 else  0 end) as ldp_dominant_experience_type_web_001,
                (case when trim(t001.ldp_dominant_experience_type) = 'mobile apps'  then  1 else  0 end) as ldp_dominant_experience_mobile_app_encoded_001,
                (case when trim(t001.ldp_dominant_experience_type) = 'mobile web'  then  1 else  0 end) as ldp_dominant_experience_type_mobile_web_001,
                (case when trim(t001.ldp_dominant_experience_type) = 'others'  then  1 else  0 end) as ldp_dominant_experience_type_others_001,
                --COALESCE(t001.ldp_dominant_apps_type, '{default_string}') AS ldp_dominant_apps_type_001,
                -- One hot encoding
                (case when trim(t001.ldp_dominant_apps_type) = 'ios core apps' then  1 else  0 end) as ldp_dominant_apps_type_ios_core_apps_001,
                (case when trim(t001.ldp_dominant_apps_type) = 'android core apps' then  1 else  0 end) as ldp_dominant_apps_type_android_core_apps_001,
                (case when trim(t001.ldp_dominant_apps_type) = 'ios rentals apps' then  1 else  0 end) as ldp_dominant_apps_type_ios_rentals_apps_001,
                (case when trim(t001.ldp_dominant_apps_type) = 'android rentals apps' then  1 else  0 end) as ldp_dominant_apps_type_android_rentals_apps_001,
                (case when trim(t001.ldp_dominant_apps_type) = 'others' then  1 else  0 end) as ldp_dominant_apps_type_others_001,
                coalesce(t001.total_searches, {default_integer}) AS total_searches_001,
                coalesce(t001.median_searches, {default_integer}) AS median_searches_001,
                coalesce(t001.average_searches, {default_double}) AS average_searches_001,
                ----------------------------------- Remove Outlieres -------------------------------------------------
                (case when coalesce(t001.total_srp_dwell_time_seconds, {default_double})>{default_dwell_threshhold} THEN {default_dwell_threshhold} ELSE COALESCE(t001.total_srp_dwell_time_seconds, {default_double}) END) AS total_srp_dwell_time_seconds_001,
                (case when coalesce(t001.average_srp_dwell_time_seconds, {default_double})>{default_dwell_threshhold} THEN {default_dwell_threshhold} ELSE COALESCE(t001.average_srp_dwell_time_seconds, {default_double}) END) AS average_srp_dwell_time_seconds_001,
                (case when coalesce(t001.median_srp_dwell_time_seconds, {default_integer})>{default_dwell_threshhold} THEN {default_dwell_threshhold} ELSE COALESCE(t001.median_srp_dwell_time_seconds, {default_integer}) END) AS median_srp_dwell_time_seconds_001,
                (case when coalesce(t001.total_lead_dwell_time_seconds, {default_double})>{default_dwell_threshhold} THEN {default_dwell_threshhold} ELSE COALESCE(t001.total_lead_dwell_time_seconds, {default_double}) END) AS total_lead_dwell_time_seconds_001,
                (case when coalesce(t001.average_lead_dwell_time_seconds, {default_double})>{default_dwell_threshhold} THEN {default_dwell_threshhold} ELSE COALESCE(t001.average_lead_dwell_time_seconds, {default_double}) END) AS average_lead_dwell_time_seconds_001,
                (case when coalesce(t001.median_lead_dwell_time_seconds, {default_integer})>{default_dwell_threshhold} THEN {default_dwell_threshhold} ELSE COALESCE(t001.median_lead_dwell_time_seconds, {default_integer}) END) AS median_lead_dwell_time_seconds_001,
                -------------------------------------------------------------------------------------------------------
                -- IMPORTANT ACTION SIGNALS 001
                coalesce(t001.total_saved_listings, {default_integer}) AS total_saved_listings_001,
                coalesce(t001.total_shared_listings, {default_integer}) AS total_shared_listings_001,
                coalesce(t001.total_saved_searches, {default_integer}) AS total_saved_searches_001,
                coalesce(t001.total_shared_searches, {default_integer}) AS total_shared_searches_001,
                -- COALESCE(t001.srp_dominant_experience_type, '{default_string}') AS srp_dominant_experience_type_001,
                -- One hot encoding
                (case when trim(t001.srp_dominant_experience_type) = 'web' then  1 else  0 end) as srp_dominant_experience_type_web_001,
                (case when trim(t001.srp_dominant_experience_type) = 'mobile web' then  1 else  0 end) as srp_dominant_experience_type_mobile_web_001,
                --COALESCE(t001.srp_dominant_apps_type, '{default_string}') AS srp_dominant_apps_type_001,
                -- One hot encoding
                (case when trim(t001.srp_dominant_apps_type) = 'others' then  1 else  0 end) as srp_dominant_apps_type_others_001,
                coalesce(t001.total_leads_submitted, {default_integer}) AS total_leads_submitted_001,
                coalesce(t001.submitted_leads_for_rent, {default_integer}) AS submitted_leads_for_rent_001,
                coalesce(t001.submitted_leads_for_sale, {default_integer}) AS submitted_leads_for_sale_001,
                coalesce(t001.submitted_leads_not_for_sale, {default_integer}) AS submitted_leads_not_for_sale_001

            FROM {db_name}.consp_member_id_summary_t001 AS t001
            WHERE replace(substr(t001.snapshot_date_mst_yyyymmdd, 1, 10),'-') = '{table_generation_date}'),

    T007 AS (
            select
                -- IMPORTANT KEYS TO JOIN ON
                t007.member_id as member_id_007,
                t007.snapshot_date_mst_yyyymmdd as snapshot_date_mst_yyyymmdd_007,
                -- IMPORTANT BEHAVIOURAL SIGNAL 007
                -- COALESCE(t007.ldp_dominant_segment, '{default_string}') AS ldp_dominant_segment_007,
                -- One hot encoding
                (case when trim(t007.ldp_dominant_segment) = 'for sale' then  1 else 0 end) as ldp_dominant_segment_for_sale_007,
                (case when trim(t007.ldp_dominant_segment) = 'not for sale' then 1 else 0 end) as ldp_dominant_segment_not_for_sale_007,
                (case when trim(t007.ldp_dominant_segment) = 'for rent' then  1 else 0 end) as ldp_dominant_segment_for_rent_007,
                (case when trim(t007.ldp_dominant_segment) = 'no_segment' then  1 else 0 end) as ldp_dominant_segment_no_segment_007,
                -- COALESCE(t007.ldp_dominant_zip, '{default_string}') AS ldp_dominant_zip_007,
                coalesce(t007.total_ldp_page_views, {default_integer}) AS total_ldp_page_views_007,
                coalesce(t007.median_ldp_page_views, {default_integer}) AS median_ldp_page_views_007,
                coalesce(t007.average_ldp_page_views, {default_double}) AS average_ldp_page_views_007,
                --------------------------------------- Remove Outliers----------------------------------------------
                (case when coalesce(t007.average_ldp_dwell_time_seconds, {default_double})>{default_dwell_threshhold} THEN {default_dwell_threshhold} ELSE COALESCE(t007.average_ldp_dwell_time_seconds, {default_double}) END) AS average_ldp_dwell_time_seconds_007,
                (case when coalesce(t007.total_ldp_dwell_time_seconds, {default_double})>{default_dwell_threshhold} THEN {default_dwell_threshhold} ELSE COALESCE(t007.total_ldp_dwell_time_seconds, {default_double}) END) AS total_ldp_dwell_time_seconds_007,
                (case when coalesce(t007.median_ldp_dwell_time_seconds, {default_integer})>{default_dwell_threshhold} THEN {default_dwell_threshhold} ELSE COALESCE(t007.median_ldp_dwell_time_seconds, {default_integer}) END) AS median_ldp_dwell_time_seconds_007,
                -----------------------------------------------------------------------------------------------------
                coalesce(t007.total_distinct_listings_viewed,  {default_integer}) AS total_distinct_listings_viewed_007,
                coalesce(t007.total_listings_viewed, {default_integer}) AS total_listings_viewed_007,
                coalesce(t007.median_distinct_listings_viewed,  {default_integer}) AS median_distinct_listings_viewed_007,
                coalesce(t007.average_distinct_listings_viewed, {default_double}) AS average_distinct_listings_viewed_007,
                coalesce(t007.median_listings_viewed, {default_integer}) AS median_listings_viewed_007,
                coalesce(t007.average_listings_viewed, {default_double}) AS average_listings_viewed_007,
                --COALESCE(t007.ldp_dominant_experience_type, '{default_string}') AS ldp_dominant_experience_type_007,
                -- One hot encoding
                (case when trim(t007.ldp_dominant_experience_type) = 'web' then  1 else  0 end) as ldp_dominant_experience_type_web_007,
                (case when trim(t007.ldp_dominant_experience_type) = 'mobile apps'  then  1 else  0 end) as ldp_dominant_experience_mobile_app_encoded_007,
                (case when trim(t007.ldp_dominant_experience_type) = 'mobile web'  then  1 else  0 end) as ldp_dominant_experience_type_mobile_web_007,
                (case when trim(t007.ldp_dominant_experience_type) = 'others'  then  1 else  0 end) as ldp_dominant_experience_type_others_007,
                --COALESCE(t007.ldp_dominant_apps_type, '{default_string}') AS ldp_dominant_apps_type_007,
                -- One hot encoding
                (case when trim(t007.ldp_dominant_apps_type) = 'ios core apps' then  1 else  0 end) as ldp_dominant_apps_type_ios_core_apps_007,
                (case when trim(t007.ldp_dominant_apps_type) = 'android core apps' then  1 else  0 end) as ldp_dominant_apps_type_android_core_apps_007,
                (case when trim(t007.ldp_dominant_apps_type) = 'ios rentals apps' then  1 else  0 end) as ldp_dominant_apps_type_ios_rentals_apps_007,
                (case when trim(t007.ldp_dominant_apps_type) = 'android rentals apps' then  1 else  0 end) as ldp_dominant_apps_type_android_rentals_apps_007,
                (case when trim(t007.ldp_dominant_apps_type) = 'others' then  1 else  0 end) as ldp_dominant_apps_type_others_007,
                coalesce(t007.total_searches, {default_integer}) AS total_searches_007,
                coalesce(t007.median_searches, {default_integer}) AS median_searches_007,
                coalesce(t007.average_searches, {default_double}) AS average_searches_007,
                ----------------------------------- Remove Outlieres -------------------------------------------------
                (case when coalesce(t007.total_srp_dwell_time_seconds, {default_double})>{default_dwell_threshhold} THEN {default_dwell_threshhold} ELSE COALESCE(t007.total_srp_dwell_time_seconds, {default_double}) END) AS total_srp_dwell_time_seconds_007,
                (case when coalesce(t007.average_srp_dwell_time_seconds, {default_double})>{default_dwell_threshhold} THEN {default_dwell_threshhold} ELSE COALESCE(t007.average_srp_dwell_time_seconds, {default_double}) END) AS average_srp_dwell_time_seconds_007,
                (case when coalesce(t007.median_srp_dwell_time_seconds, {default_integer})>{default_dwell_threshhold} THEN {default_dwell_threshhold} ELSE COALESCE(t007.median_srp_dwell_time_seconds, {default_integer}) END) AS median_srp_dwell_time_seconds_007,
                (case when coalesce(t007.total_lead_dwell_time_seconds, {default_double})>{default_dwell_threshhold} THEN {default_dwell_threshhold} ELSE COALESCE(t007.total_lead_dwell_time_seconds, {default_double}) END) AS total_lead_dwell_time_seconds_007,
                (case when coalesce(t007.average_lead_dwell_time_seconds, {default_double})>{default_dwell_threshhold} THEN {default_dwell_threshhold} ELSE COALESCE(t007.average_lead_dwell_time_seconds, {default_double}) END) AS average_lead_dwell_time_seconds_007,
                (case when coalesce(t007.median_lead_dwell_time_seconds, {default_integer})>{default_dwell_threshhold} THEN {default_dwell_threshhold} ELSE COALESCE(t007.median_lead_dwell_time_seconds, {default_integer}) END) AS median_lead_dwell_time_seconds_007,
                -------------------------------------------------------------------------------------------------------
                -- IMPORTANT ACTION SIGNALS 007
                coalesce(t007.total_saved_listings, {default_integer}) AS total_saved_listings_007,
                coalesce(t007.total_shared_listings, {default_integer}) AS total_shared_listings_007,
                coalesce(t007.total_saved_searches, {default_integer}) AS total_saved_searches_007,
                coalesce(t007.total_shared_searches, {default_integer}) AS total_shared_searches_007,
                -- COALESCE(t007.srp_dominant_experience_type, '{default_string}') AS srp_dominant_experience_type_007,
                -- One hot encoding
                (case when trim(t007.srp_dominant_experience_type) = 'web' then  1 else  0 end) as srp_dominant_experience_type_web_007,
                (case when trim(t007.srp_dominant_experience_type) = 'mobile web' then  1 else  0 end) as srp_dominant_experience_type_mobile_web_007,
                --COALESCE(t007.srp_dominant_apps_type, '{default_string}') AS srp_dominant_apps_type_007,
                -- One hot encoding
                (case when trim(t007.srp_dominant_apps_type) = 'others' then  1 else  0 end) as srp_dominant_apps_type_others_007,
                coalesce(t007.total_leads_submitted, {default_integer}) AS total_leads_submitted_007,
                coalesce(t007.submitted_leads_for_rent, {default_integer}) AS submitted_leads_for_rent_007,
                coalesce(t007.submitted_leads_for_sale, {default_integer}) AS submitted_leads_for_sale_007,
                coalesce(t007.submitted_leads_not_for_sale, {default_integer}) AS submitted_leads_not_for_sale_007

            FROM {db_name}.consp_member_id_summary_t007 AS t007
            WHERE replace(substr(t007.snapshot_date_mst_yyyymmdd, 1, 10),'-') = '{table_generation_date}'),


    T030 AS (
            select
                -- IMPORTANT KEYS TO JOIN ON
                t030.member_id as member_id_030,
                t030.snapshot_date_mst_yyyymmdd as snapshot_date_mst_yyyymmdd_030,
                -- IMPORTANT BEHAVIOURAL SIGNAL 030
                -- COALESCE(t030.ldp_dominant_segment, '{default_string}') AS ldp_dominant_segment_030,
                -- One hot encoding
                (case when trim(t030.ldp_dominant_segment) = 'for sale' then  1 else 0 end) as ldp_dominant_segment_for_sale_030,
                (case when trim(t030.ldp_dominant_segment) = 'not for sale' then 1 else 0 end) as ldp_dominant_segment_not_for_sale_030,
                (case when trim(t030.ldp_dominant_segment) = 'for rent' then  1 else 0 end) as ldp_dominant_segment_for_rent_030,
                (case when trim(t030.ldp_dominant_segment) = 'no_segment' then  1 else 0 end) as ldp_dominant_segment_no_segment_030,
                -- COALESCE(t030.ldp_dominant_zip, '{default_string}') AS ldp_dominant_zip_030,
                coalesce(t030.total_ldp_page_views, {default_integer}) AS total_ldp_page_views_030,
                coalesce(t030.median_ldp_page_views, {default_integer}) AS median_ldp_page_views_030,
                coalesce(t030.average_ldp_page_views, {default_double}) AS average_ldp_page_views_030,
                --------------------------------------- Remove Outliers----------------------------------------------
                (case when coalesce(t030.average_ldp_dwell_time_seconds, {default_double})>{default_dwell_threshhold} THEN {default_dwell_threshhold} ELSE COALESCE(t030.average_ldp_dwell_time_seconds, {default_double}) END) AS average_ldp_dwell_time_seconds_030,
                (case when coalesce(t030.total_ldp_dwell_time_seconds, {default_integer})>{default_dwell_threshhold} THEN {default_dwell_threshhold} ELSE COALESCE(t030.total_ldp_dwell_time_seconds, {default_integer}) END) AS total_ldp_dwell_time_seconds_030,
                (case when coalesce(t030.median_ldp_dwell_time_seconds, {default_double})>{default_dwell_threshhold} THEN {default_dwell_threshhold} ELSE COALESCE(t030.median_ldp_dwell_time_seconds, {default_double}) END) AS median_ldp_dwell_time_seconds_030,
                -----------------------------------------------------------------------------------------------------
                coalesce(t030.total_distinct_listings_viewed,  {default_integer}) AS total_distinct_listings_viewed_030,
                coalesce(t030.total_listings_viewed, {default_integer}) AS total_listings_viewed_030,
                coalesce(t030.median_distinct_listings_viewed,  {default_integer}) AS median_distinct_listings_viewed_030,
                coalesce(t030.average_distinct_listings_viewed, {default_double}) AS average_distinct_listings_viewed_030,
                coalesce(t030.median_listings_viewed, {default_integer}) AS median_listings_viewed_030,
                coalesce(t030.average_listings_viewed, {default_double}) AS average_listings_viewed_030,
                --COALESCE(t030.ldp_dominant_experience_type, '{default_string}') AS ldp_dominant_experience_type_030,
                -- One hot encoding
                (case when trim(t030.ldp_dominant_experience_type) = 'web' then  1 else  0 end) as ldp_dominant_experience_type_web_030,
                (case when trim(t030.ldp_dominant_experience_type) = 'mobile apps'  then  1 else  0 end) as ldp_dominant_experience_mobile_app_encoded_030,
                (case when trim(t030.ldp_dominant_experience_type) = 'mobile web'  then  1 else  0 end) as ldp_dominant_experience_type_mobile_web_030,
                (case when trim(t030.ldp_dominant_experience_type) = 'others'  then  1 else  0 end) as ldp_dominant_experience_type_others_030,
                --COALESCE(t030.ldp_dominant_apps_type, '{default_string}') AS ldp_dominant_apps_type_030,
                -- One hot encoding
                (case when trim(t030.ldp_dominant_apps_type) = 'ios core apps' then  1 else  0 end) as ldp_dominant_apps_type_ios_core_apps_030,
                (case when trim(t030.ldp_dominant_apps_type) = 'android core apps' then  1 else  0 end) as ldp_dominant_apps_type_android_core_apps_030,
                (case when trim(t030.ldp_dominant_apps_type) = 'ios rentals apps' then  1 else  0 end) as ldp_dominant_apps_type_ios_rentals_apps_030,
                (case when trim(t030.ldp_dominant_apps_type) = 'android rentals apps' then  1 else  0 end) as ldp_dominant_apps_type_android_rentals_apps_030,
                (case when trim(t030.ldp_dominant_apps_type) = 'others' then  1 else  0 end) as ldp_dominant_apps_type_others_030,
                coalesce(t030.total_searches, {default_integer}) AS total_searches_030,
                coalesce(t030.median_searches, {default_integer}) AS median_searches_030,
                coalesce(t030.average_searches, {default_double}) AS average_searches_030,
                ----------------------------------- Remove Outlieres -------------------------------------------------
                (case when coalesce(t030.total_srp_dwell_time_seconds, {default_double})>{default_dwell_threshhold} THEN {default_dwell_threshhold} ELSE COALESCE(t030.total_srp_dwell_time_seconds, {default_double}) END) AS total_srp_dwell_time_seconds_030,
                (case when coalesce(t030.average_srp_dwell_time_seconds, {default_double})>{default_dwell_threshhold} THEN {default_dwell_threshhold} ELSE COALESCE(t030.average_srp_dwell_time_seconds, {default_double}) END) AS average_srp_dwell_time_seconds_030,
                (case when coalesce(t030.median_srp_dwell_time_seconds, {default_integer})>{default_dwell_threshhold} THEN {default_dwell_threshhold} ELSE COALESCE(t030.median_srp_dwell_time_seconds, {default_integer}) END) AS median_srp_dwell_time_seconds_030,
                (case when coalesce(t030.total_lead_dwell_time_seconds, {default_double})>{default_dwell_threshhold} THEN {default_dwell_threshhold} ELSE COALESCE(t030.total_lead_dwell_time_seconds, {default_double}) END) AS total_lead_dwell_time_seconds_030,
                (case when coalesce(t030.average_lead_dwell_time_seconds, {default_double})>{default_dwell_threshhold} THEN {default_dwell_threshhold} ELSE COALESCE(t030.average_lead_dwell_time_seconds, {default_double}) END) AS average_lead_dwell_time_seconds_030,
                (case when coalesce(t030.median_lead_dwell_time_seconds, {default_integer})>{default_dwell_threshhold} THEN {default_dwell_threshhold} ELSE COALESCE(t030.median_lead_dwell_time_seconds, {default_integer}) END) AS median_lead_dwell_time_seconds_030,
                -------------------------------------------------------------------------------------------------------
                -- IMPORTANT ACTION SIGNALS 030
                coalesce(t030.total_saved_listings, {default_integer}) AS total_saved_listings_030,
                coalesce(t030.total_shared_listings, {default_integer}) AS total_shared_listings_030,
                coalesce(t030.total_saved_searches, {default_integer}) AS total_saved_searches_030,
                coalesce(t030.total_shared_searches, {default_integer}) AS total_shared_searches_030,
                -- COALESCE(t030.srp_dominant_experience_type, '{default_string}') AS srp_dominant_experience_type_030,
                -- One hot encoding
                (case when trim(t030.srp_dominant_experience_type) = 'web' then  1 else  0 end) as srp_dominant_experience_type_web_030,
                (case when trim(t030.srp_dominant_experience_type) = 'mobile web' then  1 else  0 end) as srp_dominant_experience_type_mobile_web_030,
                --COALESCE(t030.srp_dominant_apps_type, '{default_string}') AS srp_dominant_apps_type_030,
                -- One hot encoding
                (case when trim(t030.srp_dominant_apps_type) = 'others' then  1 else  0 end) as srp_dominant_apps_type_others_030,
                coalesce(t030.total_leads_submitted, {default_integer}) AS total_leads_submitted_030,
                coalesce(t030.submitted_leads_for_rent, {default_integer}) AS submitted_leads_for_rent_030,
                coalesce(t030.submitted_leads_for_sale, {default_integer}) AS submitted_leads_for_sale_030,
                coalesce(t030.submitted_leads_not_for_sale, {default_integer}) AS submitted_leads_not_for_sale_030

            FROM {db_name}.consp_member_id_summary_t030 AS t030
            WHERE replace(substr(t030.snapshot_date_mst_yyyymmdd, 1, 10),'-') = '{table_generation_date}'),

    T060 AS (
            select
                -- IMPORTANT KEYS TO JOIN ON
                t060.member_id as member_id_060,
                t060.snapshot_date_mst_yyyymmdd as snapshot_date_mst_yyyymmdd_060,
                -- IMPORTANT BEHAVIOURAL SIGNAL 060
                -- COALESCE(t060.ldp_dominant_segment, '{default_string}') AS ldp_dominant_segment_060,
                -- One hot encoding
                (case when trim(t060.ldp_dominant_segment) = 'for sale' then  1 else 0 end) as ldp_dominant_segment_for_sale_060,
                (case when trim(t060.ldp_dominant_segment) = 'not for sale' then 1 else 0 end) as ldp_dominant_segment_not_for_sale_060,
                (case when trim(t060.ldp_dominant_segment) = 'for rent' then  1 else 0 end) as ldp_dominant_segment_for_rent_060,
                (case when trim(t060.ldp_dominant_segment) = 'no_segment' then  1 else 0 end) as ldp_dominant_segment_no_segment_060,
                -- COALESCE(t060.ldp_dominant_zip, '{default_string}') AS ldp_dominant_zip_060,
                coalesce(t060.total_ldp_page_views, {default_integer}) AS total_ldp_page_views_060,
                coalesce(t060.median_ldp_page_views, {default_integer}) AS median_ldp_page_views_060,
                coalesce(t060.average_ldp_page_views, {default_double}) AS average_ldp_page_views_060,
                --------------------------------------- Remove Outliers----------------------------------------------
                (case when coalesce(t060.average_ldp_dwell_time_seconds, {default_double})>{default_dwell_threshhold} THEN {default_dwell_threshhold} ELSE COALESCE(t060.average_ldp_dwell_time_seconds, {default_double}) END) AS average_ldp_dwell_time_seconds_060,
                (case when coalesce(t060.total_ldp_dwell_time_seconds, {default_double})>{default_dwell_threshhold} THEN {default_dwell_threshhold} ELSE COALESCE(t060.total_ldp_dwell_time_seconds, {default_double}) END) AS total_ldp_dwell_time_seconds_060,
                (case when coalesce(t060.median_ldp_dwell_time_seconds, {default_integer})>{default_dwell_threshhold} THEN {default_dwell_threshhold} ELSE COALESCE(t060.median_ldp_dwell_time_seconds, {default_integer}) END) AS median_ldp_dwell_time_seconds_060,
                -----------------------------------------------------------------------------------------------------
                coalesce(t060.total_distinct_listings_viewed,  {default_integer}) AS total_distinct_listings_viewed_060,
                coalesce(t060.total_listings_viewed, {default_integer}) AS total_listings_viewed_060,
                coalesce(t060.median_distinct_listings_viewed,  {default_integer}) AS median_distinct_listings_viewed_060,
                coalesce(t060.average_distinct_listings_viewed, {default_double}) AS average_distinct_listings_viewed_060,
                coalesce(t060.median_listings_viewed, {default_integer}) AS median_listings_viewed_060,
                coalesce(t060.average_listings_viewed, {default_double}) AS average_listings_viewed_060,
                --COALESCE(t060.ldp_dominant_experience_type, '{default_string}') AS ldp_dominant_experience_type_060,
                -- One hot encoding
                (case when trim(t060.ldp_dominant_experience_type) = 'web' then  1 else  0 end) as ldp_dominant_experience_type_web_060,
                (case when trim(t060.ldp_dominant_experience_type) = 'mobile apps'  then  1 else  0 end) as ldp_dominant_experience_mobile_app_encoded_060,
                (case when trim(t060.ldp_dominant_experience_type) = 'mobile web'  then  1 else  0 end) as ldp_dominant_experience_type_mobile_web_060,
                (case when trim(t060.ldp_dominant_experience_type) = 'others'  then  1 else  0 end) as ldp_dominant_experience_type_others_060,
                --COALESCE(t060.ldp_dominant_apps_type, '{default_string}') AS ldp_dominant_apps_type_060,
                -- One hot encoding
                (case when trim(t060.ldp_dominant_apps_type) = 'ios core apps' then  1 else  0 end) as ldp_dominant_apps_type_ios_core_apps_060,
                (case when trim(t060.ldp_dominant_apps_type) = 'android core apps' then  1 else  0 end) as ldp_dominant_apps_type_android_core_apps_060,
                (case when trim(t060.ldp_dominant_apps_type) = 'ios rentals apps' then  1 else  0 end) as ldp_dominant_apps_type_ios_rentals_apps_060,
                (case when trim(t060.ldp_dominant_apps_type) = 'android rentals apps' then  1 else  0 end) as ldp_dominant_apps_type_android_rentals_apps_060,
                (case when trim(t060.ldp_dominant_apps_type) = 'others' then  1 else  0 end) as ldp_dominant_apps_type_others_060,
                coalesce(t060.total_searches, {default_integer}) AS total_searches_060,
                coalesce(t060.median_searches, {default_integer}) AS median_searches_060,
                coalesce(t060.average_searches, {default_double}) AS average_searches_060,
                ----------------------------------- Remove Outlieres -------------------------------------------------
                (case when coalesce(t060.total_srp_dwell_time_seconds, {default_double})>{default_dwell_threshhold} THEN {default_dwell_threshhold} ELSE COALESCE(t060.total_srp_dwell_time_seconds, {default_double}) END) AS total_srp_dwell_time_seconds_060,
                (case when coalesce(t060.average_srp_dwell_time_seconds, {default_double})>{default_dwell_threshhold} THEN {default_dwell_threshhold} ELSE COALESCE(t060.average_srp_dwell_time_seconds, {default_double}) END) AS average_srp_dwell_time_seconds_060,
                (case when coalesce(t060.median_srp_dwell_time_seconds, {default_integer})>{default_dwell_threshhold} THEN {default_dwell_threshhold} ELSE COALESCE(t060.median_srp_dwell_time_seconds, {default_integer}) END) AS median_srp_dwell_time_seconds_060,
                (case when coalesce(t060.total_lead_dwell_time_seconds, {default_double})>{default_dwell_threshhold} THEN {default_dwell_threshhold} ELSE COALESCE(t060.total_lead_dwell_time_seconds, {default_double}) END) AS total_lead_dwell_time_seconds_060,
                (case when coalesce(t060.average_lead_dwell_time_seconds, {default_double})>{default_dwell_threshhold} THEN {default_dwell_threshhold} ELSE COALESCE(t060.average_lead_dwell_time_seconds, {default_double}) END) AS average_lead_dwell_time_seconds_060,
                (case when coalesce(t060.median_lead_dwell_time_seconds, {default_integer})>{default_dwell_threshhold} THEN {default_dwell_threshhold} ELSE COALESCE(t060.median_lead_dwell_time_seconds, {default_integer}) END) AS median_lead_dwell_time_seconds_060,
                -------------------------------------------------------------------------------------------------------
                -- IMPORTANT ACTION SIGNALS 060
                coalesce(t060.total_saved_listings, {default_integer}) AS total_saved_listings_060,
                coalesce(t060.total_shared_listings, {default_integer}) AS total_shared_listings_060,
                coalesce(t060.total_saved_searches, {default_integer}) AS total_saved_searches_060,
                coalesce(t060.total_shared_searches, {default_integer}) AS total_shared_searches_060,
                -- COALESCE(t060.srp_dominant_experience_type, '{default_string}') AS srp_dominant_experience_type_060,
                -- One hot encoding
                (case when trim(t060.srp_dominant_experience_type) = 'web' then  1 else  0 end) as srp_dominant_experience_type_web_060,
                (case when trim(t060.srp_dominant_experience_type) = 'mobile web' then  1 else  0 end) as srp_dominant_experience_type_mobile_web_060,
                --COALESCE(t060.srp_dominant_apps_type, '{default_string}') AS srp_dominant_apps_type_060,
                -- One hot encoding
                (case when trim(t060.srp_dominant_apps_type) = 'others' then  1 else  0 end) as srp_dominant_apps_type_others_060,
                coalesce(t060.total_leads_submitted, {default_integer}) AS total_leads_submitted_060,
                coalesce(t060.submitted_leads_for_rent, {default_integer}) AS submitted_leads_for_rent_060,
                coalesce(t060.submitted_leads_for_sale, {default_integer}) AS submitted_leads_for_sale_060,
                coalesce(t060.submitted_leads_not_for_sale, {default_integer}) AS submitted_leads_not_for_sale_060

            FROM {db_name}.consp_member_id_summary_t060 AS t060
            WHERE replace(substr(t060.snapshot_date_mst_yyyymmdd, 1, 10),'-') = '{table_generation_date}'),

    T090 AS (
            select
                -- IMPORTANT KEYS TO JOIN ON
                t090.member_id as member_id_090,
                t090.snapshot_date_mst_yyyymmdd as snapshot_date_mst_yyyymmdd_090,
                -- IMPORTANT BEHAVIOURAL SIGNAL 090
                -- COALESCE(t090.ldp_dominant_segment, '{default_string}') AS ldp_dominant_segment_090,
                -- One hot encoding
                (case when trim(t090.ldp_dominant_segment) = 'for sale' then  1 else 0 end) as ldp_dominant_segment_for_sale_090,
                (case when trim(t090.ldp_dominant_segment) = 'not for sale' then 1 else 0 end) as ldp_dominant_segment_not_for_sale_090,
                (case when trim(t090.ldp_dominant_segment) = 'for rent' then  1 else 0 end) as ldp_dominant_segment_for_rent_090,
                (case when trim(t090.ldp_dominant_segment) = 'no_segment' then  1 else 0 end) as ldp_dominant_segment_no_segment_090,
                -- COALESCE(t090.ldp_dominant_zip, '{default_string}') AS ldp_dominant_zip_090,
                coalesce(t090.total_ldp_page_views, {default_integer}) AS total_ldp_page_views_090,
                coalesce(t090.median_ldp_page_views, {default_integer}) AS median_ldp_page_views_090,
                coalesce(t090.average_ldp_page_views, {default_double}) AS average_ldp_page_views_090,
                --------------------------------------- Remove Outliers----------------------------------------------
                (case when coalesce(t090.average_ldp_dwell_time_seconds, {default_double})>{default_dwell_threshhold} THEN {default_dwell_threshhold} ELSE COALESCE(t090.average_ldp_dwell_time_seconds, {default_double}) END) AS average_ldp_dwell_time_seconds_090,
                (case when coalesce(t090.total_ldp_dwell_time_seconds, {default_double})>{default_dwell_threshhold} THEN {default_dwell_threshhold} ELSE COALESCE(t090.total_ldp_dwell_time_seconds, {default_double}) END) AS total_ldp_dwell_time_seconds_090,
                (case when coalesce(t090.median_ldp_dwell_time_seconds, {default_integer})>{default_dwell_threshhold} THEN {default_dwell_threshhold} ELSE COALESCE(t090.median_ldp_dwell_time_seconds, {default_integer}) END) AS median_ldp_dwell_time_seconds_090,
                -----------------------------------------------------------------------------------------------------
                coalesce(t090.total_distinct_listings_viewed,  {default_integer}) AS total_distinct_listings_viewed_090,
                coalesce(t090.total_listings_viewed, {default_integer}) AS total_listings_viewed_090,
                coalesce(t090.median_distinct_listings_viewed,  {default_integer}) AS median_distinct_listings_viewed_090,
                coalesce(t090.average_distinct_listings_viewed, {default_double}) AS average_distinct_listings_viewed_090,
                coalesce(t090.median_listings_viewed, {default_integer}) AS median_listings_viewed_090,
                coalesce(t090.average_listings_viewed, {default_double}) AS average_listings_viewed_090,
                --COALESCE(t090.ldp_dominant_experience_type, '{default_string}') AS ldp_dominant_experience_type_090,
                -- One hot encoding
                (case when trim(t090.ldp_dominant_experience_type) = 'web' then  1 else  0 end) as ldp_dominant_experience_type_web_090,
                (case when trim(t090.ldp_dominant_experience_type) = 'mobile apps'  then  1 else  0 end) as ldp_dominant_experience_mobile_app_encoded_090,
                (case when trim(t090.ldp_dominant_experience_type) = 'mobile web'  then  1 else  0 end) as ldp_dominant_experience_type_mobile_web_090,
                (case when trim(t090.ldp_dominant_experience_type) = 'others'  then  1 else  0 end) as ldp_dominant_experience_type_others_090,
                --COALESCE(t090.ldp_dominant_apps_type, '{default_string}') AS ldp_dominant_apps_type_090,
                -- One hot encoding
                (case when trim(t090.ldp_dominant_apps_type) = 'ios core apps' then  1 else  0 end) as ldp_dominant_apps_type_ios_core_apps_090,
                (case when trim(t090.ldp_dominant_apps_type) = 'android core apps' then  1 else  0 end) as ldp_dominant_apps_type_android_core_apps_090,
                (case when trim(t090.ldp_dominant_apps_type) = 'ios rentals apps' then  1 else  0 end) as ldp_dominant_apps_type_ios_rentals_apps_090,
                (case when trim(t090.ldp_dominant_apps_type) = 'android rentals apps' then  1 else  0 end) as ldp_dominant_apps_type_android_rentals_apps_090,
                (case when trim(t090.ldp_dominant_apps_type) = 'others' then  1 else  0 end) as ldp_dominant_apps_type_others_090,
                coalesce(t090.total_searches, {default_integer}) AS total_searches_090,
                coalesce(t090.median_searches, {default_integer}) AS median_searches_090,
                coalesce(t090.average_searches, {default_double}) AS average_searches_090,
                ----------------------------------- Remove Outlieres -------------------------------------------------
                (case when coalesce(t090.total_srp_dwell_time_seconds, {default_double})>{default_dwell_threshhold} THEN {default_dwell_threshhold} ELSE COALESCE(t090.total_srp_dwell_time_seconds, {default_double}) END) AS total_srp_dwell_time_seconds_090,
                (case when coalesce(t090.average_srp_dwell_time_seconds, {default_double})>{default_dwell_threshhold} THEN {default_dwell_threshhold} ELSE COALESCE(t090.average_srp_dwell_time_seconds, {default_double}) END) AS average_srp_dwell_time_seconds_090,
                (case when coalesce(t090.median_srp_dwell_time_seconds, {default_integer})>{default_dwell_threshhold} THEN {default_dwell_threshhold} ELSE COALESCE(t090.median_srp_dwell_time_seconds, {default_integer}) END) AS median_srp_dwell_time_seconds_090,
                (case when coalesce(t090.total_lead_dwell_time_seconds, {default_double})>{default_dwell_threshhold} THEN {default_dwell_threshhold} ELSE COALESCE(t090.total_lead_dwell_time_seconds, {default_double}) END) AS total_lead_dwell_time_seconds_090,
                (case when coalesce(t090.average_lead_dwell_time_seconds, {default_double})>{default_dwell_threshhold} THEN {default_dwell_threshhold} ELSE COALESCE(t090.average_lead_dwell_time_seconds, {default_double}) END) AS average_lead_dwell_time_seconds_090,
                (case when coalesce(t090.median_lead_dwell_time_seconds, {default_integer})>{default_dwell_threshhold} THEN {default_dwell_threshhold} ELSE COALESCE(t090.median_lead_dwell_time_seconds, {default_integer}) END) AS median_lead_dwell_time_seconds_090,
                -------------------------------------------------------------------------------------------------------
                -- IMPORTANT ACTION SIGNALS 090
                coalesce(t090.total_saved_listings, {default_integer}) AS total_saved_listings_090,
                coalesce(t090.total_shared_listings, {default_integer}) AS total_shared_listings_090,
                coalesce(t090.total_saved_searches, {default_integer}) AS total_saved_searches_090,
                coalesce(t090.total_shared_searches, {default_integer}) AS total_shared_searches_090,
                -- COALESCE(t090.srp_dominant_experience_type, '{default_string}') AS srp_dominant_experience_type_090,
                -- One hot encoding
                (case when trim(t090.srp_dominant_experience_type) = 'web' then  1 else  0 end) as srp_dominant_experience_type_web_090,
                (case when trim(t090.srp_dominant_experience_type) = 'mobile web' then  1 else  0 end) as srp_dominant_experience_type_mobile_web_090,
                --COALESCE(t090.srp_dominant_apps_type, '{default_string}') AS srp_dominant_apps_type_090,
                -- One hot encoding
                (case when trim(t090.srp_dominant_apps_type) = 'others' then  1 else  0 end) as srp_dominant_apps_type_others_090,
                coalesce(t090.total_leads_submitted, {default_integer}) AS total_leads_submitted_090,
                coalesce(t090.submitted_leads_for_rent, {default_integer}) AS submitted_leads_for_rent_090,
                coalesce(t090.submitted_leads_for_sale, {default_integer}) AS submitted_leads_for_sale_090,
                coalesce(t090.submitted_leads_not_for_sale, {default_integer}) AS submitted_leads_not_for_sale_090

            FROM {db_name}.consp_member_id_summary_t090 AS t090
            WHERE replace(substr(t090.snapshot_date_mst_yyyymmdd, 1, 10),'-') = '{table_generation_date}'),


    TRAINING_SET AS (
            select
                coalesce(T001.member_id_001,T007.member_id_007, T030.member_id_030, T060.member_id_060, T090.member_id_090) as member_id,
                coalesce(T001.snapshot_date_mst_yyyymmdd_001, T007.snapshot_date_mst_yyyymmdd_007, T030.snapshot_date_mst_yyyymmdd_030, T060.snapshot_date_mst_yyyymmdd_060, T090.snapshot_date_mst_yyyymmdd_090 ) as snapshot_date_mst_yyyymmdd,
                -- Decode snapshot time
                month(coalesce(Date_parse(snapshot_date_mst_yyyymmdd_001, '%Y%m%d'),
                          date_parse(snapshot_date_mst_yyyymmdd_007, '%Y%m%d'), 
                          date_parse(snapshot_date_mst_yyyymmdd_030, '%Y%m%d'), 
                          date_parse(snapshot_date_mst_yyyymmdd_060, '%Y%m%d'), 
                          date_parse(snapshot_date_mst_yyyymmdd_090, '%Y%m%d'))) as snapshot_month,
                day_of_month(coalesce(Date_parse(snapshot_date_mst_yyyymmdd_001, '%Y%m%d'),
                          date_parse(snapshot_date_mst_yyyymmdd_007, '%Y%m%d'), 
                          date_parse(snapshot_date_mst_yyyymmdd_030, '%Y%m%d'), 
                          date_parse(snapshot_date_mst_yyyymmdd_060, '%Y%m%d'), 
                          date_parse(snapshot_date_mst_yyyymmdd_090, '%Y%m%d'))) as snapshot_day_of_month,
                day_of_week(coalesce(Date_parse(snapshot_date_mst_yyyymmdd_001, '%Y%m%d'),
                          date_parse(snapshot_date_mst_yyyymmdd_007, '%Y%m%d'), 
                          date_parse(snapshot_date_mst_yyyymmdd_030, '%Y%m%d'), 
                          date_parse(snapshot_date_mst_yyyymmdd_060, '%Y%m%d'), 
                          date_parse(snapshot_date_mst_yyyymmdd_090, '%Y%m%d'))) as snapshot_day_of_week,
                T001.*, T007.*, T030.*, T060.*, T090.*
                from T001
                full outer join T007
                on
                    T001.member_id_001 = T007.member_id_007
                full outer join T030
                on
                    T007.member_id_007 = T030.member_id_030
                full outer join T060
                on
                    T030.member_id_030 = T060.member_id_060
                full outer join T090
                on
                    T060.member_id_060 = T090.member_id_090
                    )
    select TRAINING_SET.*
    from TRAINING_SET;