# Summary of Consumer Analytical Table (Only with LDP) :


|Segment/Time Window| for sale | for rent | not for sale| no segment |
|-------------------|----------|----------|-------------|------------|
| 3 Months, ~38 Million Records | n_records=, n_cluster= 5, uncorrelated_vars = 8|n_cluster= 5, uncorrelated_vars = 8 |n_cluster= 5, uncorrelated_vars = 8 |n_cluster= 5, uncorrelated_vars = 8 |
| 1 Month, ~20 Million Records  | n_cluster= 5, uncorrelated_vars = 8|n_cluster= 5, uncorrelated_vars = 8 |n_cluster= 5, uncorrelated_vars = 8 |n_cluster= 5, uncorrelated_vars = 8 |
| 1 Week, ~13 Million Records   | n_cluster= 6, uncorrelated_vars = 8|n_cluster= 6, uncorrelated_vars = 8 |n_cluster= 6, uncorrelated_vars = 8 |n_cluster= 6, uncorrelated_vars = 8 |
| 1 Day, ~7 Million Records    | n_cluster= 7, uncorrelated_vars = 8|n_cluster= 7, uncorrelated_vars = 8 |n_cluster= 7, uncorrelated_vars = 8 |n_cluster= 7, uncorrelated_vars = 8 |


**Important Variables:**

total_ldp_page_views : 
  * median_ldp_page_views, 
  * average_ldp_page_views, 

total_ldp_dwell_time_seconds : 
   * median_ldp_dwell_time_seconds, 
   * average_ldp_dwell_time_seconds, 
   * minimum_ldp_dwell_time_seconds, 
   * maximum_ldp_dwell_time_seconds
   
**Note : total_ldp_page_views and total_ldp_dwell_time_seconds shows about 0.8 correlation**

**Data in timeframe 90, 30, 7 and 1 are orthogonal**

**Recommended Model call for stages should be >> 1 days less than 30 days.**