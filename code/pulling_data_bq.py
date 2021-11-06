# 1. Setting up
from google.cloud import bigquery
import data_cleaning


df = data_cleaning.df
output_dir = globals.OUTPUT_DIR
df = globals.DF

# 2. Downloading the data
filename =  './code/may-eleventh-e207a3fd73f9.json' #BQ access key
bq_client = bigquery.Client.from_service_account_json(filename) 
table_config = bigquery.job.LoadJobConfig(write_disposition='WRITE_APPEND')
table_config.autodetect = True


# df = """
# DECLARE DATE_TO STRING DEFAULT '2021-05-02'; 
# DECLARE DATE_FROM STRING DEFAULT  '2020-01-01';

# SELECT 
#     EXTRACT(year FROM date) as Year,
#     EXTRACT(isoweek FROM date) as Week,
#     Channel,
#     CASE WHEN division = "GDC" THEN 'IM' ELSE division END AS Division,
#     Product,
#     Campaign_Type,
#     SUM(cost) as Cost
# FROM seaocdm-data-au.im_media_reporting.all__full_combined_reporting_modified_partitioned 
# WHERE division IN ('IM', 'GDC') and Cost>0 and date<=CAST(DATE_TO as Date) and date>=CAST(DATE_FROM as Date) AND Product LIKE 'GALAXY%'
# GROUP BY
#     EXTRACT(year FROM date),
#     EXTRACT(isoweek FROM date),
#     Channel,
#     Division,
#     Product,
#     Campaign_Type
#     """
# digital_spend = bq_client.query(df).to_dataframe()

# 3. Pushing the data back to BQ
job = bq_client.load_table_from_dataframe(
        df,'may-eleventh.anhsandbox.test',job_config=table_config)
job.result()  # Waits for table load to complete.