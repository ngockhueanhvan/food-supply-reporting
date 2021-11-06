# 1. Setting up
from google.cloud import bigquery
import data_cleaning

df = data_cleaning.df

# 2. Downloading the data
filename =  './code/may-eleventh-e207a3fd73f9.json' #BQ access key
bq_client = bigquery.Client.from_service_account_json(filename) 
table_config = bigquery.job.LoadJobConfig(write_disposition='WRITE_TRUNCATE')
table_config.autodetect = True


# 3. Pushing the data back to BQ
job = bq_client.load_table_from_dataframe(
        df,'may-eleventh.food_supply_reporting.fao_food_supply',job_config=table_config)
job.result()  # Waits for table load to complete.