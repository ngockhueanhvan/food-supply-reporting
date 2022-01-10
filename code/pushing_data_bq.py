# 1. Setting up
from google.cloud import bigquery
import os
import pandas as pd
from country_data_cleaning import df
from country_data_cleaning import country
from country_data_cleaning import food

absolutepath = os.path.abspath(__file__)
fileDirectory = os.path.dirname(absolutepath)
parentDirectory = os.path.dirname(fileDirectory)

maps_dir = os.path.join(parentDirectory, 'data_in/tableau/maps.csv') # read map longs and lats file
model_dir = os.path.join(parentDirectory, 'data_in/tableau/model.csv') # read sigmoid map curves file

# 2. Downloading the data

filename = os.path.join(
    fileDirectory, 'may-eleventh-xxxxxxxxxxxxx.json')  # BQ access key
bq_client = bigquery.Client.from_service_account_json(filename)
table_config = bigquery.job.LoadJobConfig(write_disposition='WRITE_TRUNCATE')
table_config.autodetect = True


# 3. Pushing the data back to BQ

# conbined table
df_job = bq_client.load_table_from_dataframe(
    df, 'may-eleventh.food_supply_reporting.food_supply_reporting', job_config=table_config)
df_job.result()  # Waits for table load to complete.

# clean country table
country_job = bq_client.load_table_from_dataframe(
    country, 'may-eleventh.food_supply_reporting.country_table', job_config=table_config
)
country_job.result()  # Waits for table load to complete.

# clean food table
food_job = bq_client.load_table_from_dataframe(
    food, 'may-eleventh.food_supply_reporting.food_daily_consumption', job_config=table_config
)
food_job.result()  # Waits for table load to complete

# maps long and lats
maps = pd.read_csv(maps_dir)
maps_job = bq_client.load_table_from_dataframe(
    maps, 'may-eleventh.food_supply_reporting.maps_long_lat', job_config=table_config
)
maps_job.result()  # Waits for table load to complete

# sigmoid point model long and lats
model = pd.read_csv(model_dir)
model_job = bq_client.load_table_from_dataframe(
    model, 'may-eleventh.food_supply_reporting.sigmoid_point_model', job_config=table_config
)
model_job.result()  # Waits for table load to complete
