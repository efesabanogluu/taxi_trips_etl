import os
from google.cloud import storage , bigquery

TARGET_PROJECT_ID = os.environ.get('PROJECT_ID', 'premium-odyssey-377711')
str_client = storage.Client(project=TARGET_PROJECT_ID)
bq_client = bigquery.Client(project=TARGET_PROJECT_ID)

# Temp table that includes row storage data.
TARGET_SAMPLE_TABLE = f'premium-odyssey-377711.taxi_trips.temp_trips'

# Clearing erroneous data.
TARGET_NORMALIZED_TABLE = f'premium-odyssey-377711.taxi_trips.normalized_trips'
SCHEMA = None

# Data file format in the storage.
FILE_FORMAT = 'parquet'

# Airflow References for configs like table names and time formats.
EXECUTION_DS_KEY = 'ds'
TS_FORMAT = '%Y-%m-%d'
TASK_INSTANCE_KEY = 'ti'
EXECUTION_TS_KEY_2 = 'ts'
EXECUTION_TS_KEY = 'ts_nodash'
TS_NODASH_FORMAT_v1 = '%Y%m%dT%H%M%S'
TS_STRING_FORMAT = '%Y-%m-%d'

# Configs to take accurate data from storage.
BUCKET_NAME = 'tlc_green_trips'
FILE_PREFIX = '000'