import os
from datetime import datetime
from google.cloud import storage , bigquery

#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "premium-odyssey-377711-c22e0a1a55a1.json"
TARGET_PROJECT_ID = 'premium-odyssey-377711'
str_client = storage.Client(project=TARGET_PROJECT_ID)
bq_client = bigquery.Client(project=TARGET_PROJECT_ID)
TARGET_SAMPLE_TABLE = f'{TARGET_PROJECT_ID}.taxi_trips.temp_trips'
TARGET_NORMALIZED_TABLE = f'{TARGET_PROJECT_ID}.taxi_trips.normalized_trips'
SCHEMA = None
FILE_FORMAT = 'parquet'
EXECUTION_DS_KEY = 'ds'
TS_FORMAT = '%Y-%m-%d'
TASK_INSTANCE_KEY = 'ti'
EXECUTION_TS_KEY_2 = 'ts'
EXECUTION_TS_KEY = 'ts_nodash'
TS_NODASH_FORMAT_v1 = '%Y%m%dT%H%M%S'
TS_STRING_FORMAT = '%Y-%m-%d'
BUCKET_NAME = 'tlc_green_trips'
FILE_PREFIX = '000'

def storage_to_bigquery(**kwargs):
    ds = kwargs.get('ds')
    ds = datetime.strptime(ds, '%Y-%m-%d')
    files_folder = ds.strftime('%Y/%m/%d')   # '%Y%m%d')
    is_first = True
    for blob in str_client.list_blobs(bucket_or_name=BUCKET_NAME, prefix=files_folder):
        print(blob)
        if blob.name.startswith(files_folder + "/" + FILE_PREFIX):
            print(blob.name + ' is writing')
            if is_first is True:
                write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
                is_first = False
            else:
                write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            uri = 'gs://' + BUCKET_NAME + '/' + blob.name
            if FILE_FORMAT == "csv":
                if SCHEMA is None:
                    job_config = bigquery.LoadJobConfig(
                        write_disposition=write_disposition,
                        skip_leading_rows=1,
                        source_format=bigquery.SourceFormat.CSV,
                        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
                        autodetect=True
                    )
                else:
                    job_config = bigquery.LoadJobConfig(
                        write_disposition=write_disposition,
                        skip_leading_rows=1,
                        source_format=bigquery.SourceFormat.CSV,
                        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
                        schema=SCHEMA
                    )
            elif FILE_FORMAT == "parquet":
                if SCHEMA is None:
                    job_config = bigquery.LoadJobConfig(
                        write_disposition=write_disposition,
                        source_format=bigquery.SourceFormat.PARQUET,
                        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
                        autodetect=True
                    )
                else:
                    job_config = bigquery.LoadJobConfig(
                        write_disposition=write_disposition,
                        source_format=bigquery.SourceFormat.PARQUET,
                        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
                        schema=SCHEMA
                    )
            else:
                print("File format is not valid!")
            load_job = bq_client.load_table_from_uri(
                uri, TARGET_SAMPLE_TABLE, job_config=job_config
            )
            load_job.result()

def is_exist_table(table_id):
    try:
        bq_client.get_table(table_id)
        return True
    except:
        return False

def normalization_data():
    normalization_query = f"""
    select * from {TARGET_SAMPLE_TABLE} where 
    pickup_datetime = dropoff_datetime or
    passenger_count = 0 or passenger_count is Null or
    trip_distance = 0 or trip_distance is Null or
    fare_amount = 0 
    """
    if is_exist_table(TARGET_SAMPLE_TABLE):
        job_config = bigquery.QueryJobConfig(
            destination=TARGET_NORMALIZED_TABLE,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED
        )
        query_job = bq_client.query(
            normalization_query,
            job_config=job_config,
        )
        query_job.result()
        bq_client.delete_table(TARGET_SAMPLE_TABLE, not_found_ok=True)

def enrichment_data(**kwargs):
    ts_nodash = kwargs.get('ts_nodash')
    ts_nodash = datetime.strptime(ts_nodash,
                                  '%Y%m%dT%H%M%S')
    ts_nodash = ts_nodash.strftime('%Y%m%d')

    TARGET_TABLE = f'premium-odyssey-377711.taxi_trips.trips_{ts_nodash}'
    enrichment_query = f"""
    select CASE WHEN pickup_time >= '06:00:00' AND pickup_time < '12:00:00' THEN 'Morning'
      WHEN pickup_time >= '12:00:00' AND pickup_time < '18:00:00' THEN 'Noon'
      WHEN pickup_time >= '18:00:00' AND pickup_time <= '23:59:59' THEN 'Evening'
      WHEN pickup_time < '06:00:00' THEN 'Night'  end as daypart,
       pup.pickup_datetime, pup.dropoff_datetime, pup.pickup_location_id, pup.dropoff_location_id, h3_pickup, h3_dropoff from (
    select EXTRACT(TIME FROM pickup_datetime) as pickup_time, pickup_datetime, dropoff_datetime, pickup_location_id, dropoff_location_id,  `carto-os`.carto.H3_FROMGEOGPOINT((ST_CENTROID(zone_geom)),9) as h3_dropoff
        from {TARGET_NORMALIZED_TABLE}` trips
        left join (select zone_id, zone_geom, RANK() OVER (PARTITION BY zone_id ORDER BY byte_length(ST_ASTEXT(zone_geom)) DESC) AS finish_rank
        from `premium-odyssey-377711.taxi_trips.taxi_zone_geom`) geo1 on trips.dropoff_location_id = geo1.zone_id
        where finish_rank=1
        ) doff
    inner join (
        select pickup_datetime, dropoff_datetime, pickup_location_id, dropoff_location_id,  `carto-os`.carto.H3_FROMGEOGPOINT((ST_CENTROID(zone_geom)),9) as h3_pickup
         from {TARGET_NORMALIZED_TABLE}` trips
         left join (select zone_id, zone_geom, RANK() OVER (PARTITION BY zone_id ORDER BY byte_length(ST_ASTEXT(zone_geom)) DESC) AS finish_rank
        from `premium-odyssey-377711.taxi_trips.taxi_zone_geom`) geo1 on trips.pickup_location_id = geo1.zone_id
         where finish_rank=1
    ) pup
    on doff.pickup_datetime = pup.pickup_datetime and  doff.dropoff_datetime = pup.dropoff_datetime 
    and  doff.pickup_location_id = pup.pickup_location_id and  doff.dropoff_location_id = pup.dropoff_location_id
    """
    if is_exist_table(TARGET_NORMALIZED_TABLE):
        job_config = bigquery.QueryJobConfig(
            destination=TARGET_TABLE,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED
        )
        query_job = bq_client.query(
            enrichment_query,
            job_config=job_config,
        )
        query_job.result()
        bq_client.delete_table(TARGET_NORMALIZED_TABLE, not_found_ok=True)

def final_result(**kwargs):
    ts_nodash = kwargs.get('ts_nodash')
    ts_nodash = datetime.strptime(ts_nodash,
                                  '%Y%m%dT%H%M%S')
    ts_nodash = datetime.strftime(ts_nodash,
                                  '%Y%m%d')

    SOURCE_TABLE = f'premium-odyssey-377711.taxi_trips.trips_{ts_nodash}'
    TARGET_RESULT_TABLE = f'premium-odyssey-377711.taxi_trips.most_populars_{ts_nodash}'
    final_result_query = f'''
        SELECT popularity,  route, pickup_hexagons, dropoff_hexagons FROM (
         select STRUCT(h3_pickup as pickup_hexagons,h3_dropoff as dropoff_hexagons) AS route , count(*) as route_count,  
         RANK() OVER (ORDER BY  count(*) DESC) AS popularity
         FROM `{SOURCE_TABLE}` 
         group by h3_pickup, h3_dropoff order by 2 desc limit 100
        ) inner join (
        SELECT h3_dropoff as dropoff_hexagons, count(*) as dropoff_count,  RANK() OVER (ORDER BY  count(*) DESC) AS popularity 
        FROM `{SOURCE_TABLE}` group by h3_dropoff order by 2 desc limit 100
        ) using(popularity) inner join (
        SELECT h3_pickup as pickup_hexagons, count(*) as pickup_count, RANK() OVER (ORDER BY  count(*) DESC) AS popularity
         FROM `{SOURCE_TABLE}` group by h3_pickup order by 2 desc limit 100
        ) using(popularity) '''

    if is_exist_table(SOURCE_TABLE):
        job_config = bigquery.QueryJobConfig(
            destination=TARGET_RESULT_TABLE,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED
        )
        query_job = bq_client.query(
            final_result_query,
            job_config=job_config,
        )
        query_job.result()
    else:
        print(SOURCE_TABLE + "does not exist")


# A main.py that uses exactly the same functions as in the dag. It can be used to testing and backfill.
if __name__ == "__main__":
    event_date_nodash = '20230216T074500'
    event_date = '2023-02-16T08:14:46.008685'
    keywords = {'ds': '2023-02-16', 'ts_nodash': '20230216T000000'}
    storage_to_bigquery(**keywords)
    normalization_data()
    enrichment_data(**keywords)
    final_result(**keywords)
