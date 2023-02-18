from airflow import macros
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
import datetime

from taxi_trips_etl_packages.queries.enrichment_query import enrichment_query
from taxi_trips_etl_packages.queries.normalization_query import normalization_query
from taxi_trips_etl_packages.utils.constants import *

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': True,
    'start_date': datetime.datetime(2023, 2, 16),
    'email': ['efesabanoglu@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=3),
    'pool': 'default_pool',
    'execution_timeout': datetime.timedelta(hours=3),
    'provide_context': True
}

# Transferring data in storage to bigquery.
def storage_to_bigquery(**kwargs):

    # The path to go to the directory where the data is in the storage.
    ds = kwargs.get('ds')
    files_folder = macros.ds_format(ds, '%Y-%m-%d', '%Y/%m/%d')

    # if file is first one, firstly creating table is done then appending data.
    is_first = True
    for blob in str_client.list_blobs(bucket_or_name=BUCKET_NAME, prefix=files_folder):
        is_there_a_data = False
        if blob.name.startswith(files_folder + "/" + FILE_PREFIX):
            if is_first is True:
                write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
                is_first = False
            else:
                write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            uri = 'gs://' + BUCKET_NAME + '/' + blob.name
            # for different file formats, this functions are generics and they can be used in private client libraries.
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
            print(blob.name + ' has written')
            is_there_a_data = True

    # this step is short circuit operator so if it returns true, next step is continue, otherwise dag stops.
    return is_there_a_data

# helper function.
def is_exist_table(table_id):
    try:
        bq_client.get_table(table_id)
        return True
    except:
        return False

# Clearing any erroneous data,
def normalization_data():

    print(normalization_query)
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

        # when normalizing job is done, unnecessary first table is deleted.
        bq_client.delete_table(TARGET_SAMPLE_TABLE, not_found_ok=True)
    else:
        print(TARGET_SAMPLE_TABLE + "does not exist")

# step where hexagons and daypart are calculated using udf
def enrichment_data(**kwargs):
    ts_nodash = kwargs.get('ts_nodash')
    ts_nodash = datetime.datetime.strptime(ts_nodash,
                                  '%Y%m%dT%H%M%S')
    ts_nodash = datetime.datetime.strftime(ts_nodash,
                                  '%Y%m%d')
    # TARGET TABLE name is like premium-odyssey-377711.taxi_trips.trips_20140204
    TARGET_TABLE = f'premium-odyssey-377711.taxi_trips.trips_{ts_nodash}'

    print(enrichment_query)

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

         # when normalizing job is done, unnecessary first table is deleted.
        bq_client.delete_table(TARGET_NORMALIZED_TABLE, not_found_ok=True)
    else:
        print(TARGET_NORMALIZED_TABLE + "does not exist")

# step that prepares the stated results: What are the most popular pickup hexagons
#                                        What are the most popular dropoff hexagons
#                                        What are the most popular routes

def final_result(**kwargs):
    ts_nodash = kwargs.get('ts_nodash')
    ts_nodash = datetime.datetime.strptime(ts_nodash,
                                  '%Y%m%dT%H%M%S')
    ts_nodash = datetime.datetime.strftime(ts_nodash,
                                  '%Y%m%d')
                                         # when normalizing job is done, unnecessary first table is deleted.

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

with DAG(
        dag_id='taxi_trips_etl_v1.2',
        catchup=False,
        schedule_interval="00 05 * * *",
        default_args=default_args,
        is_paused_upon_creation=True) as dag:

    storage_to_bq_task = ShortCircuitOperator(
        task_id='storage_to_bq',
        retries=1,
        python_callable=storage_to_bigquery,
        execution_timeout=datetime.timedelta(minutes=120),
        # on_failure_callback=slack_fail_alert,
        # on_success_callback=slack_success_alert,
        provide_context=True
    )

    normalization_data_task = PythonOperator(
        task_id='normalization_data',
        retries=1,
        python_callable=normalization_data,
        execution_timeout=datetime.timedelta(minutes=120),
        # on_failure_callback=slack_fail_alert,
        # on_success_callback=slack_success_alert,
        provide_context=True
    )

    enrichment_data_task = PythonOperator(
        task_id='enrichment_data',
        retries=1,
        python_callable=enrichment_data,
        execution_timeout=datetime.timedelta(minutes=120),
        # on_failure_callback=slack_fail_alert,
        # on_success_callback=slack_success_alert,
    )

    final_result_task = PythonOperator(
        task_id='final_result',
        retries=1,
        python_callable=final_result,
        execution_timeout=datetime.timedelta(minutes=120),
        # on_failure_callback=slack_fail_alert,
        # on_success_callback=slack_success_alert,
    )


storage_to_bq_task >> normalization_data_task >> enrichment_data_task >> final_result_task

if __name__ == "__main__":
    event_date_nodash = '20230216T074500'
    event_date = '2023-02-16T08:14:46.008685'
    #slack_success_alert("")
    print(f'One turn finished for {event_date}')