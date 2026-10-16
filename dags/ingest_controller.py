from airflow.decorators import dag
from airflow.sdk import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime
import itertools
import os

locations = [int(el) for el in os.environ.get('LOCATIONS').split(',')]
years = [int(el) for el in os.environ.get('YEARS').split(',')]

combinations = list(itertools.product(locations, years))

@dag("ingest_controller", 
     schedule=None,
     catchup=False,
     start_date=datetime(2026, 1, 1),
      params={
              'db_name': Param('aq_raw.db', type='string',
                               pattern=r'^[\w\- ]+\.(db|ddb|duckdb)$',
                               description='DuckDB database filename (allowed filetypes: .db, .ddb, .duckdb)'),
              'target_table': Param('measurements', type='string',
                                    description='SQL table to store ingested data')
     },
     description="OpenAQ historical S3 data ingestion for a given location and year.",
     tags=["duckdb", "controller"])
def trigger_dags():
    previous_task = None
    for (location, year) in combinations:
        trigger = TriggerDagRunOperator(
            task_id=f'openaq_ingest_{location}_{year}',
            trigger_dag_id='ingest_openaq_s3',
            conf={'db_name': '{{ params.db_name }}',
                  'target_table': '{{ params.target_table }}',
                  'location': location,
                  'year': year},
            wait_for_completion=True,
            poke_interval=20
        )

        if previous_task:
            previous_task >> trigger
        previous_task = trigger

controller_dag = trigger_dags()
