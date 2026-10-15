#!/usr/bin/env python
# coding: utf-8
from airflow.decorators import dag
from airflow.providers.standard.operators.python import ExternalPythonOperator
from airflow.sdk import Param

from datetime import datetime
import os

VENV_PATH = os.path.join(os.environ.get('UV_PROJECT_ENVIRONMENT'), 'bin/python')

def setup_db(db_name, target_table):
    import duckdb

    # Setup DB
    db_filepath = os.path.join(os.environ.get('DUCKDB_FOLDER'), db_name)
    print(f'Setting up {db_filepath}')
    with duckdb.connect(db_filepath) as conn:
        conn.execute(f'''
        CREATE TABLE IF NOT EXISTS {target_table} (
            location_id BIGINT,
            sensors_id BIGINT,
            location VARCHAR,
            datetime VARCHAR,
            lat DOUBLE PRECISION,
            lon DOUBLE PRECISION,
            parameter VARCHAR,
            units VARCHAR,
            value DOUBLE PRECISION,
            created_at TIMESTAMP,
            filename VARCHAR
        )
        ''')

def fetch_missing_files(db_name, target_table, location, year):
    import duckdb

    db_filepath = os.path.join(os.environ.get('DUCKDB_FOLDER'), db_name)
    with duckdb.connect(db_filepath) as conn:
        _ = conn.execute("""
        LOAD httpfs;
        SET s3_region='us-east-1';
        """)

        # Get list of files not in local storage
        files_missing = conn.sql('''
        SELECT file
        FROM glob('s3://openaq-data-archive/records/csv.gz/locationid=$location/year=$year/*/*.csv.gz')
        WHERE file NOT IN (SELECT DISTINCT filename FROM query_table($table))
        ''', params={'table': target_table, 'location': location, 'year': year}).pl()['file'].to_list()

    return files_missing

def ingest_s3(db_name, target_table, location, year, files_missing):
    import duckdb
    
    if not files_missing:
        print(f'Location {location} - {year}: No missing files were found, no data was obtained.')
    else:
        try:
            db_filepath = os.path.join(os.environ.get('DUCKDB_FOLDER'), db_name)
            with duckdb.connect(db_filepath) as conn:
                _ = conn.execute("""
                LOAD httpfs;
                SET s3_region='us-east-1';
                """)

                rows_before = conn.sql('''SELECT COUNT(*) AS num_rows from query_table($table)''',
                                       params={'table': target_table}).pl()['num_rows'][0]
    
                # Read S3 into DuckDB
                conn.sql(f'''
                INSERT INTO {target_table}
                SELECT
                    location_id, sensors_id, location, datetime, lat, lon, parameter, units, value, NOW() AS created_at, filename
                FROM read_csv(
                    $files,
                    filename = true,
                    hive_partitioning = false,
                    header = true,
                    store_rejects = true,
                    delim = ',',
                    columns = {{'location_id': 'BIGINT', 'sensors_id': 'BIGINT', 'location': 'VARCHAR', 
                                'datetime': 'VARCHAR', 'lat': 'DOUBLE', 'lon': 'DOUBLE', 'parameter': 'VARCHAR',
                                'units': 'VARCHAR', 'value': 'DOUBLE'}}
                );
                ''', params={'files': files_missing})
    
                rows_after = conn.sql('''SELECT COUNT(*) AS num_rows from query_table($table)''',
                                        params={'table': target_table}).pl()['num_rows'][0]
    
                added_rows = rows_after - rows_before
                print(f'Location {location} - {year}: Added {added_rows} rows')
        except FileNotFoundError as e:
            print(f'Location {location} - {year}: File not found!', e)

@dag("ingest_openaq_s3", 
     schedule=None,
     catchup=False,
     start_date=datetime(2026, 1, 1),
     params={
         'db_name': Param('aq_raw.db', type='string',
                          pattern=r'^[\w\- ]+\.(db|ddb|duckdb)$',
                          description='DuckDB database filename (allowed filetypes: .db, .ddb, .duckdb)'),
         'target_table': Param('measurements', type='string',
                               description='SQL table to store ingested data'),
         'location': Param(3180, type='integer', minimum=1,
                           description='OpenAQ Location ID'),
         'year': Param(2020, type='integer', minimum=2015,
                       description='Year of data to obtain location readings from')
     },
     render_template_as_native_obj=True,
     description="OpenAQ historical S3 data ingestion for a given location and year.",
     tags=["duckdb", "ingest", "s3"])
def process_year_data():
    setup_task = ExternalPythonOperator(
        task_id='duckdb_setup',
        python=VENV_PATH,
        python_callable=setup_db,
        op_kwargs={'db_name': '{{ params.db_name }}',
                   'target_table': '{{ params.target_table }}'},
    )

    calculate_missing_task = ExternalPythonOperator(
        task_id='calculate_missing_files',
        python=VENV_PATH,
        python_callable=fetch_missing_files,
        op_kwargs={'db_name': '{{ params.db_name }}',
                   'target_table': '{{ params.target_table }}',
                   'location': '{{ params.location }}',
                   'year': '{{ params.year }}'}
    )

    ingest_task = ExternalPythonOperator(
        task_id='ingest_missing_files',
        python=VENV_PATH,
        python_callable=ingest_s3,
        op_kwargs={'db_name': '{{ params.db_name }}',
                   'target_table': '{{ params.target_table }}',
                   'location': '{{ params.location }}',
                   'year': '{{ params.year }}',
                   'files_missing': calculate_missing_task.output},
    )

    setup_task >> calculate_missing_task >> ingest_task

process_dag = process_year_data()
