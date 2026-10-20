from airflow.decorators import dag
from airflow.hooks.base import BaseHook
from airflow.providers.standard.operators.python import ExternalPythonOperator
from airflow.sdk import Param

from datetime import datetime
import os

VENV_PATH = os.path.join(os.environ.get('UV_PROJECT_ENVIRONMENT'), 'bin/python')
LOCATIONS = [int(el) for el in os.environ.get('LOCATIONS').split(',')]

def setup_db(db_name, target_table):
    import duckdb

    # Setup DB
    db_filepath = os.path.join(os.environ.get('DUCKDB_FOLDER'), db_name)
    print(f'Setting up {db_filepath}')
    with duckdb.connect(db_filepath) as conn:
        conn.execute('CREATE SCHEMA IF NOT EXISTS raw')
        conn.execute(f'''
        CREATE TABLE IF NOT EXISTS raw.{target_table} (
            id BIGINT PRIMARY KEY,
            name VARCHAR,
            sensor_units VARCHAR[],
            lat DOUBLE PRECISION,
            lon DOUBLE PRECISION,
            licenses VARCHAR[],
            dt_start TIMESTAMP,
            dt_end TIMESTAMP,
            created_at TIMESTAMP
        )
        ''')

def get_missing_locations(db_name, target_table, locations):
    import duckdb
    
    db_filepath = os.path.join(os.environ.get('DUCKDB_FOLDER'), db_name)
    with duckdb.connect(db_filepath) as conn:
        locations_missing = conn.sql(f'''
        SELECT array_agg(loc)
        FROM (
            SELECT unnest($list) as loc
        )
        WHERE loc NOT IN (SELECT DISTINCT id FROM raw.{target_table})
        ''', params={'list': locations}).fetchall()[0][0]

    return locations_missing

def ingest_api(db_name, target_table, locations_missing, connection_uri, delay_s):
    import duckdb
    import requests
    import time
    import warnings
    from urllib.parse import unquote, urlparse

    parsed = urlparse(connection_uri)
    api_key = unquote(parsed.password)
    
    if not locations_missing:
        print(f'No missing locations were found, no data was obtained.')
    else:
        open_aq_api_url = 'https://api.openaq.org/v3/locations'
        headers = {'X-API-Key': api_key}

        db_filepath = os.path.join(os.environ.get('DUCKDB_FOLDER'), db_name)
        with duckdb.connect(db_filepath) as conn:
            rows_before = conn.sql('''SELECT COUNT(*) AS num_rows from query_table($table)''',
                                   params={'table': f'raw.{target_table}'}).fetchall()[0][0]
                
            entries_skipped = []
            try:
                for loc in locations_missing:
                    res = requests.get(f'{open_aq_api_url}/{loc}', headers=headers)
                    entry = res.json()['results'][0]
                    time.sleep(delay_s)
                    try:
                        new_row = {
                            'id': entry['id'],
                            'name': entry['name'],
                            'sensor_units': [s['name'] for s in entry['sensors']],
                            'lat': entry['coordinates']['latitude'],
                            'lon': entry['coordinates']['longitude'],
                            'licenses': [(l['name'], l['attribution']['name']) for l in entry['licenses']],
                            'dt_start': entry['datetimeFirst']['utc'],
                            'dt_end': entry['datetimeLast']['utc'],
                        }
            
                        conn.sql(f'''
                        INSERT OR IGNORE INTO raw.{target_table} (id, name, sensor_units, lat, lon, licenses, dt_start, dt_end, created_at)
                        VALUES ($id, $name, $sensor_units, $lat, $lon, $licenses, $dt_start, $dt_end, NOW());
                        ''', params=new_row)
                    except TypeError:
                        entries_skipped.append(entry['id'])
                if len(entries_skipped) > 0:
                    warnings.warn(f'{len(entries_skipped)} stations were skipped due to parsing errors: {entries_skipped}')
            except requests.exceptions.HTTPError:
                print(f'API call returned {res.status_code}: {res.text}')

            rows_after = conn.sql('''SELECT COUNT(*) AS num_rows from query_table($table)''',
                                  params={'table': f'raw.{target_table}'}).fetchall()[0][0]
            added_rows = rows_after - rows_before
            print(f'Location info: Added {added_rows} rows')

@dag("openaq_location_info",
    schedule=None,
    catchup=False,
    start_date=datetime(2026, 1, 1),
    params={
        'db_name': Param('aq_raw.db', type='string',
                        pattern=r'^[\w\- ]+\.(db|ddb|duckdb)$',
                        description='DuckDB database filename (allowed filetypes: .db, .ddb, .duckdb)'),
        'target_table': Param('locations', type='string',
                              description='SQL table to store ingested data'),
        'request_delay': Param(0.25, type='number',
                               description='Waiting time (seconds) between API calls')
    },
    render_template_as_native_obj=True,
    description="OpenAQ location data ingestion for all specified locations.",
    tags=["duckdb", "ingest", "openaq", "api"])
def process_location_data():
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
        python_callable=get_missing_locations,
        op_kwargs={'db_name': '{{ params.db_name }}',
                   'target_table': '{{ params.target_table }}',
                   'locations': LOCATIONS}
    )

    ingest_task = ExternalPythonOperator(
        task_id='ingest_missing_files',
        python=VENV_PATH,
        python_callable=ingest_api,
        op_kwargs={'db_name': "{{ params.db_name }}",
                    'target_table': "{{ params.target_table }}",
                    'locations_missing': calculate_missing_task.output,
                    'connection_uri': " {{ conn.open_aq_connection.get_uri() }} ",
                    'delay_s': "{{ params.request_delay }}"},
    )


    setup_task >> calculate_missing_task >> ingest_task

locations_dag = process_location_data()