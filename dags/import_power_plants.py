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
    csv_filepath = os.path.join(os.environ.get('POWER_PLANT_DATA_FILEPATH'), 'global_power_plant_database.csv')
    print(f'Setting up {db_filepath} and importing from {csv_filepath}')
    
    with duckdb.connect(db_filepath) as conn:
        conn.execute('CREATE SCHEMA IF NOT EXISTS raw')
        conn.execute(f'''
        CREATE TABLE IF NOT EXISTS raw.{target_table}
        as SELECT * FROM '{csv_filepath}'
        ''')

        rows_after = conn.sql('''SELECT COUNT(*) AS num_rows from query_table($table)''',
                              params={'table': f'raw.{target_table}'}).pl()['num_rows'][0]

    print(f'There are currently {rows_after} rows in raw.{target_table}')

@dag("import_power_plants", 
     schedule=None,
     catchup=False,
     start_date=datetime(2026, 1, 1),
     params={
         'db_name': Param('aq_raw.db', type='string',
                          pattern=r'^[\w\- ]+\.(db|ddb|duckdb)$',
                          description='DuckDB database filename (allowed filetypes: .db, .ddb, .duckdb)'),
         'target_table': Param('power_plants', type='string',
                               description='SQL table to store imported power plant data'),
     },
     description="Import Global Power Plant Database from local csv.",
     tags=["duckdb", "ingest", "csv"])
def get_power_plants():
    setup_task = ExternalPythonOperator(
        task_id='duckdb_setup',
        python=VENV_PATH,
        python_callable=setup_db,
        op_kwargs={'db_name': '{{ params.db_name }}',
                    'target_table': '{{ params.target_table }}'},
    )

    setup_task

import_dag = get_power_plants()
