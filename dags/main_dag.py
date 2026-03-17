import sys
sys.path.append('/opt/airflow')

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from scripts.music_logic import (scout_instruments, 
                                 save_instruments, 
                                 harvest_recordings, 
                                 move_to_clickhouse
)

default_args = {
    'owner': 'jaxonlarsen',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 23),
}

def save_instruments_from_xcom(**context):
    """Retrieves instrument_map from previous task via XCom."""
    instrument_map = context['ti'].xcom_pull(task_ids='scout_instruments')
    save_instruments(instrument_map)

with DAG(
    'musicbrainz_global_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id='scout_instruments',
        python_callable=scout_instruments
    )

    t2 = PythonOperator(
        task_id='save_instruments',
        python_callable=save_instruments_from_xcom
    )

    t3 = PythonOperator(
        task_id='harvest_recordings',
        python_callable=harvest_recordings
    )

    t4 = PythonOperator(
        task_id='move_to_clickhouse',
        python_callable=move_to_clickhouse
    )

    t1 >> t2 >> t3 >> t4
