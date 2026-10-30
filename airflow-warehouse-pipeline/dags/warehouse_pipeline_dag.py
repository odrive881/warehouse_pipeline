import os
import sys

# Where the dbt project and loader.py are mounted inside the Airflow containers
PIPELINE_DIR = os.environ.get('WAREHOUSE_PIPELINE_DIR', '/opt/airflow/warehouse_pipeline')
sys.path.append(PIPELINE_DIR)

from loader import ensure_raw_schema, run_loader

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

with DAG(
    dag_id='warehouse_pipeline',
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    setup_schema = PythonOperator(
        task_id='ensure_raw_schema',
        python_callable=ensure_raw_schema,
    )

    load_raw = PythonOperator(
        task_id='load_raw',
        python_callable=run_loader,
    )

    seed = BashOperator(
        task_id='seed',
        bash_command=f"cd {PIPELINE_DIR} && dbt seed --profiles-dir {PIPELINE_DIR}",
    )

    build = BashOperator(
        task_id='build',
        bash_command=f"cd {PIPELINE_DIR} && dbt build --profiles-dir {PIPELINE_DIR}",
    )

    setup_schema >> load_raw >> seed >> build