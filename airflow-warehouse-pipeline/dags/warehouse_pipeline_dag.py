import sys
sys.path.append('/opt/airflow/warehouse_pipeline')

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
        bash_command=("cd /opt/airflow/warehouse_pipeline && "
                      "dbt seed --profiles-dir /opt/airflow/warehouse_pipeline"
        ),
    )

    build = BashOperator(
        task_id='build',
        bash_command=("cd /opt/airflow/warehouse_pipeline && "
                      "dbt build --profiles-dir /opt/airflow/warehouse_pipeline"
        ),
    )

    setup_schema >> load_raw >> seed >> build