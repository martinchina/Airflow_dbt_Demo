from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_BIN = "/Users/peng/CursorAI_Projects/Airflow_dbt_Demo/.venv/bin/dbt"
DBT_PROJECT_DIR = "/Users/peng/CursorAI_Projects/Airflow_dbt_Demo/dbt_demo"
DBT_PROFILES_DIR = "/Users/peng/CursorAI_Projects/Airflow_dbt_Demo/dbt_demo/profiles"

default_args = {
    "owner": "airflow",
}

with DAG(
    dag_id="dbt_duckdb_demo",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=f'cd "{DBT_PROJECT_DIR}" && "{DBT_BIN}" debug --profiles-dir "{DBT_PROFILES_DIR}"',
    )
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f'cd "{DBT_PROJECT_DIR}" && "{DBT_BIN}" build --profiles-dir "{DBT_PROFILES_DIR}"',
    )
    dbt_debug >> dbt_build
