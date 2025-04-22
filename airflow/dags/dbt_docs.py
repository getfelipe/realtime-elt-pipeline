"""
Generate and serve dbt documentation locally using BashOperator in Airflow.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator

import sys
sys.path.insert(0, '/usr/local/airflow/')
from include.profiles import project_path, profiles_dir # Import the modules from the path

with DAG(
    dag_id='dbt_docs',
    catchup=False,
    schedule_interval=None,
    start_date=None,
) as dag:
    
    # BashOperator to generate dbt docs
    generate_dbt_docs = BashOperator(
        task_id='generate_dbt_docs',
        bash_command=(
            f"source /usr/local/airflow/dbt_venv/bin/activate && "
            f"dbt docs generate --project-dir {project_path} --profiles-dir {profiles_dir} --static"
        )
    )

    # BashOperator to check the docs on the host
    serve_dbt_docs = BashOperator(
        task_id='serve_dbt_docs',
        bash_command=(
            f"source /usr/local/airflow/dbt_venv/bin/activate && "
            f"dbt docs serve --project-dir {project_path} --profiles-dir {profiles_dir} "
            f"--port 9081 --host 0.0.0.0 --no-browser"
        )
    )

    generate_dbt_docs >> serve_dbt_docs