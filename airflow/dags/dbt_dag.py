"""
Create the following models to perform the transformations:

staging: Contains only raw data, without any transformations.
intermediate: Removes duplicate data and validates column integrity.
mart: Business metrics for use in Metabase.
"""

import sys
sys.path.insert(0, '/usr/local/airflow/')

from include.profiles import profile_config, venv_execution_config, render_config, project_config
from cosmos import DbtDag
from datetime import datetime

# The DAG is created using the DbtDag class from the cosmos module.
simple_dag = DbtDag(
    project_config=project_config,
    profile_config=profile_config,
    execution_config=venv_execution_config,
    catchup=False,
    dag_id='dbt_dag',
    render_config=render_config,
)