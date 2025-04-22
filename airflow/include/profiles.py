"""
It includes the necessary imports and configurations for the dbt project using the Cosmos library.
"""

from cosmos import ProfileConfig, ExecutionConfig, ProjectConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
import datetime
from pathlib import Path
from cosmos.config import RenderConfig
from cosmos.constants import TestBehavior



project_path = Path('/usr/local/airflow/dags/dbt_2').as_posix()
profiles_dir = "/usr/local/airflow/.dbt"
profiles_yml_filepath = Path(f"{profiles_dir}/profiles.yml").as_posix()
dbt_executable_path = str(Path('/usr/local/airflow/dbt_venv/bin/dbt'))
path_manifest_local = "/usr/local/airflow/dags/dbt_2/target/"
project_config = ProjectConfig(project_path)


# The variable “dbt_executable_path” is the path to where the dbt executable is located.
venv_execution_config = ExecutionConfig(
    dbt_executable_path=dbt_executable_path,
)

# “profile_name” is the project name,  “target_name” is the target desired to run the dbt model, 
# and “profiles_yml_filepath” is where the profiles.yml path is in the Docker container.
profile_config = ProfileConfig(
    profile_name='dbt_2',
    target_name='dev',
    profiles_yml_filepath=profiles_yml_filepath,
)

render_config = RenderConfig(
        select=["path:models/"], # Run only models in models/
        exclude=["path:seeds/"], # Avoid models in seeds/
        test_behavior=TestBehavior.AFTER_ALL, # Run tests after all models
        )