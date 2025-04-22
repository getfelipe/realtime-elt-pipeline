"""
Iterate over the files loaded into S3 on the current date, select those without the 'proceed' prefix, 
concatenate their contents, load them into Postgres, and rename the files with the 'proceed' prefix.
"""

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow import DAG 
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import os
import io
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
BUCKET_NAME = os.getenv("BUCKET_NAME")

default_args = {
    'start_date':None,
}
current_conn = "postgres_default" #"postgres_prod"
# This DAG downloads files from S3, processes them, and loads them into a PostgreSQL database.
with DAG(dag_id='load_to_postgres', default_args=default_args,
         schedule_interval=None, catchup=False) as dag:
    
    reference_date = datetime.today().strftime('%d%m%Y_%H%M%S')[:8] # Set the reference date for file processing

    # Function to download files from S3 and process them, it iterates over the last 15 days
    def download_file_from_s3(**kwargs):
        s3_hook = S3Hook(aws_conn_id='aws_default')
        reference_dates = [datetime.today() - timedelta(days=i) for i in range(0, 15, 1)]
        
        df_list = []
        for reference_date in reference_dates:
            date_str = reference_date.strftime('%d%m%Y_%H%M%S')[:8]
            files = s3_hook.list_keys(bucket_name=BUCKET_NAME, prefix=f'kafka/{date_str}/')
            if not files:
                print("Files are not found: ", date_str)
                continue

            for key in files:
                if 'processed' in str(os.path.basename(key)):
                    continue
                obj = s3_hook.get_key(key=key, bucket_name=BUCKET_NAME)
                bytes_io = io.BytesIO(obj.get()['Body'].read())
                df = pd.read_parquet(bytes_io)
                df = df.drop(columns=['message_key'], errors='ignore')
                if not df.empty:
                    df_list.append(df)
                    new_key = f'kafka/{date_str}/processed_{os.path.basename(key)}'  # Rename the file in S3
                    s3_hook.copy_object(
                        source_bucket_key=key,
                        dest_bucket_key=new_key,
                        source_bucket_name=BUCKET_NAME,
                        dest_bucket_name=BUCKET_NAME
                    )
                    s3_hook.delete_objects(bucket=BUCKET_NAME, keys=[key])

            if not df_list:
                print("The df_list is not valid:", date_str)
                continue

            full_df = pd.concat(df_list, ignore_index=True)
            full_df.to_csv("/tmp/temp.csv", index=False)
            kwargs['ti'].xcom_push(key='columns', value=list(full_df.columns))

    # Function to load data into PostgreSQL
    # It also retrieves the column names from XCom to ensure the correct mapping.
    def load_to_postgres(**kwargs,):
        first_load = kwargs['dag_run'].conf.get('first_load', False)
        if first_load:
            print("First load is True, skipping the load to PostgreSQL.")
            return

        pg_hook = PostgresHook(postgres_conn_id=current_conn)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        columns = kwargs['ti'].xcom_pull(task_ids='download_file_from_s3', key='columns')
        if not columns:
            print("None file found to load in PostgreSQL.")
            return

        with open('/tmp/temp.csv', 'r') as f:
            cursor.copy_expert(
                f"""
                COPY maintenance.production_list ({', '.join(columns)})
                FROM STDIN WITH CSV HEADER DELIMITER ',';
                """,
                f
            )
        conn.commit()
        print("Data loaded successfully into PostgreSQL.")
    
    download_task = PythonOperator(
        task_id='download_file_from_s3',
        python_callable=download_file_from_s3,
    )

    load_to_postgres_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
    )

    trigger_dbt_dag = TriggerDagRunOperator(
            task_id="trigger_dbt_dag",
            trigger_dag_id="dbt_dag",
        )

    download_task >> load_to_postgres_task >> trigger_dbt_dag