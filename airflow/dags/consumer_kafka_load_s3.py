"""
Get messages from the Kafka topic and load them into S3
"""

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator

from pathlib import Path
from pendulum import datetime

import os
import json
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
BUCKET_NAME = os.getenv("BUCKET_NAME")
KAFKA_TOPIC = "my_topic"

default_args = {
    'start_date': None, 
}
dir_data = Path(f"/usr/local/airflow/datasets/")

with DAG(dag_id='consumer_kafka_load_s3', default_args=default_args, 
         schedule_interval=None, catchup=False,) as dag:
    
    # Function to consume messages from Kafka and call the upload function
    def consume_function(messages):
        if messages is not None and messages.value() is not None:
            message_key = json.loads(messages.key().decode("utf-8"))
            messages_value = json.loads(messages.value().decode("utf-8"))

            flattened_data = []
            for record in messages_value:
                record["message_key"] = message_key
                flattened_data.append(record)

            df = pd.DataFrame(flattened_data)
            upload_file_to_s3(message_key, df)
        else:
            print("No messages received or message value is None.")

    # Function to upload the .parquet to S3
    def upload_file_to_s3(message_key, df):
        
        file_path = f"{dir_data}{message_key}.csv"  # Save DataFrame to CSV
        dir_data.mkdir(parents=True, exist_ok=True)
        df.to_parquet(file_path, index=False)

        s3_hook = S3Hook(aws_conn_id='aws_default')  # Upload to S3
        s3_hook.load_file(file_path, key=f"kafka/{message_key[:8]}/{message_key}.parquet", bucket_name=BUCKET_NAME, replace=True)

    start_empty_task = DummyOperator(task_id='start')
    end_empty_task = DummyOperator(task_id='end')

    # Kafka consumer operator
    consume_treats = ConsumeFromTopicOperator(
        task_id="consume_treats",
        kafka_config_id="kafka_default",
        topics=[KAFKA_TOPIC],
        apply_function=consume_function,
        poll_timeout=25,
        max_messages=400,
        max_batch_size=4,
    )

    # Trigger the dependent DAG
    trigger_schema_with_seeds = TriggerDagRunOperator(
            task_id="trigger_schema_with_seeds",
            trigger_dag_id="schema_with_seeds",
        )

    start_empty_task >> consume_treats >> trigger_schema_with_seeds >> end_empty_task