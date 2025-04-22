"""
Kafka Sensor DAG
This DAG uses a Kafka sensor to wait for messages on a specified Kafka topic.
When a message is received, it triggers another DAG to process the data.
It also retriggers itself to continue listening for new messages.
"""

from airflow import DAG
from airflow.sensors.base import BaseSensorOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pendulum import datetime
from kafka import KafkaConsumer

class KafkaSensor(BaseSensorOperator):
    def __init__(self, topic, bootstrap_servers, group_id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id

    def poke(self, context):
        try:
            consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                auto_offset_reset='latest',
                enable_auto_commit=False
            )
            message = next(consumer, None)
            consumer.close()
            return message is not None
        except Exception as e:
            self.log.error(f"KafkaSensor error: {e}")
            return False

default_args = {
    "start_date": None,
}

with DAG(
    dag_id="kafka_sensor_trigger_consumer",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Sensor-based DAG that triggers consumer DAG when Kafka receives data",
) as dag:

    wait_for_kafka = KafkaSensor(
        task_id="wait_for_kafka_message",
        topic="my_topic",
        bootstrap_servers="kafka:9092",
        group_id="group_1",
        poke_interval=30,  # Check every 15 seconds
        timeout=300,       # Wait up to five minutes
        #soft_fail=False,    # Fail the DAG if the sensor times out
    )

    trigger_consumer_kafka_load_s3 = TriggerDagRunOperator(
        task_id="trigger_consumer_kafka_load_s3",
        trigger_dag_id="consumer_kafka_load_s3",  # DaG to trigger when Kafka message is received
    )

    retrigger_self = TriggerDagRunOperator(
        task_id="retrigger_self",
        trigger_dag_id="kafka_sensor_trigger_consumer", # Retrigger this DAG
    )


    wait_for_kafka >> trigger_consumer_kafka_load_s3 >> retrigger_self
