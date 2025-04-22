from kafka import KafkaProducer
from faker import Faker
import time
import json
import random 
import uuid
from datetime import date, datetime, timedelta
import pandas as pd
import psycopg2
import psycopg2.extras
import os

# Set the locale to Portuguese (Brazil)
fake = Faker('pt_BR')

# This function is used to serialize the data before sending it to Kafka
def default_serializer(obj):
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    raise TypeError(f"Type {obj.__class__.__name__} not serializable")

# Kafka producer configuration
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    api_version=(3, 8, 0),
    value_serializer=lambda v: json.dumps(v, default=default_serializer).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Directory path
dir_seed = "/opt/airflow/dags/dbt_2/seeds/"
os.makedirs(dir_seed, exist_ok=True)

# Directory to save CSV files, which are used for table schema
files = ['machine_list.csv', 'operator_list.csv', 'status_table.csv', 'production_list.csv']
machine_path = os.path.join(dir_seed, files[0])
operator_path = os.path.join(dir_seed, files[1])
status_path = os.path.join(dir_seed, files[2])
production_path = os.path.join(dir_seed, files[3])
file_paths = [os.path.join(dir_seed, file) for file in files]

# Class to handle data generation and maintenance
class DataMaintenance:
    def __init__(self):
        self.status_table = []
        self.branches = []
        self.machine_list = []
        self.operator_list = []
        self.production_list = []
        self.generate_data()

    # Create CSV files if they do not exist or load them from PostgreSQL
    def generate_data(self):
        if not os.path.isfile(machine_path) or not os.path.isfile(operator_path) or not os.path.isfile(status_path):
            print("Creating the CSV file: ", machine_path)
            print("Creating the CSV file: ", operator_path)
            print("Creating the CSV file: ", status_path)
            self.branches = ['Plant A', 'Plant B', 'Plant C']
            self.status_table = [
                {"status_id": 1, "status_description": "Running"},
                {"status_id": 2, "status_description": "Stopped"},
                {"status_id": 3, "status_description": "Maintenance"}
            ]
            for _ in range(10):
                self.machine_list.append({
                    "machine_id": str(uuid.uuid4()),
                    "machine_name": fake.word().capitalize() + " Machine",
                    "factory_branch": random.choice(self.branches)
                })
                self.operator_list.append({
                    "operator_id": str(uuid.uuid4()),
                    "operator_name": fake.name(),
                    "operator_cpf": fake.cpf(),
                    "operator_phone": fake.phone_number(),
                    "operator_email": fake.email()
                })

            # Convert into dataframes
            self.df_machine = pd.DataFrame(self.machine_list)
            self.df_operator = pd.DataFrame(self.operator_list)
            self.df_status = pd.DataFrame(self.status_table)

            # Export to csv
            self.df_machine.to_csv(f'{dir_seed}/machine_list.csv', index=False)
            self.df_operator.to_csv(f'{dir_seed}/operator_list.csv', index=False)
            self.df_status.to_csv(f'{dir_seed}/status_table.csv', index=False)
            print("Os arquivos foram criados no diretório: ", dir_seed)
            for path in file_paths[:-1]:
                os.chown(path, 50000, 0)  # UID=50000, GID=0
                os.chmod(path, 0o664)

        else: 
            # If exists then load by postgres and convert to dataframe
            print("All schema files already exist, loading them...")
            try:
                conn = psycopg2.connect(
                    host=os.environ.get("PGHOST"),
                    database=os.environ.get("PGDATABASE"),
                    user=os.environ.get("PGUSER"),
                    password=os.environ.get("PGPASSWORD"),
                    port=os.environ.get("PGPORT", 5432)
                )
                
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    schema = os.environ.get("PGSCHEMA", "public")
                    for table_name in ["machine_list", "operator_list", "status_table"]:
                        cur.execute(f'SELECT * FROM "{schema}"."{table_name}"')
                        setattr(self, table_name, cur.fetchall())

                print("Data loaded from PostgreSQL successfully.")
            except Exception as e:
                print('Trying to load from the CSV')
                try:
                    self.machine_list = pd.read_csv(f'{dir_seed}/machine_list.csv')
                    self.operator_list = pd.read_csv(f'{dir_seed}/operator_list.csv')
                    self.status_table = pd.read_csv(f'{dir_seed}/status_table.csv')

                    print('Success in loading data from CSV')

                except Exception as e:
                    print('Fail to load the data from PostgreSQL and CSV')

            finally:
                if 'conn' in locals() and conn:
                    conn.close()

    # Generate production data
    def generate_production(self, n):
        production_data = []
        for _ in range(n):
            production_date = fake.date_between(start_date='-1y', end_date='today')
            maintenance = random.choice([True, False])
            maintenance_time = str(random.randint(10, 300)) if maintenance else None
            maintenance_cost = str(round(random.uniform(50, 1500), 2)) if maintenance else None

            try:
                record = {
                    "production_id": str(uuid.uuid4()),
                    "machine_id": random.choice(self.machine_list)['machine_id'],
                    "operator_id": random.choice(self.operator_list)['operator_id'],
                    "production_date": production_date.isoformat(),
                    "production_value": round(random.uniform(50, 1500), 2),
                    "production_status": random.choice(self.status_table)['status_id'],
                    "units_produced": random.randint(1, 1000),
                    "maintenance": maintenance,
                    "maintenance_time": maintenance_time,
                    "maintenance_cost": maintenance_cost,
                }
            except:
                try:
                    record = {
                        "production_id": str(uuid.uuid4()),
                        "machine_id": random.choice(self.machine_list['machine_id'].tolist()),
                        "operator_id": random.choice(self.operator_list['operator_id'].tolist()),
                        "production_date": production_date.isoformat(),
                        "production_value": round(random.uniform(50, 1500), 2),
                        "production_status": random.choice(self.status_table['status_id'].tolist()),
                        "units_produced": random.randint(1, 1000),
                        "maintenance": maintenance,
                        "maintenance_time": maintenance_time,
                        "maintenance_cost": maintenance_cost,
                    }
                except Exception as ex:
                    print('Fail in creating record: ', ex)
            production_data.append(record)
        self.production_list.extend(production_data)

        # Set the permissions so that the Astro user can read the file in the directory
        if not os.path.isfile(production_path):
            self.df_production = pd.DataFrame(self.production_list)
            self.df_production.to_csv(f'{dir_seed}production_list.csv', index=False)
            os.chown(production_path, 50000, 0)  # UID=50000, GID=0
            os.chmod(production_path, 0o664)

        return production_data

if __name__ == '__main__':

    dm = DataMaintenance()
    topic = 'my_topic' # Kafka topic name

    # Simulate 10 interactions with the Kafka producer
    # This is just a simulation, in a real-world scenario, you would have a continuous stream of data
    for n in range(0, 10, 1):
        reference_date = (datetime.today() - timedelta(days=n)).strftime('%d%m%Y_%H%M%S')
        prod_data = dm.generate_production(100) # Generate 100 records
        producer.send(topic, key=reference_date, value=prod_data) # Send the data to Kafka
        time.sleep(1)
