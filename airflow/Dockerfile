# image for astronomer/astro-runtime:12.7.1
FROM quay.io/astronomer/astro-runtime:12.7.1

# Update repositories and install git
USER root
RUN apt-get update && apt-get install -y git

# Create seeds directory, which will be mounted as a volume in the dbt container
# and will be used to store the dbt seeds
RUN mkdir -p /opt/airflow/dags/dbt_2/seeds 

# Create a virtual environment for dbt and install dbt-postgres
USER astro
RUN python -m venv /usr/local/airflow/dbt_venv && \
    /usr/local/airflow/dbt_venv/bin/pip install dbt-postgres==1.8.2