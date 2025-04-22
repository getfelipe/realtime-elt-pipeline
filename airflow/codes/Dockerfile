FROM python:3.12-slim

WORKDIR /app

COPY producer.py .

ENV PGHOST=""
ENV PGDATABASE=""
ENV PGUSER=""
ENV PGPASSWORD=""
ENV PGPORT=5432
ENV PGSCHEMA=""

RUN pip install pandas kafka-python faker psycopg2-binary

CMD ["python", "producer.py"]