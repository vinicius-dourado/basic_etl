# my_dag.py
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.operators.email import EmailOperator
from etl import extract, transform, load
import logging

#I've decided to implement a SLA so that we can escale the support in case of failure.
sla_time = timedelta(hours=1) 

logging.basicConfig(level=logging.INFO)

email_to = 'viuaraujo@gmail.com'

@dag(schedule_interval=timedelta(days=1), start_date=datetime(2024, 1, 1), catchup=False, tags=['etl'],
    sla_miss_callback=EmailOperator(
    task_id='email_notification',
    to=email_to,
    subject='SLA Missed!',
    html_content="SLA Missed! The DAG '{{ dag.dag_id }}' didn't complete in time.",
    ),
    default_args={
        'retries': 3,  # Number of retries
        'retry_delay': timedelta(minutes=2), # Time btw tries
    }
    )

def etl_dag():

    @task()
    def extract_task():
        logging.info("Starting extract task...")
        result = extract()
        logging.info("Extract task completed.")
        return result

    @task()
    def transform_task(raw_path):
        logging.info("Starting transform task...")
        result = transform(raw_path)
        logging.info("Transform task completed.")
        return result


    @task()
    def load_task(transform_result):
        logging.info("Starting load task...")
        curated_path, stats_path = transform_result
        load(curated_path, stats_path)
        logging.info("Load task completed.")

    raw_result = extract_task()
    transform_result = transform_task(raw_result)
    load_task(transform_result)

my_dag = etl_dag()
