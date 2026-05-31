from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime


with DAG(
    'DAG_Vertica',
    start_date=datetime(2022, 10, 1),
    schedule_interval='@daily',
    catchup=True,
    tags=['Kafka', 'Spark', 'Vertica']
) as dag:

    task_vertica = BashOperator(
        task_id='task_vertica',
        bash_command=(
            'spark-submit '
            # Библиотека для интеграции Spark с Kafka.
            '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 '
            '--jars /lessons/vertica-jdbc-25.3.0-0.jar '
            '--master local[*] '
            '/lessons/py/ETL_Vertica.py '
            '{{ ds }}'
        )
    )
