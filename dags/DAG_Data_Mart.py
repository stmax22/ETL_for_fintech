from airflow import DAG
from airflow.providers.vertica.operators.vertica import VerticaOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from datetime import datetime


# Данные для подключения к БД.
vertica_conn_id = 'conn_vertica'

with DAG(
    'DML_Vertica',
    start_date=datetime(2022, 10, 1),
    schedule_interval='@daily',
    catchup=True,
    tags=['Kafka', 'Spark', 'Vertica']
) as dag:

    wait_for_task = ExternalTaskSensor(
        task_id='wait_for_task_vertica',
        external_dag_id='DAG_Vertica',
        external_task_id='task_vertica',
        execution_delta=None,
        mode='reschedule',
        poke_interval=60,
        timeout=1800
    )

    dml_vertica = VerticaOperator(
        task_id='dml_vertica',
        vertica_conn_id=vertica_conn_id,
        sql='/lessons/sql/DML_Vertica.sql'
    )

    wait_for_task >> dml_vertica
