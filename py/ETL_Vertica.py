import logging
import sys

from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StructType,
    StructField,
    StringType
)
from connection import (
    vertica_password,
    vertica_url,
    vertica_user,
    kafka_host,
    kafka_password,
    kafka_port,
    kafka_topic,
    kafka_username
)


# Задаем формат лог-сообщений.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def spark_init(name: str) -> SparkSession:
    """Метод создаёт Spark сессию."""

    try:
        Session = (
            SparkSession.builder
                        .appName(name)
                        .config('spark.sql.session.timeZone', 'UTC')
                        .getOrCreate()
        )

        logging.info('Spark сессия успешно создана!')

        return Session

    except Exception:
        logging.exception('Ошибка при создании Spark сессии.')
        raise


def read_from_kafka(spark: SparkSession, date: str) -> DataFrame:
    """Метод читает входные данные из Kafka и приводит их в нужный вид.
       В Kafka мы получаем данные для 2-х таблиц (transactions и currencies).
    """

    try:
        # Получаем вчерашнее число.
        yesterday = datetime.strptime(date, "%Y-%m-%d").date()

        logging.info(f'Дата за которую необходимо получить данные: {yesterday}.')

        # Читаем входные данные из Kafka.
        kafka_security_options = {
            'kafka.bootstrap.servers': f'{kafka_host}:{kafka_port}',
            'kafka.security.protocol': 'SASL_SSL',
            'kafka.sasl.mechanism': 'SCRAM-SHA-512',
            'kafka.sasl.jaas.config': f'org.apache.kafka.common.security.scram.ScramLoginModule required username={kafka_username} password={kafka_password};',
        }

        load_df = (
            spark.read
                 .format('kafka')
                 .options(**kafka_security_options)
                 .option('subscribe', kafka_topic)
                 .load()
        )

        logging.info('Данные прочитаны из Kafka!')

        # Общая схема.
        general_scheme = StructType(
            [
                StructField('object_id', StringType(), True),
                StructField('object_type', StringType(), True),
                StructField('sent_dttm', StringType(), True),
                StructField('payload', StringType(), True)
            ]
        )

        # Десериализуем данные, убираем строки с Null и оставляем данные за нужную дату.
        parsed_value = load_df.select(F.from_json(F.col('value').cast('string'),
                                                  general_scheme).alias('parsed_value')) \
                              .select('parsed_value.*') \
                              .na.drop(how='all') \
                              .where((F.to_date(F.col('sent_dttm')) == yesterday))

        return parsed_value

    except Exception:
        logging.exception('Ошибка при обработке данных из Kafka.')
        raise


def preparing_data_currencies(df: DataFrame) -> None:
    """Метод подготавливает данные для записи в таблицу "currencies" базы данных Vertica."""

    try:
        # Схема payload данных из CURRENCY.
        payload_schema_currency = StructType(
            [
                StructField('date_update', StringType(), True),
                StructField('currency_code', IntegerType(), True),
                StructField('currency_code_with', IntegerType(), True),
                StructField('currency_with_div', DecimalType(5, 3), True)
            ]
        )

        # Подготавливаем данные.
        df_currencies = df.select('payload') \
                          .where(F.col('object_type') == 'CURRENCY')

        payload_currencies = df_currencies.select(F.from_json(F.col('payload').cast('string'),
                                                  payload_schema_currency).alias('parsed_payload')) \
                                          .select('parsed_payload.*')

        # Проверяем наличие хотя бы одной записи в датафрейме.
        if payload_currencies.take(1):
            logging.info('Данные для таблицы "currencies" обработаны! Начинаем загружать данные в Vertica.')

            write_df(payload_currencies, 'currencies')

    except Exception:
        logging.exception('Ошибка при обработке данных для таблицы "currencies".')
        raise


def preparing_data_transactions(df: DataFrame) -> None:
    """Метод подготавливает данные для записи в таблицу "transactions" базы данных Vertica."""

    try:
        # Схема payload данных из TRANSACTION.
        payload_schema_transaction = StructType(
            [
                StructField('operation_id', StringType(), True),
                StructField('account_number_from', IntegerType(), True),
                StructField('account_number_to', IntegerType(), True),
                StructField('currency_code', IntegerType(), True),
                StructField('country', StringType(), True),
                StructField('status', StringType(), True),
                StructField('transaction_type', StringType(), True),
                StructField('amount', IntegerType(), True),
                StructField('transaction_dt', StringType(), True)
            ]
        )

        # Подготавливаем данные для записи в таблицу transactions БД.
        df_transactions = df.select('payload') \
                            .where(F.col('object_type') == 'TRANSACTION')

        payload_transactions = df_transactions.select(F.from_json(F.col('payload').cast('string'),
                                                                  payload_schema_transaction).alias('parsed_payload')) \
                                              .select('parsed_payload.*')

        # Проверяем наличие хотя бы одной записи в датафрейме.
        if payload_transactions.take(1):
            logging.info('Данные для таблицы "transactions" обработаны! Начинаем загружать данные в Vertica.')

            write_df(payload_transactions, 'transactions')

    except Exception:
        logging.exception('Ошибка при обработке данных для таблицы "transactions".')
        raise


def write_df(df: DataFrame, name_table: str) -> None:
    """Метод записывает обработанные данные в таблицы Vertica."""

    # Формируем полное название таблицы.
    table_full_name = f'VT25110761DD38__STAGING.{name_table}'

    # Конфигурация подключения.
    vertica_config = {
        'url': vertica_url,
        'dbtable': table_full_name,
        'user': vertica_user,
        'password': vertica_password,
        'driver': 'com.vertica.jdbc.Driver'
    }

    try:
        # Записываем DataFrame в Vertica через Spark JDBC.
        df.write \
          .format('jdbc') \
          .options(**vertica_config) \
          .mode('append') \
          .save()

        logging.info(f'Данные успешно загружены в таблицу "{table_full_name}"!')
    except Exception as e:
        logging.error(f'Ошибка загрузки данных: {e}')
        raise


if __name__ == "__main__":
    business_dt = sys.argv[1]
    spark = spark_init('Spark_connection')
    df_from_kafka = read_from_kafka(spark, business_dt)
    df_from_kafka.persist()  # Cохраняем датафрейм в память.
    preparing_data_currencies(df_from_kafka)
    preparing_data_transactions(df_from_kafka)
    df_from_kafka.unpersist()  # Очищаем память от датафрейма.
