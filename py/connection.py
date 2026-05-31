import os
from dotenv import load_dotenv

load_dotenv()

kafka_host = str(os.getenv('KAFKA_HOST'))
kafka_password = str(os.getenv('KAFKA_CONSUMER_PASSWORD'))
kafka_port = int(str(os.getenv('KAFKA_PORT')))
kafka_topic = str(os.getenv('KAFKA_SOURCE_TOPIC'))
kafka_username = str(os.getenv('KAFKA_CONSUMER_USERNAME'))

vertica_db = str(os.getenv('VERTICA_WAREHOUSE_DB'))
vertica_host = str(os.getenv('VERTICA_WAREHOUSE_HOST'))
vertica_password = str(os.getenv('VERTICA_WAREHOUSE_PASSWORD'))
vertica_port = str(os.getenv('VERTICA_WAREHOUSE_PORT'))
vertica_url = str(os.getenv('VERTICA_WAREHOUSE_URL'))
vertica_user = str(os.getenv('VERTICA_WAREHOUSE_USER'))
