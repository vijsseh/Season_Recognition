import requests
import json
from confluent_kafka import Consumer, KafkaException, KafkaError
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

bucket_name = "testseasons"
s3_hook = S3Hook(aws_conn_id="minios3_conn")


# Функция для обработки сообщений из Kafka
def consume_from_kafka() -> None:
    # Подключаемся к Kafka
    consumer = Consumer({
        'bootstrap.servers': 'kafka:9093',  # Адрес Kafka
        'group.id': 'apis',  # ID группы потребителей
        'auto.offset.reset': 'earliest'  # Начинаем читать с самого начала
    })

    # Подписываемся на топик
    consumer.subscribe(['meta'])

    # Ожидаем получения сообщений
    try:
        timeout_ms = 1000  # Таймаут на 1 секунду
        message_count = 0  # Считаем количество полученных сообщений

        while True:
            msg = consumer.poll(timeout=timeout_ms / 1000)

            if msg is None:
                print("No message received")
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"End of partition reached {msg.partition}")
                else:
                    raise KafkaException(msg.error())
            else:
                message_count += 1
                print(f"{message_count} Received message: {msg.value().decode('utf-8')}")
                data = json.loads(msg.value().decode('utf-8'))
                lat, lng = data['location']['lat'], data['location']['lng']
                year_month = data['date']
                key = str(lat) + 'x' + str(lng) + 'x' + str(year_month) + '/data.json'

                # Получаем данные о погоде
                url = f'https://archive-api.open-meteo.com/v1/archive?latitude={lat}&longitude={lng}&start_date={year_month}-01&end_date={year_month}-28&daily=temperature_2m_mean&timezone=auto'

                response_temp = requests.get(url, verify=False)
                if response_temp.status_code == 200:
                    print(f'GOT HTTP RESPONSE FROM API: {response_temp.status_code}')

                    if 'error' not in response_temp:
                        data['elevation'] = response_temp.json()['elevation']
                        data['units_temp'] = response_temp.json()['daily_units']['temperature_2m_mean']
                        data['mean_temp'] = sum(response_temp.json()['daily']['temperature_2m_mean']) / len(response_temp.json()['daily']['temperature_2m_mean'])

                    # Дополняем сообщение
                    edited_data = json.dumps(data).encode('utf-8')

                    # Загружаем в S3
                    s3_hook.load_bytes(edited_data, key=key, bucket_name=bucket_name, replace=True)
                    print('UPLOAD JSON TO MINIO')
                else:
                    print(f'GOT HTTP RESPONSE FROM API: {response_temp.status_code}')

    except KeyboardInterrupt:
        print("Process interrupted")
    finally:
        consumer.close()


# Определение DAG для Airflow
default_args = {
    "retries": 10,
    "retry_delay": timedelta(seconds=5), ## между попытками ждем 10 секунд
}

with DAG(
        dag_id="08.kafka_event_driven",
        default_args=default_args,
        start_date=datetime(2023, 1, 1),
        schedule=None,  # Необходимо использовать `schedule_interval` для периодичности
        catchup=False,
        tags=['kafka']
) as dag:
    enrich_event = PythonOperator(
        task_id="enrich_event",
        python_callable=consume_from_kafka,
    )

    enrich_event