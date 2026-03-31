import requests
import random
import json
from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from confluent_kafka import Producer, Consumer, KafkaException, KafkaError, TopicPartition
from airflow.models.param import Param

### НАСТРОЙКИ API ###
API_KEY = Variable.get("GoogleApiKey")  # google apiшка
# Параметры для MinIO (S3)
bucket_name = "testseasons"
s3_hook = S3Hook(aws_conn_id="minios3_conn")


# Функция для получения случайных координат
def get_random_coords(lat_min: int, lat_max: int, lon_min: int, lon_max: int) -> tuple:
    lat = random.uniform(lat_min, lat_max)
    lon = random.uniform(lon_min, lon_max)
    return lat, lon


# Функция для получения изображения с Google Street View Static API и загрузки в MinIO
def get_street_view_image_to_minio(lat_min: int, lat_max: int, lon_min: int, lon_max: int, radius: int, api_key: str, partition: int, counter: int) -> None:
    conf = {"bootstrap.servers": "kafka:9093"}
    producer = Producer(conf)  # Замените на имя вашего контейнера с Kafka
    topic = 'metadata'  # Замените на нужный вам топик

    rand_lat, rand_lon = get_random_coords(lat_min, lat_max, lon_min, lon_max)

    url = f"https://maps.googleapis.com/maps/api/streetview/metadata?size=640x640&location={rand_lat},{rand_lon}&return_error_code=true&heading=90&pitch=0&radius={radius}&key={api_key}"
    response_json = requests.get(url).json()

    if response_json['status'] == "OK":
        lat, lng = response_json['location']['lat'], response_json['location']['lng']
        image_url = f"https://maps.googleapis.com/maps/api/streetview?size=640x640&location={lat},{lng}&return_error_code=true&heading=90&pitch=0&radius={radius}&key={api_key}"
        response_image = requests.get(image_url)
        year_month = response_json['date']

        work_dir = str(lat) + 'x' + str(lng) + 'x' + str(year_month)
        file_key = f"{work_dir}/street_view_image.jpg"
        # json_file_name = f"{work_dir}/data.json"

        try:
            # s3_hook.load_bytes(json.dumps(response_json).encode('utf-8'), json_file_name, bucket_name=bucket_name)
            producer.produce(topic=topic, partition=partition, value=json.dumps(response_json).encode('utf-8'))
            s3_hook.load_bytes(response_image.content, file_key, bucket_name=bucket_name)
            print(f"{counter} Изображение успешно загружено в MinIO как {file_key}")
        except Exception as e:
            print(f"{counter} Ошибка при загрузке изображения в MinIO: {e}")
    else:
        print(f"{counter} Ошибка получения изображения для {rand_lat}, {rand_lon}")


def consume_from_kafka(**context) -> None:

    config = context.get('dag_run').conf if context.get('dag_run') else {}

    if config['continent'] == 'eurasia':
        partition = 1
    elif config['continent'] == 'asia':
        # # asia
        partition = 3
    elif config['continent'] == 'europe':
        # europe
        partition = 4
    elif config['continent'] == 'north_america':
        partition = 2
    else:
        partition = 0

    consumer = Consumer({
        'bootstrap.servers': 'kafka:9093',  # Адрес Kafka
        'group.id': f'{config['continent']}',  # ID группы потребителей
        'auto.offset.reset': 'earliest',
        'enable.idempotence': 'true'# Начинаем читать с самого начала
    })

    consumer.assign([TopicPartition(topic='metadata', partition=partition)])
    try:
        timeout_ms = 1000  # Таймаут на 1 секунду
        message_count = 0  # Считаем количество полученных сообщений

        while True:
            partitions = consumer.assignment()
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
                print(f"{message_count} Received message: {msg.value().decode('utf-8')}, партиции: {partitions}")
                data = json.loads(msg.value().decode('utf-8'))
                lat, lng = data['location']['lat'], data['location']['lng']
                year_month = data['date']
                key = str(lat) + 'x' + str(lng) + 'x' + str(year_month) + '/data.json'

                # Получаем данные о погоде
                url = f'https://archive-api.open-meteo.com/v1/archive?latitude={lat}&longitude={lng}&start_date={year_month}-01&end_date={year_month}-28&daily=temperature_2m_mean&timezone=auto'

                try:
                    response = requests.head(url, timeout=5)
                    if response.status_code == 200:
                        response_temp = requests.get(url)
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
                except:
                    print('Нет ответа от апи')
    except KeyboardInterrupt:
        print("Process interrupted")
    finally:
        consumer.close()

def run_dag(**context) -> str:
    config = context.get('dag_run').conf if context.get('dag_run') else {}

    counter = 0
    api_n = config['api_n']

    print(f"Searching streetview in {config['continent']}")
    if config['continent'] == 'eurasia':
        lat_min, lat_max, lon_min, lon_max = 45, 70, 33, 160
        partition = 1
    elif config['continent'] == 'asia':
        # # asia
        lat_min, lat_max, lon_min, lon_max  = 24, 50, 49, 116
        partition = 3
    elif config['continent'] == 'europe':
        # europe
        lat_min, lat_max, lon_min, lon_max  = 40, 60, 0, 35
        partition = 4
    elif config['continent'] == 'north_america':
        lat_min, lat_max, lon_min, lon_max = 30, 60, -121, -77
        partition = 2
    else:
        lat_min, lat_max, lon_min, lon_max  = -32, 11, -80, -34
        partition = 0

    while counter <= api_n:
        try:
            get_street_view_image_to_minio(lat_min, lat_max, lon_min, lon_max, 55000, API_KEY, partition, counter)
            counter += 1
        except Exception as e:
            print(f'Ошибка: {e}')

    return f'За {api_n} запросов получено {counter} фотографий'

default_args = {
    "retries": 5,
    "retry_delay": timedelta(seconds=5), ## между попытками ждем 10 секунд
}

with DAG(
    dag_id='minio_streetview_dag',
    description='Загрузка изображений из Google Street View в MinIO',
    schedule_interval=None,  # можно настроить расписание, например, ежедневно
    start_date=datetime(2026, 3, 16),
    catchup=False,
    default_args=default_args,
    params={
        "continent": Param(
            default="europe",
            type="string",
        ),

        "api_n": Param(
            default=1000,
            type="integer",
        )
    },
    max_active_runs=5,
) as dag:
    # Задача в DAG
    upload_task = PythonOperator(
        task_id='upload_streetview_images_to_minio',
        python_callable=run_dag,
        provide_context=True,
    )

    enrich_event = PythonOperator(
        task_id="enrich_event",
        python_callable=consume_from_kafka,
        provide_context=True,
    )

[upload_task, enrich_event]