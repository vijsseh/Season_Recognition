import os
import json
import random
import requests
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from airflow.models.param import Param
from airflow.utils.state import TaskInstanceState

from confluent_kafka import Producer, Consumer, KafkaException, KafkaError, TopicPartition


CONTINENT_CONFIG = {
    'eurasia': {'lat': (45, 70), 'lon': (33, 160), 'partition': 1},
    'asia': {'lat': (24, 50), 'lon': (49, 116), 'partition': 3},
    'europe': {'lat': (40, 60), 'lon': (0, 35), 'partition': 4},
    'north_america': {'lat': (30, 60), 'lon': (-121, -77), 'partition': 2},
    'south_america': {'lat': (-32, 11), 'lon': (-80, -34), 'partition': 0}
}

API_KEY = Variable.get("GoogleApiKey")
BUCKET_NAME = "testseasons"
KAFKA_BROKER = "kafka:9093"
TOPIC_NAME = "metadata"

s3_hook = S3Hook(aws_conn_id="minios3_conn")


def get_random_coords(lat_bounds: tuple, lon_bounds: tuple) -> tuple:
    """Генерация случайных координат в заданных границах."""
    lat = random.uniform(*lat_bounds)
    lon = random.uniform(*lon_bounds)
    return lat, lon


def get_street_view_image_to_minio(config: dict, producer: Producer, partition: int, counter: int) -> None:
    """Запрос метаданных и фото из Google API, отправка в Kafka и S3."""
    rand_lat, rand_lon = get_random_coords(config['lat'], config['lon'])

    url = f"https://maps.googleapis.com/maps/api/streetview/metadata?size=640x640&location={rand_lat},{rand_lon}&return_error_code=true&heading=90&pitch=0&radius=55000&key={API_KEY}"

    try:
        response = requests.get(url, timeout=5)
        response_json = response.json()
    except Exception as e:
        print(f"[{counter}] Ошибка запроса к API метаданных: {e}")
        return

    if response_json.get('status') == "OK":
        lat, lng = response_json['location']['lat'], response_json['location']['lng']
        image_url = f"https://maps.googleapis.com/maps/api/streetview?size=640x640&location={lat},{lng}&return_error_code=true&heading=90&pitch=0&radius=55000&key={API_KEY}"

        try:
            response_image = requests.get(image_url, timeout=10)
            year_month = response_json['date']
            work_dir = f"{lat}x{lng}x{year_month}"
            file_key = f"{work_dir}/street_view_image.jpg"

            producer.produce(
                topic=TOPIC_NAME,
                partition=partition,
                value=json.dumps(response_json).encode('utf-8')
            )

            s3_hook.load_bytes(response_image.content, file_key, bucket_name=BUCKET_NAME, replace=True)
            print(f"[{counter}] Изображение успешно загружено в MinIO: {file_key}")

        except Exception as e:
            print(f"[{counter}] Ошибка при обработке или загрузке медиа-файлов: {e}")
    else:
        print(f"[{counter}] Точка {rand_lat}, {rand_lon} не содержит панорам StreetView.")


def run_dag(**context) -> str:
    """Таска-Продюсер: генерирует запросы и наполняет Кафку."""
    dag_run_conf = context.get('dag_run').conf if context.get('dag_run') else {}
    continent = dag_run_conf.get('continent', 'europe')
    api_n = dag_run_conf.get('api_n', 1000)

    cfg = CONTINENT_CONFIG.get(continent, CONTINENT_CONFIG['europe'])
    partition = cfg['partition']

    print(f"Запуск генерации данных для континента: {continent}, Партиция: {partition}")

    producer = Producer({"bootstrap.servers": KAFKA_BROKER})

    counter = 0
    while counter < api_n:
        get_street_view_image_to_minio(cfg, producer, partition, counter)
        counter += 1

    producer.flush()
    return f'Успешно обработано запросов к API: {counter}'


def consume_from_kafka(**context) -> None:
    """Таска-Консьюмер: параллельно обогащает данные погодой, ожидая новые сообщения."""
    dag_run_conf = context.get('dag_run').conf if context.get('dag_run') else {}
    continent = dag_run_conf.get('continent', 'europe')

    cfg = CONTINENT_CONFIG.get(continent, CONTINENT_CONFIG['europe'])
    partition = cfg['partition']

    dag_run = context['dag_run']
    producer_ti = dag_run.get_task_instance('upload_streetview_images_to_minio')

    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': f'airflow_group_{continent}',
        'auto.offset.reset': 'earliest',  # Точка вместо нижнего подчеркивания
        'enable.auto.commit': True
    })
    consumer.assign([TopicPartition(topic=TOPIC_NAME, partition=partition)])

    try:
        print(f"Консьюмер запущен. Слушает партицию {partition} для региона {continent}...")

        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                producer_ti.refresh_from_db()
                producer_is_running = producer_ti.state in [TaskInstanceState.RUNNING, TaskInstanceState.QUEUED]

                if producer_is_running:
                    print("В Кафке пусто, но продюсер еще работает. Ожидаю 5 секунд...")
                    time.sleep(5)
                    continue
                else:
                    print("Продюсер завершил работу. Очередь пуста. Завершение обогащения.")
                    break

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    producer_ti.refresh_from_db()
                    if producer_ti.state in [TaskInstanceState.RUNNING, TaskInstanceState.QUEUED]:
                        time.sleep(5)
                        continue
                    else:
                        break
                else:
                    raise KafkaException(msg.error())

            try:
                data = json.loads(msg.value().decode('utf-8'))
                lat, lng = data['location']['lat'], data['location']['lng']
                year_month = data['date']
                key = f"{lat}x{lng}x{year_month}/data.json"

                url = f'https://archive-api.open-meteo.com/v1/archive?latitude={lat}&longitude={lng}&start_date={year_month}-01&end_date={year_month}-28&daily=temperature_2m_mean&timezone=auto'
                response = requests.get(url, timeout=5)

                if response.status_code == 200:
                    res_json = response.json()
                    if 'error' not in res_json:
                        temps = res_json.get('daily', {}).get('temperature_2m_mean', [])
                        if temps:
                            data['elevation'] = res_json.get('elevation')
                            data['units_temp'] = res_json.get('daily_units', {}).get('temperature_2m_mean')
                            data['mean_temp'] = sum(temps) / len(temps)

                        s3_hook.load_bytes(
                            json.dumps(data).encode('utf-8'),
                            key=key,
                            bucket_name=BUCKET_NAME,
                            replace=True
                        )
                        print(f"Успешно обогащен и сохранен JSON для координат: {lat}, {lng}")
                    else:
                        print(f"Open-Meteo API вернул ошибку: {res_json['error']}")
                else:
                    print(f"Open-Meteo API ответил со статусом: {response.status_code}")

            except Exception as e:
                print(f"Ошибка обработки отдельного сообщения консьюмером: {e}")

    finally:
        consumer.close()


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(seconds=10),
}

with DAG(
        dag_id='minio_streetview_dag',
        description='Параллельная загрузка изображений Google Street View в MinIO и стриминг-обогащение через Kafka',
        tags=['dataset', 'streaming'],
        schedule_interval=None,
        start_date=datetime(2026, 3, 16),
        catchup=False,
        default_args=default_args,
        params={
            "continent": Param(default="europe", type="string"),
            "api_n": Param(default=1000, type="integer")
        },
        max_active_runs=5,
) as dag:
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