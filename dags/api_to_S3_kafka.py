import requests
import random
import json
from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime
from confluent_kafka import Producer

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
def get_street_view_image_to_minio(lat_min: int, lat_max: int, lon_min: int, lon_max: int, radius: int, api_key: str, counter: int) -> None:
    conf = {"bootstrap.servers": "kafka:9093"}
    producer = Producer(conf)  # Замените на имя вашего контейнера с Kafka
    topic = 'meta'  # Замените на нужный вам топик

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
            producer.produce(topic=topic, value=json.dumps(response_json).encode('utf-8'))
            s3_hook.load_bytes(response_image.content, file_key, bucket_name=bucket_name)
            print(f"{counter} Изображение успешно загружено в MinIO как {file_key}")
        except Exception as e:
            print(f"{counter} Ошибка при загрузке изображения в MinIO: {e}")
    else:
        print(f"{counter} Ошибка получения изображения для {rand_lat}, {rand_lon}")


def run_dag(**context) -> str:
    counter = 0
    api_n = 3000
    # # rus
    # lat_min = 45
    lat_max = 70
    lon_min = 33
    lon_max = 160

    # asia
    lat_min = 24
    # lat_max = 50
    # lon_min = 49
    lon_max = 116
    while counter <= api_n:
        try:
            get_street_view_image_to_minio(lat_min, lat_max, lon_min, lon_max, 155000, API_KEY, counter)
            counter += 1
        except Exception as e:
            print(f'Ошибка: {e}')

    return f'За {api_n} запросов получено {counter} фотографий'


with DAG(
    'minio_streetview_dag',
    description='Загрузка изображений из Google Street View в MinIO',
    schedule_interval='@daily',  # можно настроить расписание, например, ежедневно
    start_date=datetime(2026, 3, 16),
    catchup=False,
) as dag:
    # Задача в DAG
    upload_task = PythonOperator(
        task_id='upload_streetview_images_to_minio',
        python_callable=run_dag,
        provide_context=True,
    )

    # erichment_task = PythonOperator(
    #     task_id='enrichment_of_data',
    #     python_callable=enrichment_of_data,
    #     provide_context=True,
    # )

upload_task