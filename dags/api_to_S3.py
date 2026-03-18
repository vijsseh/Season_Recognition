import requests
import random
import json
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

load_dotenv()
### НАСТРОЙКИ API ###
GEONAMES_USERNAME = os.getenv("GEONAMES_USERNAME") # регистрация бесплатная: geonames.org
API_KEY = os.getenv("GOOGLE_API_KEY")  # google apiшка

# Параметры для MinIO (S3)
bucket_name = "highresolutionseasons"
s3_hook = S3Hook(aws_conn_id="minios3_conn")

counter = 0
saved_count = 0


# Функция для получения случайных координат
def get_random_coords(lat_min, lat_max, lon_min, lon_max):
    lat = random.uniform(lat_min, lat_max)
    lon = random.uniform(lon_min, lon_max)
    return lat, lon


# Функция для получения изображения с Google Street View Static API и загрузки в MinIO
def get_street_view_image_to_minio(lat_min, lat_max, lon_min, lon_max, radius, api_key):
    global counter, saved_count

    rand_lat, rand_lon = get_random_coords(lat_min, lat_max, lon_min, lon_max)

    url = f"https://maps.googleapis.com/maps/api/streetview/metadata?size=900x600&location={rand_lat},{rand_lon}&return_error_code=true&heading=90&pitch=0&radius={radius}&key={api_key}"
    response_json = requests.get(url).json()

    if response_json['status'] == "OK":
        lat, lng = response_json['location']['lat'], response_json['location']['lng']
        image_url = f"https://maps.googleapis.com/maps/api/streetview?size=900x600&location={lat},{lng}&return_error_code=true&heading=90&pitch=0&radius={radius}&key={api_key}"
        response_image = requests.get(image_url)

        work_dir = response_json['pano_id']
        file_name = f"{work_dir}/street_view_image.jpg"
        json_file_name = f"{work_dir}/data.json"
        # Загрузка изображения в MinIO
        try:
            s3_hook.load_bytes(json.dumps(response_json).encode('utf-8'), json_file_name, bucket_name=bucket_name)
            # Затем загружаем изображение
            s3_hook.load_bytes(response_image.content, file_name, bucket_name=bucket_name)
            saved_count += 1
            print(f"Изображение успешно загружено в MinIO как {file_name}")
        except Exception as e:
            print(f"Ошибка при загрузке изображения в MinIO: {e}")
    else:
        print(f"Ошибка получения изображения для {rand_lat}, {rand_lon}")


# DAG для запуска процесса
def run_dag(**kwargs):
    global counter
    api_n = 20000
    lat_min = 45
    lat_max = 70
    lon_min = 33
    lon_max = 160

    try:
        while counter <= api_n:
            get_street_view_image_to_minio(lat_min, lat_max, lon_min, lon_max, API_KEY)
            counter += 1
    except Exception as e:
        print(f'За {api_n} запросов получено {saved_count} фотографий. Ошибка: {e}')


# Определение DAG
def run_dag(**context):
    global counter
    api_n = 1000
    lat_min = 45
    lat_max = 70
    lon_min = 33
    lon_max = 160

    try:
        while counter <= api_n:
            get_street_view_image_to_minio(lat_min, lat_max, lon_min, lon_max, 100000, API_KEY)
            counter += 1
    except Exception as e:
        print(f'За {api_n} запросов получено {saved_count} фотографий. Ошибка: {e}')

with DAG(
    'minio_streetview_dag',
    description='Загрузка изображений из Google Street View в MinIO',
    schedule_interval='@hourly',  # можно настроить расписание, например, ежедневно
    start_date=datetime(2026, 3, 16),
    catchup=False,
) as dag:
    # Задача в DAG
    upload_task = PythonOperator(
        task_id='upload_streetview_images_to_minio',
        python_callable=run_dag,
        provide_context=True,
    )



upload_task