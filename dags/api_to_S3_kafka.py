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
        timeout_ms = 500 # Таймаут на 1 секунду
        # Инициализация счетчика ДО цикла, иначе он всегда сбрасывается
        streak = 0
        message_count = 0

        while True:
            # poll() в confluent_kafka возвращает ОДНО сообщение за раз из внутренного буфера пакета
            msg = consumer.poll(timeout=timeout_ms / 1000)

            if msg is None:
                streak += 1
                if streak > 150:
                    print("Кафка простаивает. Завершение работы.")
                    break  # Для Airflow DAG лучше использовать break, чтобы корректно выйти из функции
                print("No message received")
                continue
            else:
                print(msg.value().decode('utf-8'))
                streak = 0

            # Если сообщение получено, сбрасываем стрик пустых ответов
            streak = 0

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"End of partition reached {msg.partition()}")
                else:
                    raise KafkaException(msg.error())
                continue

            # Обработка успешного сообщения
            message_count += 1
            partitions = consumer.assignment()
            print(f"{message_count} Received message, партиции: {partitions}")

            try:
                data = json.loads(msg.value().decode('utf-8'))
                lat = data['location']['lat']
                lng = data['location']['lng']
                year_month = data['date']
                key = f"{lat}x{lng}x{year_month}/data.json"

                # Делаем сразу GET запрос (минус один сетевой запрос)
                url = f'https://archive-api.open-meteo.com/v1/archive?latitude={lat}&longitude={lng}&start_date={year_month}-01&end_date={year_month}-28&daily=temperature_2m_mean&timezone=auto'

                response = requests.get(url, timeout=(3.05, 3))

                if response.status_code == 200:
                    res_json = response.json()
                    print(f'GOT HTTP RESPONSE FROM API: {response.status_code}')

                    if 'error' not in res_json:
                        res_daily = res_json.get('daily', {})
                        temps = res_daily.get('temperature_2m_mean', [])

                        # Защита от пустых списков температур во избежание ZeroDivisionError
                        if temps:
                            data['elevation'] = res_json.get('elevation')
                            data['units_temp'] = res_json.get('daily_units', {}).get('temperature_2m_mean')
                            data['mean_temp'] = sum(temps) / len(temps)

                        # Дополняем сообщение и отправляем в S3
                        edited_data = json.dumps(data).encode('utf-8')
                        s3_hook.load_bytes(edited_data, key=key, bucket_name=bucket_name, replace=True)
                        print('UPLOAD JSON TO MINIO')
                    else:
                        print(f"API returned error: {res_json['error']}")
                else:
                    print(f'API responded with status code: {response.status_code}')

            except requests.RequestException as e:
                print(f'Ошибка сети или таймаут API: {e}')
            except (json.JSONDecodeError, KeyError) as e:
                print(f'Ошибка валидации/парсинга данных: {e}')
            except Exception as e:
                print(f'Непредвиденная ошибка: {e}')

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
    schedule_interval=None,
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

if __name__ == "__main__":
    print(1)