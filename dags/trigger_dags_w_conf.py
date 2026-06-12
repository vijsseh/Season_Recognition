import os
import io
import json
import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
from minio import Minio


ACCESS_KEY = os.getenv('MINIO_ROOT_USER')
SECRET_KEY = os.getenv('MINIO_ROOT_PASSWORD')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
}

with DAG(
        dag_id='trigger_dags_w_conf',
        start_date=datetime(2026, 3, 31),
        default_args=default_args,
        schedule_interval='@daily',
        tags=['dag run'],
        max_active_runs=1,
        catchup=False,

) as dag:

    continents = ["europe", "south_america", "north_america", "eurasia", "asia"]

    trigger_tasks = []
    for continent in continents:
        trigger_task = TriggerDagRunOperator(
            task_id=f'trigger_dagrun_{continent}',
            trigger_dag_id='minio_streetview_dag',
            conf={"continent": continent, "api_n": 5},
            wait_for_completion=True,
            deferrable=True,
            poke_interval=30,
        )
        trigger_tasks.append(trigger_task)


    # 3. Наш отрефакторенный скрипт компакции метаданных в Parquet
    @task(task_id="consolidate_metadata_to_parquet")
    def aggregate_metadata_to_parquet():
        client = Minio("minio:9000", access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=False)
        bucket_name = 'testseasons'

        all_dataframe_data = []
        objects = client.list_objects(bucket_name, recursive=True)

        for obj in objects:
            if not obj.object_name.lower().endswith('data.json'):
                continue

            try:
                response = client.get_object(bucket_name, obj.object_name)
                data_json = json.load(response)

                date = data_json.get('date')
                if date:
                    month = int(date.split('-')[1])
                    seasons = {12: 'winter', 1: 'winter', 2: 'winter',
                               3: 'spring', 4: 'spring', 5: 'spring',
                               6: 'summer', 7: 'summer', 8: 'summer'}
                    season = seasons.get(month, 'fall')
                else:
                    season = None

                all_dataframe_data.append({
                    'date': date,
                    'lat': data_json.get('location', {}).get('lat'),
                    'lng': data_json.get('location', {}).get('lng'),
                    'elevation': data_json.get('elevation'),
                    'mean_temp': data_json.get('mean_temp'),
                    'pano_id': data_json.get('pano_id'),
                    'season': season,
                    'photo_s3_path': obj.object_name.replace('data.json', 'street_view_image.jpg')
                })
            except Exception as e:
                print(f"Ошибка обработки {obj.object_name}: {e}")
            finally:
                response.close()

        if not all_dataframe_data:
            print("Новых данных для сборки не найдено.")
            return

        df = pd.DataFrame(all_dataframe_data)

        df['lat'] = df['lat'].astype(str)
        df['lng'] = df['lng'].astype(str)
        df['elevation'] = df['lat'].astype(str)
        df['mean_temp'] = df['lng'].astype(str)

        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
        parquet_buffer.seek(0)

        analytics_key = f"analytics/metadata_{datetime.now().strftime('%Y%m%d')}.parquet"
        client.put_object(
            bucket_name=bucket_name,
            object_name=analytics_key,
            data=parquet_buffer,
            length=parquet_buffer.getbuffer().nbytes,
            content_type='application/octet-stream'
        )
        print(f"Успешно сохранен датасет: {analytics_key}")


    compaction_task = aggregate_metadata_to_parquet()


    trigger_tasks >> compaction_task