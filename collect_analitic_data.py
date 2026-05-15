import os
import json

import pandas as pd
import matplotlib.pyplot as plt
import folium
from folium.plugins import HeatMap
from minio import Minio
from minio.error import S3Error
import base64
from dotenv import load_dotenv

load_dotenv()
ACCESS_KEY = os.getenv('MINIO_ROOT_USER')
SECRET_KEY = os.getenv('MINIO_ROOT_PASSWORD')

client = Minio(
    "localhost:9002",
    access_key=ACCESS_KEY,
    secret_key=SECRET_KEY,
    secure=False
)
bucket_name = 'testseasons'


def image_to_base64(photo_data):
    try:
        return base64.b64encode(photo_data).decode('utf-8')
    except Exception as e:
        print(f"Ошибка при чтении изображения: {e}")
        return None


def collect_json_data_from_minio():
    data = []

    try:

        objects = client.list_objects(bucket_name, recursive=True)
        print(objects)

        for obj in objects:

            if obj.object_name.endswith('data.json'):
                response = client.get_object(bucket_name, obj.object_name)

                try:

                    data_json = json.load(response)

                    date = data_json.get('date')
                    lat = data_json.get('location', {}).get('lat')
                    lng = data_json.get('location', {}).get('lng')
                    elevation = data_json.get('elevation')
                    mean_temp = data_json.get('mean_temp')
                    pano_id = data_json.get('pano_id')

                    if date:
                        year, month = date.split('-')
                    else:
                        year, month = None, None

                    month = int(month)
                    season = 'winter' if month in [1, 2, 12] else 'spring' if month in [3, 4, 5] else 'summer' if month in [6, 7, 8] else 'fall'

                    data.append({
                        'date': date,
                        'lat': lat,
                        'lng': lng,
                        'elevation': elevation,
                        'mean_temp': mean_temp,
                        'pano_id': pano_id,
                        'season': season,
                    })
                except json.JSONDecodeError:
                    print(f"Ошибка чтения JSON файла: {obj.object_name}")
            elif obj.object_name.endswith('.jpg'):
                path = 'photo' + '/' + obj.object_name
                try:
                    os.mkdir('photo' + '/' + obj.object_name.split('/')[0])
                    client.fget_object(bucket_name, obj.object_name, path)
                except FileExistsError:
                    pass

    except S3Error as err:
        print(f"Ошибка при получении объектов: {err}")


    df = pd.DataFrame(data)
    # print(df)
    return df


# directory = "/home/dmitriy/PycharmProjects/season_pipline/s3_storage/highresolutionseasons"
objects = client.list_objects(bucket_name)

df = collect_json_data_from_minio()

month_counts = df['season'].value_counts(normalize=True)
print(df['season'].value_counts())
month_counts = month_counts.sort_index()

month_counts.plot.pie(autopct='%1.1f%%', figsize=(5, 5), startangle=90)
plt.title('Процент записей по сезонам')
plt.ylabel('')  # Убираем метку оси Y
plt.savefig('eda_files/month_percentage_pie_chart.png', bbox_inches='tight')


df.to_csv("eda_files/metadata.csv", index=False)  # Можно сохранить в CSV файл

def make_html():
    map_center = [df['lat'].mean(), df['lng'].mean()]  # Центрируем карту на среднем значении lat и lng
    m = folium.Map(location=map_center, zoom_start=5)

    for season in df['season'].unique():
        season_df = df[df['season'] == season]
        map_center = [season_df['lat'].mean(), season_df['lng'].mean()]
        m = folium.Map(location=map_center, zoom_start=5)

        for _, row in season_df.iterrows():
            try:
                photo_path = client.get_object(bucket_name, f"{row['pano_id']}/street_view_image.jpg")
            except S3Error:
                local = str(row['lat']) + 'x' + str(row['lng']) + 'x' + str(row['date'])
                photo_path = client.get_object(bucket_name, f"{local}/street_view_image.jpg")
            photo_data = photo_path.read()
            photo_base64 = image_to_base64(photo_data)

            popup_content = f"""
                        <strong>Pano ID:</strong> {row['pano_id']}<br>
                        <strong>Date:</strong> {row['date']}<br>
                        <strong>Mean Temperature:</strong> {row['mean_temp']}<br>
                        <img src="data:image/jpeg;base64,{photo_base64}"  width="300" height="300">
                        """

            if row['lat'] and row['lng']:
                folium.Marker(
                    location=[row['lat'], row['lng']],
                    popup=folium.Popup(popup_content),
                ).add_to(m)
        m.save(f'eda_files/{season}_locations_map.html')

make_html()