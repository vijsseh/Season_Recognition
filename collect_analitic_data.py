import os
import json
import pandas as pd
import matplotlib.pyplot as plt
import folium
from minio import Minio
from minio.error import S3Error
import base64

client = Minio(
    "localhost:9002",  # Адрес вашего MinIO
    access_key="admin",
    secret_key="admin123",
    secure=False  # Если MinIO не использует HTTPS
)
bucket_name = 'highresolutionseasons'


def image_to_base64(photo_data):
    try:
        return base64.b64encode(photo_data).decode('utf-8')
    except Exception as e:
        print(f"Ошибка при чтении изображения: {e}")
        return None
# Функция для обхода директорий и сбора данных из файла data.json

def collect_json_data_from_minio():
    data = []  # Список для хранения данных

    try:
        # Список объектов в бакете
        objects = client.list_objects(bucket_name, recursive=True)
        print(objects)

        for obj in objects:
            # Проверяем, если это файл 'data.json'
            if obj.object_name.endswith('data.json'):
                # Получаем объект (файл) из MinIO
                response = client.get_object(bucket_name, obj.object_name)

                try:
                    # Читаем и загружаем JSON из файла
                    data_json = json.load(response)

                    # Извлекаем нужные поля
                    date = data_json.get('date')
                    lat = data_json.get('location', {}).get('lat')
                    lng = data_json.get('location', {}).get('lng')
                    pano_id = data_json.get('pano_id')

                    # Добавляем данные в список
                    if date:
                        year, month = date.split('-')  # Разделяем на год и месяц
                    else:
                        year, month = None, None  # Если дата отсутствует, присваиваем None

                    month = int(month)
                    season = 'winter' if month in [1, 2, 12] else 'spring' if month in [3, 4, 5] else 'summer' if month in [6, 7, 8] else 'fall'

                    # Добавляем данные в список
                    data.append({
                        'date': date,
                        'lat': lat,
                        'lng': lng,
                        'pano_id': pano_id,
                        'season': season,
                    })
                except json.JSONDecodeError:
                    print(f"Ошибка чтения JSON файла: {obj.object_name}")
    except S3Error as err:
        print(f"Ошибка при получении объектов: {err}")

    # Создаем DataFrame из собранных данных
    df = pd.DataFrame(data)
    # print(df)
    return df

# Указываем директорию для поиска файлов data.json
# directory = "/home/dmitriy/PycharmProjects/season_pipline/s3_storage/highresolutionseasons"  # Замените на путь к вашей директории
objects = client.list_objects(bucket_name)

# Собираем данные в DataFrame
df = collect_json_data_from_minio()

# Печатаем итоговый DataFrame
month_counts = df['season'].value_counts(normalize=True)
print(df['season'].value_counts())
month_counts = month_counts.sort_index()

month_counts.plot.pie(autopct='%1.1f%%', figsize=(5, 5), startangle=90)
plt.title('Процент записей по сезонам')
plt.ylabel('')  # Убираем метку оси Y
plt.savefig('eda_files/month_percentage_pie_chart.png', bbox_inches='tight')


# Сохраняем DataFrame в файл (опционально)
df.to_csv("eda_files/metadata.csv", index=False)  # Можно сохранить в CSV файл

def make_html():
    map_center = [df['lat'].mean(), df['lng'].mean()]  # Центрируем карту на среднем значении lat и lng
    m = folium.Map(location=map_center, zoom_start=5)

    # Добавляем маркеры на карту для каждого расположения
    for season in df['season'].unique():
        # Фильтруем данные по сезону
        season_df = df[df['season'] == season]

        # Создаем карту с центральной точкой (например, в центре координат сезона)
        map_center = [season_df['lat'].mean(), season_df['lng'].mean()]  # Центрируем карту на среднем значении lat и lng
        m = folium.Map(location=map_center, zoom_start=5)


        # Добавляем маркеры на карту для каждого расположения в этом сезоне
        for _, row in season_df.iterrows():
            photo_path = client.get_object(bucket_name, f"{row['pano_id']}/street_view_image.jpg")
            photo_data = photo_path.read()
            photo_base64 = image_to_base64(photo_data)

            popup_content = f"""
                        <strong>Pano ID:</strong> {row['pano_id']}<br>
                        <strong>Date:</strong> {row['date']}<br>
                        <img src="data:image/jpeg;base64,{photo_base64}"  width="300" height="300">
                        """

            if row['lat'] and row['lng']:
                folium.Marker(
                    location=[row['lat'], row['lng']],
                    popup=folium.Popup(popup_content),
                ).add_to(m)
        m.save(f'eda_files/{season}_locations_map.html')

make_html()