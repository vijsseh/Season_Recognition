import os
import json
import pandas as pd
import matplotlib.pyplot as plt
import folium
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


import io
import requests


def collect_json_data_from_minio():
    all_dataframe_data = []

    from collections import defaultdict
    folders_content = defaultdict(lambda: {"has_json": False, "has_jpg": False})

    try:
        objects = client.list_objects(bucket_name, recursive=True)

        for obj in objects:
            if '/' in obj.object_name:
                folder_path = obj.object_name.rsplit('/', 1)[0]
            else:
                folder_path = ""

            name_lower = obj.object_name.lower()


            if name_lower.endswith('data.json'):
                folders_content[folder_path]["has_json"] = True

                response = client.get_object(bucket_name, obj.object_name)
                try:
                    data_json = json.load(response)

                    date = data_json.get('date')
                    lat = data_json.get('location', {}).get('lat')
                    lng = data_json.get('location', {}).get('lng')
                    elevation = data_json.get('elevation')
                    mean_temp = data_json.get('mean_temp')
                    pano_id = data_json.get('pano_id', None)

                    if date:
                        year, month = date.split('-')
                        month = int(month)
                        season = 'winter' if month in [1, 2, 12] else 'spring' if month in [3, 4,
                                                                                            5] else 'summer' if month in [
                            6, 7, 8] else 'fall'
                    else:
                        season = None

                    all_dataframe_data.append({
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
                finally:
                    response.close()
            elif name_lower.endswith('.jpg') or name_lower.endswith('.jpg'):
                folders_content[folder_path]["has_jpg"] = True

                path = os.path.join('photo', obj.object_name)
                local_dir = os.path.dirname(path)

                path = 'photo' + '/' + obj.object_name
                try:
                    os.mkdir('photo' + '/' + obj.object_name.split('/')[0])
                    client.fget_object(bucket_name, obj.object_name, path)
                except FileExistsError:
                    print()

    except S3Error as err:
        print(f"Ошибка при получении объектов: {err}")

    target_folders_count = 0
    for folder, content in folders_content.items():
        if content["has_jpg"] and not content["has_json"]:
            # if 'x' not in folder:
            #     continue
            #
            # try:
            #     lat, lng, year_month = folder.split('x')
            # except ValueError:
            #     print(f"Имя папки '{folder}' не соответствует формату 'latxlngxdate'")
            #     continue
            #
            # key = f"{lat}x{lng}x{year_month}/data.json"
            # url = f'https://archive-api.open-meteo.com/v1/archive?latitude={lat}&longitude={lng}&start_date={year_month}-01&end_date={year_month}-28&daily=temperature_2m_mean&timezone=auto'
            #
            # api_data = {
            #     'location': {'lat': lat, 'lng': lng},
            #     'date': year_month,
            # }
            # try:
            #     response = requests.get(url, timeout=(3.05, 3))
            # except Exception:
            #     print(Exception)
            # if response.status_code == 200:
            #     res_json = response.json()
            #     print(f'GOT HTTP RESPONSE FROM API: {response.status_code}')
            #
            #     if 'error' not in res_json:
            #         res_daily = res_json.get('daily', {})
            #         temps = res_daily.get('temperature_2m_mean', [])
            #
            #         if temps:
            #             api_data['elevation'] = res_json.get('elevation')
            #             api_data['units_temp'] = res_json.get('daily_units', {}).get('temperature_2m_mean')
            #             api_data['mean_temp'] = sum(temps) / len(temps)
            #
            #             try:
            #                 month = int(year_month.split('-')[1])
            #                 api_data['season'] = 'winter' if month in [1, 2, 12] else 'spring' if month in [3, 4,
            #                                                                                                 5] else 'summer' if month in [
            #                     6, 7, 8] else 'fall'
            #             except Exception:
            #                 api_data['season'] = None
            #             edited_data = json.dumps(api_data).encode('utf-8')
            #             data_stream = io.BytesIO(edited_data)
            #
            #             # Официальный метод загрузки данных из памяти в MinIO
            #             client.put_object(
            #                 bucket_name=bucket_name,
            #                 object_name=key,
            #                 data=data_stream,
            #                 length=len(edited_data),
            #                 content_type='application/json'
            #             )
            #             print(f'UPLOAD JSON TO MINIO: {key}')
            #
            #             # Также добавляем эти новые данные в общий список для DataFrame
            #             all_dataframe_data.append({
            #                 'date': api_data['date'],
            #                 'lat': lat,
            #                 'lng': lng,
            #                 'elevation': api_data.get('elevation'),
            #                 'mean_temp': api_data.get('mean_temp'),
            #                 'pano_id': None,
            #                 'season': api_data['season']
            #             })
            #     else:
            #         print(f"API returned error: {res_json['error']}")
            # else:
            #     print(f'API responded with status code: {response.status_code}')

            target_folders_count += 1

    print("\n" + "=" * 50)
    print(f"Анализ структуры:")
    print(f"Кол-во папок, где ЕСТЬ (.jpg), но НЕТ (data.json): {target_folders_count}")
    print("=" * 50 + "\n")

    df = pd.DataFrame(all_dataframe_data)
    return df


# directory = "/home/dmitriy/PycharmProjects/season_pipline/s3_storage/highresolutionseasons"
objects = client.list_objects(bucket_name)

df = collect_json_data_from_minio()
# df['lat'] = df['lat'].astype('float16')
# df['lng'] = df['lng'].astype('float16')
month_counts = df['season'].value_counts(normalize=True)
print(df['season'].value_counts())
month_counts = month_counts.sort_index()

# print(df['mean_temp'].mean())
# print(df['mean_temp'].std())
# print(df['lat'].mean())
# print(df['lat'].std())
# print(df['lng'].mean())
# print(df['lng'].std())
# print(df['elevation'].mean())
# print(df['elevation'].std())

month_counts.plot.pie(autopct='%1.1f%%', figsize=(5, 5), startangle=90)
plt.title('Процент записей по сезонам')
plt.ylabel('')  # Убираем метку оси Y
plt.savefig('eda_files/month_percentage_pie_chart.png', bbox_inches='tight')


df.to_csv("eda_files/metadata.csv", index=False)  # Можно сохранить в CSV файл

def make_html():
    for season in df['season'].unique():
        season_df = df[df['season'] == season]
        m = folium.Map(location=[0,0], zoom_start=5)

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
                    location=[float(row['lat']), float(row['lng'])],
                    popup=folium.Popup(popup_content),
                ).add_to(m)
        m.save(f'eda_files/{season}_locations_map.html')

# make_html()