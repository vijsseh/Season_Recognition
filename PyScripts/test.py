import os
import pandas as pd


def check_missing_photo_folders(csv_path, photo_base_dir='photo'):
    # 1. Проверяем, существует ли базовая папка photo
    if not os.path.exists(photo_base_dir):
        print(f"Ошибка: Директория '{photo_base_dir}' не найдена!")
        return

    # 2. Читаем CSV файл
    try:
        # Предполагаем, что колонка с ключом называется 'folder_key' или 'key'.
        # Если название другое, замените 'key' на ваше имя колонки.
        df = pd.read_csv(csv_path)

        # Если у вас нет отдельной колонки, а есть lat, lng, date, то можно собрать ключ так:
        df['key'] = df['lat'].astype(str) + 'x' + df['lng'].astype(str) + 'x' + df['date'].astype(str)

        if 'key' not in df.columns:
            # Если колонки 'key' нет, попробуем взять первую колонку файла
            column_name = df.columns[0]
            print(f"Предупреждение: Колонка 'key' не найдена. Используем первую колонку: '{column_name}'")
        else:
            column_name = 'key'

        # Получаем список уникальных ключей из CSV (очищая от пробелов)
        csv_keys = set(df[column_name].dropna().astype(str).str.strip())

    except Exception as e:
        print(f"Ошибка при чтении CSV файла: {e}")
        return

    # 3. Получаем список существующих папок в директории photo
    # Фильтруем, оставляя только директории (игнорируем файлы, если они там есть)
    existing_folders = {
        name for name in os.listdir(photo_base_dir)
        if os.path.isdir(os.path.join(photo_base_dir, name))
    }

    # 4. Сравниваем множества
    found_keys = csv_keys.intersection(existing_folders)
    missing_keys = csv_keys - existing_folders
    extra_folders = existing_folders - csv_keys

    # 5. Выводим красивый отчет
    print("\n" + "=" * 60)
    print(f"РЕЗУЛЬТАТЫ ПРОВЕРКИ СТРУКТУРЫ ПАПОК")
    print("=" * 60)
    print(f"Всего уникальных ключей в CSV:      {len(csv_keys)}")
    print(f"Найдено папок из CSV на диске:      {len(found_keys)}  ({len(found_keys) / len(csv_keys) * 100:.1f}%)")
    print(f"Отсутствует папок на диске:         {len(missing_keys)}")

    if extra_folders:
        print(f"Лишних папок на диске (нет в CSV): {len(extra_folders)}")

    print("-" * 60)

    # Показываем примеры отсутствующих папок (до 10 штук), чтобы не спамить консоль
    if missing_keys:
        print("Примеры отсутствующих папок на диске:")
        for i, missing in enumerate(sorted(missing_keys)):
            if i >= 10:
                print(f" ... и еще {len(missing_keys) - 10} папок.")
                break
            print(f"  - {photo_base_dir}/{missing}")

    print("=" * 60 + "\n")

    return list(missing_keys)

# --- Пример запуска ---
# Замени 'data.csv' на путь к твоему CSV файлу

missing = check_missing_photo_folders('/home/dmtuser/MyPetProjects/season_pipline/eda_files/metadata.csv', photo_base_dir='/home/dmtuser/MyPetProjects/season_pipline/photo')
# 1. Пути к файлам
csv_path = '/home/dmtuser/MyPetProjects/season_pipline/eda_files/metadata.csv'
photo_dir = '/home/dmtuser/MyPetProjects/season_pipline/photo'
output_csv_path = '/home/dmtuser/MyPetProjects/season_pipline/eda_files/missing_metadata.csv'

# 2. Получаем список ОТСУТСТВУЮЩИХ папок из твоей функции
missing_keys_list = check_missing_photo_folders(csv_path, photo_base_dir=photo_dir)
missing_keys_set = set(missing_keys_list)

if missing_keys_set:
    # 3. Читаем исходный файл
    df_original = pd.read_csv(csv_path)

    # Создаем временный ключ для фильтрации
    df_original['key'] = (
            df_original['lat'].astype(str) + 'x' +
            df_original['lng'].astype(str) + 'x' +
            df_original['date'].astype(str)
    )

    # 4. Фильтруем: оставляем ТОЛЬКО строки, которых НЕТ на диске
    # (найденные строки здесь полностью отсекаются)
    filtered_df = df_original[~df_original['key'].isin(missing_keys_set)].copy()

    # Удаляем временный ключ, возвращая исходную структуру колонок
    filtered_df.drop(columns=['key'], inplace=True)

    # 5. Сохраняем результат
    filtered_df.to_csv(output_csv_path, index=False)

    print(f"Готово! Из файла удалены все найденные позиции.")
    print(f"В новом файле осталось {len(filtered_df)} строк (было {len(df_original)}).")
    print(f"Сохранено в: {output_csv_path}")
else:
    print("Все папки найдены на диске. Нет смысла сохранять пустой файл.")