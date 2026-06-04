import os
import csv
import random
import pandas as pd
from sklearn.preprocessing import StandardScaler
from PIL import Image


PHOTO_DIR = r"/home/dmtuser/MyPetProjects/season_pipline/photo"
CSV_PATH = r"/home/dmtuser/MyPetProjects/season_pipline/eda_files/metadata.csv"
OUTPUT_DIR = r"/home/dmtuser/MyPetProjects/season_pipline/PyScripts"

TRAIN_CSV = os.path.join(OUTPUT_DIR, "train.csv")
TEST_CSV = os.path.join(OUTPUT_DIR, "test.csv")

NUMERIC_COLS = ["lat", "lng", "elevation", "mean_temp"]

RANDOM_SEED  = 42
TRAIN_RATIO  = 0.8


df = pd.read_csv(CSV_PATH)
print(f"Загружено строк в CSV: {len(df)}")
print(f"Колонки: {df.columns.tolist()}")

# Убираем лишние пробелы в названиях колонок
df.columns = df.columns.str.strip()


df["latxlngxdate"] = (
    df["lat"].astype(str) + "x" +
    df["lng"].astype(str) + "x" +
    df["date"].astype(str)
)


photo_map = {}

missing_photo   = []
found_photo     = []

for folder_name in os.listdir(PHOTO_DIR):
    folder_path = os.path.join(PHOTO_DIR, folder_name)
    if not os.path.isdir(folder_path):
        continue

    parts = folder_name.split("x")
    if len(parts) < 3:
        continue
    key = f"{parts[0]}x{parts[1]}x{parts[2]}"

    # Ищем street_view_image внутри папки
    found = None
    for file_name in os.listdir(folder_path):
        if file_name.startswith("street_view_image"):
            full_path = os.path.join(folder_path, file_name)
            # Проверяем что файл реально существует и не пустой
            if os.path.isfile(full_path) and os.path.getsize(full_path) > 0:
                found = full_path
                break

    if found:
        try:
            with Image.open(found) as img:
                img.verify()
            photo_map[key] = found
            found_photo.append(folder_name)
        except Exception:
            missing_photo.append(folder_name)
    else:
        missing_photo.append(folder_name)

print(f"Найдено фоток (файл существует): {len(found_photo)}")
print(f"Папок без фото (пропускаем):     {len(missing_photo)}")
if missing_photo:
    print("  Примеры папок без фото:")
    for name in missing_photo[:5]:
        print(f"    {name}")


df["image_path"] = df["latxlngxdate"].map(photo_map)

matched = df["image_path"].notna().sum()
print(f"Совпавших строк (CSV ↔ фото): {matched} из {len(df)}")

df = df[df["image_path"].notna()].reset_index(drop=True)


if "season" in df.columns:
    unique_seasons = sorted(df["season"].dropna().unique())
    season_map = {season: idx for idx, season in enumerate(unique_seasons)}

    df["season"] = df["season"].map(season_map)

    # Сохраняем маппинг в отдельный файл
    season_legend_path = os.path.join(OUTPUT_DIR, "season_encoding.csv")
    with open(season_legend_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["code", "season"])
        for season, code in season_map.items():
            writer.writerow([code, season])

    print(f"Маппинг сезонов:")
    for season, code in season_map.items():
        print(f"  {code} → {season}")
    print(f"Сохранён в: {season_legend_path}")


df = df.sample(frac=1, random_state=RANDOM_SEED).reset_index(drop=True)

split_idx  = int(len(df) * TRAIN_RATIO)
train_df   = df.iloc[:split_idx]
test_df    = df.iloc[split_idx:]

print(f"Train: {len(train_df)} строк | Test: {len(test_df)} строк")


train_df.to_csv(TRAIN_CSV, index=False, encoding="utf-8")
test_df.to_csv(TEST_CSV,  index=False, encoding="utf-8")

print(f"\nГотово!")
print(f"  Train → {TRAIN_CSV}")
print(f"  Test  → {TEST_CSV}")


print("\nПервые 3 строки train:")
print(train_df[["latxlngxdate", "image_path", "mean_temp", "elevation", "lat", "lng"]].head(3).to_string())
