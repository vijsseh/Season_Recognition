import asyncio
import logging
import os
import torch
import torch.nn as nn
from torchvision import models
import torchvision.transforms as transforms
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message
from PIL import Image


TOKEN = "8633413200:AAGqiHLbwT94svdhgkL-ftkCIvJ2tuIaT2I"
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

bot = Bot(token=TOKEN)
dp = Dispatcher()
logging.basicConfig(level=logging.INFO)

# КОНСТАНТЫ ДЛЯ МАСШТАБИРОВАНИЯ ТЕМПЕРАТУРЫ
# Замените эти значения на среднее (mean) и отклонение (std) из вашего TRAIN датасета!
MEAN_TEMP_TRAIN = 15.318994134972797  # Пример значения
STD_TEMP_TRAIN = 9.459355684811385  # Пример значения


# 2. Архитектура вашей нейросети
class SeasnonNN(nn.Module):
    def __init__(self, num_class=4):
        super().__init__()
        self.resnet = models.resnet50(weights=None)  # Веса загрузим из файла
        self.resnet.fc = nn.Identity()

        self.tableNN = nn.Sequential(
            nn.Linear(4, 16),
            nn.BatchNorm1d(16),
            nn.ReLU(),
            nn.Linear(16, 32),
            nn.ReLU()
        )
        self.finclassifier = nn.Sequential(
            nn.Linear(2048 + 32, 256),
            nn.ReLU(),
            nn.Dropout(0.4),
            nn.Linear(256, 4)
        )

    def forward(self, image, numeric_data):
        output_resnet = self.resnet(image)
        output_tableNN = self.tableNN(numeric_data)
        concat = torch.cat((output_resnet, output_tableNN), dim=1)
        p = self.finclassifier(concat)
        return p


# 3. Инициализация и загрузка модели при старте бота
model = SeasnonNN(num_class=4)

PATH_TO_WEIGHTS = "season_model2.pth"

if os.path.exists(PATH_TO_WEIGHTS):
    model.load_state_dict(torch.load(PATH_TO_WEIGHTS, map_location=device))
    model = model.to(device)
    model.eval()
    logging.info("Модель успешно загружена!")
else:
    logging.warning(f"Файл весов {PATH_TO_WEIGHTS} не найден! Бот будет выдавать ошибку при обработке.")

img_transform = transforms.Compose([
    transforms.Resize((224, 224)),
    transforms.ToTensor(),
    transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225]),
])

CLASS_NAMES = ["Зима", "Весна", "Лето", "Осень"]


# Функция инференса (предсказания)
def predict_season(image_path: str, raw_features: list) -> str:
    try:
        img = Image.open(image_path).convert('RGB')
        img_tensor = img_transform(img).unsqueeze(0).to(device)  # Добавляем размер батча [1, 3, 224, 224]

        lat, lng, elevation, temp = raw_features

        temp_norm = (temp - MEAN_TEMP_TRAIN) / STD_TEMP_TRAIN

        features = torch.tensor([[lat, lng, elevation, temp_norm]], dtype=torch.float32).to(device)

        with torch.no_grad():
            logits = model(img_tensor, features)
            predicted_idx = torch.argmax(logits, dim=1).item()

        # Возвращаем красивый ответ
        return f"Прогноз модели: {CLASS_NAMES[predicted_idx]} (Класс {predicted_idx})"

    except Exception as e:
        return f"Ошибка внутри модели: {e}"


# Обработка команды /start
@dp.message(F.text == "/start")
async def cmd_start(message: Message):
    await message.answer(
        "Привет! Я мультимодальная нейросеть.\n\n"
        "Отправь мне **фотографию**, а в **описании (подписи) к ней** "
        "укажи через запятую 4 числа: \n"
        "`lat_norm, lng_norm, elevation_norm, mean_temp`\n\n"
        "Пример подписи: `0.54, -0.12, 0.3, 18.5`"
    )


# Обработка входящих фотографий
@dp.message(F.photo)
async def handle_photo(message: Message):
    # Проверяем, передал ли пользователь текстовые фичи в описании к фото
    if not message.caption:
        await message.reply(
            "Пожалуйста, отправьте фото ПОВТОРНО и добавьте к нему описание с параметрами: `lat, lng, elevation, temp`")
        return

    try:
        raw_features = [float(x.strip()) for x in message.caption.replace(',', ' ').split()]

        if len(raw_features) != 4:
            await message.reply(
                "Ошибка: в описании должно быть ровно 4 числа через запятую или пробел!\nПример: `0.5, -0.2, 0.1, 15.5`")
            return
    except ValueError:
        await message.reply(
            "Не удалось распознать числа в описании. Убедитесь, что там только цифры и точки. Пример: `0.5, -0.2, 0.1, 15.5`")
        return

    photo = message.photo[-1]
    await message.answer("Получил данные. Модель обрабатывает запрос...")

    local_filename = f"temp_{photo.file_id}.jpg"

    try:
        file_info = await bot.get_file(photo.file_id)
        await bot.download_file(file_info.file_path, destination=local_filename)

        ml_result = await asyncio.to_thread(predict_season, local_filename, raw_features)

        await message.reply(f"Результат анализа:\n\n{ml_result}")

    except Exception as e:
        logging.error(f"Ошибка при обработке: {e}")
        await message.answer("Произошла ошибка при загрузке или обработке изображения.")

    finally:
        if os.path.exists(local_filename):
            os.remove(local_filename)


async def main():
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())