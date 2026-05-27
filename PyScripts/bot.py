import asyncio
import io
import torch
import torch.nn as nn
from torchvision import models, transforms
from PIL import Image
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup


class SeasnonNN(nn.Module):
    def __init__(self, num_class=4):
        super().__init__()
        self.resnet = models.resnet50(weights=models.ResNet50_Weights.DEFAULT)
        for par in self.resnet.parameters():
            par.requires_grad = False
        for param in self.resnet.layer4.parameters():
            param.requires_grad = True
        self.resnet.fc = nn.Identity()

        # Обновленная архитектура табличной части
        self.tableNN = nn.Sequential(
            nn.Linear(4, 32),
            nn.BatchNorm1d(32),
            nn.ReLU(),
            nn.Linear(32, 64),
            nn.BatchNorm1d(64),
            nn.ReLU(),
            nn.Linear(64, 64),
            nn.ReLU()
        )

        self.finclassifier = nn.Sequential(
            nn.Linear(2048 + 64, 256),
            nn.ReLU(),
            nn.Dropout(0.5),
            nn.Linear(256, 4)
        )

    def forward(self, image, numeric_data):
        output_resnet = self.resnet(image)
        output_tableNN = self.tableNN(numeric_data)
        concat = torch.cat((output_resnet, output_tableNN), dim=1)
        p = self.finclassifier(concat)
        return p



BOT_TOKEN = "8633413200:AAGqiHLbwT94svdhgkL-ftkCIvJ2tuIaT2I"  # Твой токен

# 🛑 НОВЫЕ ДАННЫЕ ДЛЯ НОРМАЛИЗАЦИИ
TEMP_MEAN = 15.024931328762243
TEMP_STD = 9.508946785361216

LAT_MEAN = 44.64154238191048
LAT_STD = 13.630374623501336

LNG_MEAN = 49.20748828978892
LNG_STD = 47.90025076135358

ELEVATION_MEAN = 601.3890909090909
ELEVATION_STD = 873.2692353925235

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()


device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
model = SeasnonNN(num_class=4).to(device)

model.load_state_dict(torch.load('finalMODEL2.pth', map_location=device))

model.eval()

image_transform = transforms.Compose([
    transforms.Resize((224, 224)),
    transforms.ToTensor(),
    transforms.Normalize([0.485, 0.456, 0.406],
                         [0.229, 0.224, 0.225]),
])

# Классы (0, 1, 2, 3)
CLASS_NAMES = {
    0: "Зима ❄️",
    1: "Весна 🌸",
    2: "Лето ☀️",
    3: "Осень 🍁"
}


class PredictionState(StatesGroup):
    waiting_for_image = State()
    waiting_for_features = State()


@dp.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    await message.answer(
        "Привет! Я бот для классификации сезонов.\n"
        "Пожалуйста, отправь мне **фотографию**."
    )
    await state.set_state(PredictionState.waiting_for_image)


@dp.message(PredictionState.waiting_for_image, F.photo)
async def handle_photo(message: Message, state: FSMContext):
    photo = message.photo[-1]
    photo_bytes = io.BytesIO()
    await bot.download(photo, destination=photo_bytes)

    await state.update_data(image_bytes=photo_bytes.getvalue())

    # Просим РЕАЛЬНЫЕ данные
    await message.answer(
        "Фото получено! Теперь отправь 4 числа через пробел (реальные значения):\n"
        "`широта долгота высота(м) температура(°C)`\n\n"
        "*(Например, координаты Москвы:)* `55.75 37.61 150 12.5`",
        parse_mode="Markdown"
    )
    await state.set_state(PredictionState.waiting_for_features)


@dp.message(PredictionState.waiting_for_features, F.text)
async def handle_features(message: Message, state: FSMContext):
    try:
        numbers = list(map(float, message.text.replace(',', '.').split()))
        if len(numbers) != 4:
            raise ValueError

        raw_lat, raw_lng, raw_elevation, raw_temp = numbers


        norm_lat = (raw_lat - LAT_MEAN) / LAT_STD
        norm_lng = (raw_lng - LNG_MEAN) / LNG_STD
        norm_elevation = (raw_elevation - ELEVATION_MEAN) / ELEVATION_STD
        norm_temp = (raw_temp - TEMP_MEAN) / TEMP_STD

        user_data = await state.get_data()
        image_bytes = user_data['image_bytes']

        # Готовим картинку
        image = Image.open(io.BytesIO(image_bytes)).convert('RGB')
        image_tensor = image_transform(image).unsqueeze(0).to(device)

        # Готовим признаки: передаем в тензор уже НОРМАЛИЗОВАННЫЕ значения
        features_tensor = torch.tensor([[norm_lat, norm_lng, norm_elevation, norm_temp]], dtype=torch.float32).to(
            device)

        # Делаем предсказание
        with torch.no_grad():
            output = model(image_tensor, features_tensor)
            predicted_class = torch.argmax(output, dim=1).item()

        result_text = CLASS_NAMES.get(predicted_class, f"Неизвестный класс: {predicted_class}")

        await message.answer(
            f"🧠 Нейросеть предсказывает: **{result_text}**\n\n"
            f"*(Нормализованные данные для модели: lat={norm_lat:.2f}, lng={norm_lng:.2f}, elev={norm_elevation:.2f}, temp={norm_temp:.2f})*",
            parse_mode="Markdown"
        )

        await message.answer("Отправь новую фотографию, если хочешь повторить.")
        await state.set_state(PredictionState.waiting_for_image)

    except ValueError:
        await message.answer(
            "❌ Ошибка формата. Пожалуйста, введи ровно 4 числа через пробел (можно использовать точки или минусы).\n"
            "Пример: `55.75 37.61 150 12.5`", parse_mode="Markdown"
        )


async def main():
    print("Бот запущен и готов к работе!")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())