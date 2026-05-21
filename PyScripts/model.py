import pandas as pd
import torch
import torch.nn as nn
from torchvision import models
import numpy as np
import torch.utils.data as data
import torchvision.transforms.v2 as tfs
from torchvision import transforms
import torch.optim
from PIL import Image


class SeasonsDataset(data.Dataset):
    def __init__(self, dff):
        self.df = dff
        self.transform = transforms.Compose([
            transforms.Resize((224, 224)),
            transforms.ToTensor(),
            transforms.Normalize([0.485, 0.456, 0.406],
                                 [0.229, 0.224, 0.225]),
        ])

    def __len__(self):
        return len(self.df)

    def __getitem__(self, idx):
        row = self.df.iloc[idx]
        image = Image.open(row['image_path']).convert('RGB')
        image = self.transform(image)
        features = torch.tensor([row['lat_norm'], row['lng_norm'], row['elevation_norm'], row['mean_temp']],
                                dtype=torch.float32)
        target = torch.tensor(row['label'], dtype=torch.long)
        return (image, features), target


class SeasnonNN(nn.Module):
    def __init__(self, num_class=4):
        super().__init__()
        self.resnet = models.resnet50(weights=models.ResNet50_Weights.DEFAULT)
        for par in self.resnet.parameters():
            par.requires_grad = False
        for param in self.resnet.layer4.parameters():
            param.requires_grad = True
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


df = pd.read_csv(r"C:\Users\Gaming PC\pythonproject\dataset\train.csv", sep=',')
newdf = df[['image_path', 'label', 'lat_norm', 'lng_norm', 'elevation_norm', 'mean_temp']].copy()
newdf['mean_temp'] = (newdf['mean_temp'] - newdf['mean_temp'].mean()) / newdf['mean_temp'].std()
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
train = SeasonsDataset(newdf)
train_loader = data.DataLoader(train, batch_size=16, shuffle=True)
model = SeasnonNN(num_class=4).to(device)
loss = nn.CrossEntropyLoss()
optimizer = torch.optim.Adam(model.parameters(), lr=0.0001)
for epoch in range(21):
    model.train()
    train_loss = 0
    if epoch % 5 == 0:
        print(f"Epoch {epoch} | loss: {train_loss / len(train_loader):.4f}")
    for (image, features), labels in train_loader:
        image = image.to(device)
        features = features.to(device)
        labels = labels.to(device)
        optimizer.zero_grad()
        result = model(image, features)
        res_loss = loss(result, labels)
        res_loss.backward()
        optimizer.step()
        train_loss += res_loss.item()

torch.save(model.state_dict(), 'season_model2.pth')
