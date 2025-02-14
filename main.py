import pandas as pd
import json
import os
from pathlib import Path
from collections import defaultdict


def read_json(file_path):
    """Считывает JSON из файла."""
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            return json.load(file)
    except Exception as e:
        print(f"Ошибка при чтении файла {file_path}: {e}")
        return None


def collect_data_from_directory(base_dir):
    """
    Рекурсивно обходит директорию, собирая пути к JSON файлам
    и структурируя данные по продуктам Google.
    """
    export_data = defaultdict(list)
    base_path = Path(base_dir)

    for root, _, files in os.walk(base_path):
        for file in files:
            if file.endswith("МоиДействия.json"):
                product = Path(root).name  # Имя продукта из пути
                file_path = os.path.join(root, file)
                export_data[product].append(file_path)

    return export_data


def merge_json_data(file_paths):
    """
    Считывает и объединяет данные из списка JSON файлов.
    """
    combined_data = []
    for file_path in file_paths:
        data = read_json(file_path)
        if data:
            # Если данные есть, добавляем их в общий список
            combined_data.extend(data)
    return combined_data


# Пример работы с данными
base_directory = "G:/Мой Диск/Google/takeout_bohdanhusak6_20250119_232544/Takeout/Мои действия"
export_data = collect_data_from_directory(base_directory)

combined_data = {}
for product, file_paths in export_data.items():
    combined_data[product] = merge_json_data(file_paths)

# Преобразуем данные в DataFrame
def convert_to_dataframe(data):
    # Преобразуем список событий в DataFrame
    all_events = []
    for product, events in data.items():
        for event in events:
            event['product'] = product  # Добавляем информацию о продукте
            all_events.append(event)

    # Создаем DataFrame из списка словарей
    df = pd.DataFrame(all_events)

    # Если нужно привести данные к нужному формату, например, распаковать массивы или другие преобразования
    # Пример распаковки данных в столбцах
    # df['headers'] = df['headers'].apply(unpack_header)

    return df

# Преобразуем данные в DataFrame
df = convert_to_dataframe(combined_data)

# Выводим первые несколько строк DataFrame
print(df.head())

# Если нужно сохранить DataFrame в CSV
# df.to_csv("combined_data.csv", index=False)