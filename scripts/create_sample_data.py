import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

def create_sample_data():
    """Создает тестовый CSV файл с данными о продажах"""
    
    # Создаем директории
    os.makedirs('data/raw', exist_ok=True)
    os.makedirs('data/processed', exist_ok=True)
    
    # Генерируем данные
    np.random.seed(42)
    n_records = 10000
    
    dates = pd.date_range(start='2024-01-01', end='2024-03-01', periods=n_records)
    
    data = pd.DataFrame({
        'transaction_date': np.random.choice(dates, n_records),
        'product_id': np.random.randint(1000, 1100, n_records),
        'quantity': np.random.randint(1, 10, n_records),
        'price': np.random.uniform(10, 500, n_records),
        'region': np.random.choice(['North', 'South', 'East', 'West', 'Central'], n_records),
        'customer_id': np.random.randint(10000, 20000, n_records)
    })
    
    # Вычисляем общую сумму
    data['total_amount'] = data['quantity'] * data['price']
    
    # Сохраняем в CSV
    file_path = 'data/raw/sales_data.csv'
    data.to_csv(file_path, index=False)
    
    print(f"✅ Создан файл с данными: {file_path}")
    print(f"📊 Количество записей: {len(data)}")
    print(f"📅 Диапазон дат: {data['transaction_date'].min()} - {data['transaction_date'].max()}")
    
    return data

if __name__ == "__main__":
    create_sample_data()