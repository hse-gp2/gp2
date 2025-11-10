import pandas as pd
import numpy as np
import re
import logging
from pathlib import Path
import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def clean_text(text):
    if pd.isna(text) or not isinstance(text, str):
        return text
    
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'[\n\r\t]+', ' ', text)
    text = re.sub(r'[^\w\s.,!?;:()\'"-]', '', text)
    text = text.strip()
    
    return text

def extract_year(date_str):
    if pd.isna(date_str) or not isinstance(date_str, str):
        return None
    
    year_match = re.search(r'\b(19|20)\d{2}\b', date_str)
    if year_match:
        return int(year_match.group())
    
    return None

def clean_price(price_str):
    if pd.isna(price_str):
        return None
    
    if isinstance(price_str, (int, float)):
        return float(price_str) if price_str > 0 else None
    
    if isinstance(price_str, str):
        price_match = re.search(r'[\d.]+', price_str)
        if price_match:
            try:
                price = float(price_match.group())
                return price if price > 0 else None
            except ValueError:
                return None
    
    return None

def clean_description(description):
    if pd.isna(description) or not isinstance(description, str):
        return None
    
    description = re.sub(r'<[^>]+>', '', description)
    description = clean_text(description)
    
    if len(description) < 10:
        return None
    
    return description

def clean_data(df):
    logger.info("Начинаем очистку данных...")
    
    df_cleaned = df.copy()
    
    if 'title' in df_cleaned.columns:
        logger.info("Очистка названий...")
        df_cleaned['title'] = df_cleaned['title'].apply(clean_text)
    
    if 'description' in df_cleaned.columns:
        logger.info("Очистка описаний...")
        df_cleaned['description'] = df_cleaned['description'].apply(clean_description)
    
    if 'author' in df_cleaned.columns:
        logger.info("Очистка авторов...")
        df_cleaned['author'] = df_cleaned['author'].apply(clean_text)
    
    if 'published_date' in df_cleaned.columns:
        logger.info("Извлечение года публикации...")
        df_cleaned['year'] = df_cleaned['published_date'].apply(extract_year)
    
    if 'price' in df_cleaned.columns:
        logger.info("Очистка цен...")
        df_cleaned['price'] = df_cleaned['price'].apply(clean_price)
    
    logger.info("Анализ пропущенных значений...")
    missing_stats = df_cleaned.isnull().sum()
    missing_percent = (missing_stats / len(df_cleaned)) * 100
    
    logger.info("\nПропущенные значения:")
    for col in df_cleaned.columns:
        if missing_stats[col] > 0:
            logger.info(f"  {col}: {missing_stats[col]} ({missing_percent[col]:.2f}%)")
    
    numeric_cols = df_cleaned.select_dtypes(include=[np.number]).columns
    logger.info(f"\nОбработка выбросов в числовых полях: {numeric_cols.tolist()}")
    
    for col in numeric_cols:
        if col in ['price', 'average_rating', 'ratings_count', 'page_count']:
            df_cleaned.loc[df_cleaned[col] < 0, col] = np.nan
            
            Q1 = df_cleaned[col].quantile(0.25)
            Q3 = df_cleaned[col].quantile(0.75)
            IQR = Q3 - Q1
            upper_bound = Q3 + 3 * IQR
            lower_bound = Q1 - 3 * IQR
            
            outliers = ((df_cleaned[col] < lower_bound) | (df_cleaned[col] > upper_bound)).sum()
            if outliers > 0:
                logger.info(f"  {col}: найдено {outliers} выбросов")
                df_cleaned.loc[(df_cleaned[col] < lower_bound) | (df_cleaned[col] > upper_bound), col] = np.nan
    
    logger.info("Очистка данных завершена")
    
    return df_cleaned

if __name__ == "__main__":
    input_file = config.DATA_DIR / "merged_books.csv"
    output_file = config.DATA_DIR / "cleaned_books.csv"
    
    logger.info(f"Загрузка данных из {input_file}...")
    df = pd.read_csv(input_file)
    
    logger.info(f"Исходный датасет: {len(df)} записей, {len(df.columns)} колонок")
    
    df_cleaned = clean_data(df)
    
    df_cleaned.to_csv(output_file, index=False)
    logger.info(f"Очищенные данные сохранены в {output_file}")
    
    print(f"\nСтатистика очищенного датасета:")
    print(f"Записей: {len(df_cleaned)}")
    print(f"Колонок: {len(df_cleaned.columns)}")
    print(f"\nПропущенные значения:")
    print(df_cleaned.isnull().sum())
