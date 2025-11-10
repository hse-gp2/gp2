import pandas as pd
import logging
from pathlib import Path
import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def merge_datasets(google_books_file, gutenberg_file, output_file):
    logger.info("Загрузка данных из Google Books API...")
    df_google = pd.read_csv(google_books_file)
    logger.info(f"Загружено {len(df_google)} записей из Google Books API")
    
    logger.info("Загрузка данных из Project Gutenberg...")
    df_gutenberg = pd.read_csv(gutenberg_file)
    logger.info(f"Загружено {len(df_gutenberg)} записей из Project Gutenberg")
    
    if 'source' not in df_google.columns:
        df_google['source'] = 'google_books_api'
    if 'source' not in df_gutenberg.columns:
        df_gutenberg['source'] = 'project_gutenberg'
    
    common_cols = set(df_google.columns) & set(df_gutenberg.columns)
    logger.info(f"Общие колонки: {common_cols}")
    
    df_merged = pd.concat([df_google, df_gutenberg], ignore_index=True, sort=False)
    
    logger.info(f"Объединенный датасет содержит {len(df_merged)} записей")
    logger.info(f"Колонки: {df_merged.columns.tolist()}")
    
    df_merged.to_csv(output_file, index=False)
    logger.info(f"Объединенные данные сохранены в {output_file}")
    
    return df_merged

if __name__ == "__main__":
    google_books_file = config.DATA_DIR / "google_books_api.csv"
    gutenberg_file = config.DATA_DIR / "project_gutenberg.csv"
    output_file = config.DATA_DIR / "merged_books.csv"
    
    df_merged = merge_datasets(google_books_file, gutenberg_file, output_file)
    
    print(f"\nСтатистика объединенного датасета:")
    print(f"Всего записей: {len(df_merged)}")
    print(f"Колонок: {len(df_merged.columns)}")
    print(f"\nПервые строки:")
    print(df_merged.head())
