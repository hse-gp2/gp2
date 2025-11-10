import requests
import pandas as pd
import time
import logging
from pathlib import Path
import config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_books_by_query(query, api_key, max_results=40, start_index=0, retry_count=0, max_retries=3):
    url = "https://www.googleapis.com/books/v1/volumes"
    params = {
        "q": query,
        "maxResults": max_results,
        "startIndex": start_index
    }
    
    if api_key:
        params["key"] = api_key
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        if hasattr(e, 'response') and e.response is not None:
            if e.response.status_code == 429:
                if retry_count < max_retries:
                    wait_time = (2 ** retry_count) * 60
                    logger.warning(f"Превышен лимит запросов (429). Ожидание {wait_time} секунд перед повтором {retry_count + 1}/{max_retries}...")
                    time.sleep(wait_time)
                    return get_books_by_query(query, api_key, max_results, start_index, retry_count + 1, max_retries)
                else:
                    logger.error(f"Превышен лимит запросов после {max_retries} попыток. Пропускаем этот запрос.")
                    return None
            
            if api_key and e.response.status_code == 403:
                logger.warning(f"API недоступен с ключом (возможно, API не включен в проекте). Пробуем без ключа...")
                params.pop("key", None)
                try:
                    response = requests.get(url, params=params, timeout=30)
                    response.raise_for_status()
                    return response.json()
                except:
                    pass
        
        logger.error(f"Ошибка при запросе к API: {e}")
        if hasattr(e, 'response') and e.response is not None:
            try:
                error_data = e.response.json()
                if 'error' in error_data and 'message' in error_data['error']:
                    logger.error(f"Детали ошибки: {error_data['error']['message']}")
            except:
                logger.error(f"Текст ответа: {e.response.text[:500]}")
        return None

def parse_book_data(book_item):
    volume_info = book_item.get("volumeInfo", {})
    
    authors = volume_info.get("authors", [])
    author = ", ".join(authors) if authors else None
    
    categories = volume_info.get("categories", [])
    category = ", ".join(categories) if categories else None
    
    book_data = {
        "id": book_item.get("id"),
        "title": volume_info.get("title"),
        "author": author,
        "publisher": volume_info.get("publisher"),
        "published_date": volume_info.get("publishedDate"),
        "description": volume_info.get("description"),
        "categories": category,
        "language": volume_info.get("language"),
        "page_count": volume_info.get("pageCount"),
        "average_rating": volume_info.get("averageRating"),
        "ratings_count": volume_info.get("ratingsCount"),
        "thumbnail": volume_info.get("imageLinks", {}).get("thumbnail") if volume_info.get("imageLinks") else None,
        "source": "google_books_api"
    }
    
    return book_data

def collect_books_from_api(api_key, queries, max_books_per_query=200, output_file=None):
    all_books = []
    seen_ids = set()
    
    if output_file and Path(output_file).exists():
        logger.info(f"Загрузка существующих данных из {output_file}")
        try:
            existing_df = pd.read_csv(output_file)
            if 'id' in existing_df.columns:
                seen_ids = set(existing_df['id'].dropna().unique())
                all_books = existing_df.to_dict('records')
                logger.info(f"Загружено {len(all_books)} существующих записей")
        except Exception as e:
            logger.warning(f"Не удалось загрузить существующие данные: {e}")
    
    for query_idx, query in enumerate(queries, 1):
        logger.info(f"[{query_idx}/{len(queries)}] Обработка запроса: {query}")
        books_collected = 0
        
        start_index = 0
        max_requests = 500
        request_count = 0
        
        while start_index < max_books_per_query and request_count < max_requests:
            logger.info(f"Запрос книг {start_index} - {start_index + 40}")
            
            result = get_books_by_query(query, api_key, max_results=40, start_index=start_index)
            request_count += 1
            
            if result and "items" in result and len(result["items"]) > 0:
                items_count = len(result["items"])
                for item in result["items"]:
                    book_data = parse_book_data(item)
                    if book_data and book_data.get("id") and book_data["id"] not in seen_ids:
                        seen_ids.add(book_data["id"])
                        all_books.append(book_data)
                        books_collected += 1
                
                if items_count == 0:
                    break
                
                start_index += items_count
            else:
                logger.warning(f"Не удалось получить данные для запроса: {query} (start_index={start_index})")
                break
            
            time.sleep(config.API_DELAY)
            
            if output_file and len(all_books) > 0 and len(all_books) % 100 == 0:
                try:
                    df_temp = pd.DataFrame(all_books)
                    df_temp.to_csv(output_file, index=False)
                    logger.info(f"Промежуточное сохранение: {len(all_books)} записей")
                except Exception as e:
                    logger.error(f"Ошибка при промежуточном сохранении: {e}")
        
        logger.info(f"Собрано {books_collected} книг для запроса: {query} (всего: {len(all_books)})")
        
        if output_file and len(all_books) > 0:
            try:
                df_temp = pd.DataFrame(all_books)
                df_temp.to_csv(output_file, index=False)
                logger.info(f"Сохранено после запроса '{query}': {len(all_books)} записей")
            except Exception as e:
                logger.error(f"Ошибка при сохранении: {e}")
        
        time.sleep(2.0)
    
    df = pd.DataFrame(all_books)
    logger.info(f"Всего собрано книг: {len(df)}")
    return df

if __name__ == "__main__":
    API_KEY = config.GOOGLE_BOOKS_API_KEY
    
    if not API_KEY:
        logger.warning("API ключ не найден! Используется запрос без ключа (с ограничениями)")
        logger.warning("Для полноценной работы включите Books API в Google Cloud Console")
    
    queries = [
        "fiction", "science fiction", "mystery", "romance", "thriller",
        "biography", "history", "philosophy", "programming", "art",
        "novel", "drama", "adventure", "fantasy", "horror",
        "poetry", "essay", "children", "young adult", "classic",
        "contemporary", "literary", "nonfiction", "memoir", "self-help",
        "business", "economics", "psychology", "sociology", "politics"
    ]
    
    output_file = config.DATA_DIR / "google_books_api.csv"
    df_books = collect_books_from_api(API_KEY, queries, max_books_per_query=300, output_file=output_file)
    
    df_books.to_csv(output_file, index=False)
    logger.info(f"Финальные данные сохранены в {output_file}")
