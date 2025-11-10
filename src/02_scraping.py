import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import logging
import re
from pathlib import Path
from tqdm import tqdm
import config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_page(url, retry_count=0, max_retries=3):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        }
        response = requests.get(url, headers=headers, timeout=config.SCRAPING_TIMEOUT)
        response.raise_for_status()
        return BeautifulSoup(response.content, 'html.parser')
    except requests.exceptions.RequestException as e:
        if retry_count < max_retries:
            logger.warning(f"Ошибка при запросе {url}, повтор {retry_count + 1}/{max_retries}")
            time.sleep(2 ** retry_count)
            return get_page(url, retry_count + 1, max_retries)
        logger.error(f"Ошибка при запросе {url}: {e}")
        return None

def parse_book_from_listing(book_element):
    try:
        book_data = {
            'source': 'project_gutenberg'
        }
        
        title_elem = book_element.find('span', class_='title')
        if title_elem:
            book_data['title'] = title_elem.get_text(strip=True)
        
        author_elem = book_element.find('span', class_='subtitle')
        if author_elem:
            book_data['author'] = author_elem.get_text(strip=True)
        
        link_elem = book_element.find('a')
        if link_elem:
            href = link_elem.get('href', '')
            if href:
                if not href.startswith('http'):
                    book_data['detail_link'] = 'https://www.gutenberg.org' + href
                else:
                    book_data['detail_link'] = href
        
        return book_data if book_data.get('title') else None
    except Exception as e:
        logger.error(f"Ошибка при парсинге книги: {e}")
        return None

def get_book_details(book_url):
    soup = get_page(book_url)
    if not soup:
        return {}
    
    details = {}
    
    try:
        desc_elem = soup.find('div', class_='bibrec')
        if desc_elem:
            desc_text = desc_elem.get_text(strip=True)
            if len(desc_text) > 50:
                details['description'] = desc_text[:1000]
        
        bibrec = soup.find('table', class_='bibrec')
        if bibrec:
            rows = bibrec.find_all('tr')
            for row in rows:
                th = row.find('th')
                td = row.find('td')
                if th and td:
                    key = th.get_text(strip=True).lower().replace(' ', '_')
                    value = td.get_text(strip=True)
                    if key == 'release_date':
                        year_match = re.search(r'\b(19|20)\d{2}\b', value)
                        if year_match:
                            details['year'] = int(year_match.group())
                    details[key] = value
        
        subjects_elem = soup.find('td', string=re.compile('Subject', re.I))
        if subjects_elem:
            parent = subjects_elem.find_parent('tr')
            if parent:
                td = parent.find('td', class_='field_value')
                if td:
                    subjects = [a.get_text(strip=True) for a in td.find_all('a')]
                    if subjects:
                        details['subjects'] = ', '.join(subjects[:5])
        
    except Exception as e:
        logger.error(f"Ошибка при получении деталей книги {book_url}: {e}")
    
    return details

def scrape_gutenberg(min_books=10000, output_file=None):
    all_books = []
    
    if output_file and Path(output_file).exists():
        logger.info(f"Загрузка существующих данных из {output_file}")
        try:
            existing_df = pd.read_csv(output_file)
            all_books = existing_df.to_dict('records')
            logger.info(f"Загружено {len(all_books)} существующих записей")
        except Exception as e:
            logger.warning(f"Не удалось загрузить существующие данные: {e}")
    
    base_url = "https://www.gutenberg.org"
    
    categories = [
        "Adventure", "American Literature", "British Literature", "French Literature", 
        "German Literature", "Russian Literature", "Classics of Literature", "Biographies",
        "Novels", "Short Stories", "Poetry", "Plays/Films/Dramas", "Romance",
        "Science-Fiction & Fantasy", "Crime", "Thrillers & Mystery", "Mythology",
        "History - American", "History - British", "History - European", "History - Ancient",
        "History - Medieval/Middle Ages", "History - Modern", "Art", "Architecture", "Music",
        "Religion/Spirituality", "Philosophy & Ethics", "Cooking & Drinking", "Sports/Hobbies",
        "Travel Writing", "Health & Medicine", "Mathematics", "Science - Physics",
        "Science - Chemistry/Biochemistry", "Science - Biology", "Business/Management",
        "Economics", "Law & Criminology", "Psychiatry/Psychology", "Sociology", "Politics"
    ]
    
    logger.info(f"Начинаем сбор данных с Project Gutenberg")
    logger.info(f"Цель: минимум {min_books} книг")
    
    seen_titles = set()
    if all_books:
        seen_titles = {book.get('title', '') for book in all_books if book.get('title')}
    
    for query_idx, query in enumerate(categories, 1):
        if len(all_books) >= min_books:
            break
            
        logger.info(f"[{query_idx}/{len(categories)}] Категория: {query}")
        
        start_index = 1
        max_index = 10000
        items_per_page = 25
        consecutive_empty_pages = 0
        max_empty_pages = 5
        
        while start_index < max_index and len(all_books) < min_books:
            search_url = f"{base_url}/ebooks/search/?query={query.replace(' ', '+')}&start_index={start_index}"
            logger.info(f"Запрос: {search_url}")
            soup = get_page(search_url)
            
            if not soup:
                logger.warning(f"Не удалось загрузить страницу для запроса: {query} (start_index={start_index})")
                break
            
            books = soup.find_all('li', class_='booklink')
            
            if not books:
                logger.info(f"Больше нет книг для запроса: {query}")
                break
            
            books_collected_this_page = 0
            for book_elem in books:
                if len(all_books) >= min_books:
                    break
                
                book_data = parse_book_from_listing(book_elem)
                
                if book_data and book_data.get('title'):
                    if book_data['title'] not in seen_titles:
                        seen_titles.add(book_data['title'])
                        all_books.append(book_data)
                        books_collected_this_page += 1
            
            logger.info(f"Собрано на странице: {books_collected_this_page} книг (всего: {len(all_books)})")
            
            if books_collected_this_page == 0:
                consecutive_empty_pages += 1
                if consecutive_empty_pages >= max_empty_pages:
                    logger.info(f"Пропущено {max_empty_pages} страниц подряд без новых книг. Переход к следующей категории.")
                    break
            else:
                consecutive_empty_pages = 0
            
            if output_file and len(all_books) > 0 and len(all_books) % 100 == 0:
                try:
                    df_temp = pd.DataFrame(all_books)
                    df_temp.to_csv(output_file, index=False)
                    logger.info(f"Промежуточное сохранение: {len(all_books)} записей")
                except Exception as e:
                    logger.error(f"Ошибка при промежуточном сохранении: {e}")
            
            start_index += items_per_page
            time.sleep(0.3)
        
        logger.info(f"Собрано {len(all_books)} книг (запрос: {query})")
        time.sleep(1.0)
    
    logger.info(f"Всего собрано книг: {len(all_books)}")
    
    df = pd.DataFrame(all_books)
    return df

if __name__ == "__main__":
    output_file = config.DATA_DIR / "project_gutenberg.csv"
    df_books = scrape_gutenberg(min_books=10000, output_file=output_file)
    
    df_books.to_csv(output_file, index=False)
    logger.info(f"Финальные данные сохранены в {output_file}")
    
    print(f"\nСобрано книг: {len(df_books)}")
    print(f"Колонки: {df_books.columns.tolist()}")
    print(f"\nПервые строки:")
    print(df_books.head())
