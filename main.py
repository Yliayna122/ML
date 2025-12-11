import requests # для http-запросов к сайту
import time # для управления задержками между запросами
from bs4 import BeautifulSoup #для парсинга и навигации по html-дереву
import random
import sqlite3 # для работы с бд
import uuid
from datetime import datetime
import re # очистка текста от лишних переносов
from typing import Union

# _________________________________1. Основные параметры___________________________________________________

User_agent = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
]
# Строки, имитирующие браузеры, который используется для случайного выбора при каждом запросе

Db = 'SakhaNews.db'
Links = 'links.txt'  # временный файл с собранными ссылками на статьи
SakhaNews_url = "https://1sn.ru"
Max_obrabotka_page = 10
Zaderchka_stat = 0.5  # задержки в сек. для соблюдения Rate limit
Zaderchka_page = 1.0
Min_len_text = 50  # минимальная длина очищенного текста для записи в бд (для отсеивания статей, состоящих только из медиа)

Max_repeat = 3  # максимальное кол-во повторных попыток при ошибках
Delay_repeat = 5  # задержка в секундах для повторной попытки

Razdel = [
    "vlast-i-politika", "finansy-i-nalogi", "zilyo-zkx", "obshhestvo",
    "nauka-i-obrazovanie", "kultura", "zdorove-medicina", "pravo-kriminal",
    "cs-proissestviya", "sport", "ekonomika-i-biznes", "istoriya", "internet",
    "transport-i-svyaz", "abk-neft-i-gaz", "ekologiya", "literaturnaya-stranica"
]


# Функция для повторных запросов при ошибках
def fetch_url_retry(url: str, max_retries: int = Max_repeat) -> Union[str, None]:

    for attempt in range(max_retries):
        headers = {'User-Agent': random.choice(User_agent)}
        try:
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status() # вызовет исключение для 4xx,5xx ошибок

            return response.text

        except requests.exceptions.RequestException as e:
            status_code = getattr(e.response, 'status_code', 'Timeout/Connection Error')

            # Критические ошибки (404, 403) не повторяем
            if status_code in [404, 403]:
                print(f" Ошибка {status_code}: Невозможно повторить")
                return None

            # Повторяем только при временных ошибках (5xx, 429, таймауты)
            if attempt < max_retries - 1:
                delay = Delay_repeat * (2 ** attempt) + random.uniform(0, 1)
                print(f"Ошибка {status_code} на попытке {attempt + 1}. Повтор через {delay:.2f} сек.")
                time.sleep(delay)
            else:
                print(f"Ошибка {status_code}: Достигнут максимум попыток ({max_retries}). Пропуск.")
                return None
    return None


# _________________________________2. База данных___________________________________________________

# Создание таблицы статей в бд
def create_table():
    connection = sqlite3.connect(Db) # установка соединения с файлом бд
    cursor = connection.cursor() # объект-посредник для отправки команд в бд
    cursor.execute("""
        create table if not exists articles (
            guid TEXT PRIMARY KEY,
            title TEXT,
            description TEXT,
            url TEXT UNIQUE,
            published_at TEXT NULL,
            comments_count INTEGER NULL,
            created_at_utc TEXT,
            rating INTEGER NULL  
        )
    """)
    connection.commit()
    connection.close()


# Функция отвечает за сохранение спарсенной статьи в бд
def saving_article(data: dict) -> bool:
    connection = sqlite3.connect(Db)
    cursor = connection.cursor()
    data['guid'] = str(uuid.uuid4()) # генерация id
    data['created_at_utc'] = datetime.utcnow().isoformat() # время создания записи в бд
    sql = """
        insert into articles (
            guid, title, description, url, published_at, comments_count, created_at_utc, rating
        ) values (
            :guid, :title, :description, :url, :published_at, :comments_count, :created_at_utc, :rating
        )
    """
# Обработка ошибок
    try:
        cursor.execute(sql, data)
        connection.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    except Exception as e:
        print(f"Ошибка при вставке в БД: {e}")
        return False
    finally:
        connection.close()

# _________________________________3. Парсинг и очистка статьи___________________________________________________

# Функция загружает и парсит одну статью, очищает текст и возвращает данные
def parse_article(url: str) -> Union[dict, None]: # на вход url, на выход словарь с данными либо none
    # каркас для будущих данных
    data = {
        'url': url,
        'comments_count': 0, # значение по умолчанию
        'rating': None,
    }
    # Загрузка контента с повторами
    article_html = fetch_url_retry(url)
    if not article_html:
        return None
    try:
        soup = BeautifulSoup(article_html, 'lxml')

        # Извлечение основных полей. Заголовок статьи
        title_tag = soup.select_one('h1[itemprop="headline"]') # ищем тег h1 с атрибутом itemprop="headline"
        data['title'] = title_tag.get_text(strip=True) if title_tag else 'Нет заголовка' # извлекается текст из тега

        # Дата публикации
        data['published_at'] = None
        date_span = soup.select_one('.alert-secondary span') # ищем элемент с классом alert-secondary, а внутри него тег span

        if date_span:
            date_str = date_span.get_text(strip=True)
            try:
                published_dt = datetime.strptime(date_str, '%d.%m.%Y %H:%M') # преобразование строки в объект datetime
                data['published_at'] = published_dt.isoformat() # преобразование datetime в строку ISO-формата
            except ValueError:
                pass

        # Кол-во комментариев
        data['comments_count'] = 0
        reply_buttons = soup.find_all(
            lambda tag: (tag.name == 'a' or tag.name == 'button') and 'ответить' in tag.get_text(strip=True).lower())
        if reply_buttons:
            data['comments_count'] = len(reply_buttons)

        # Очистка текста
        article_content = soup.select_one('.detail_text') # ищем элемент с классом detail_text

        if article_content:
            # удаление медиа-контента
            media_selectors = [
                'figure', 'img', 'iframe', '.gallery-block', '.yandex-rtb',
            ]

            for selector in media_selectors:
                for media_tag in article_content.select(selector):
                    media_tag.decompose()

            # Получение чистого текста и очистка
            clean_text = article_content.get_text('\n', strip=True)

            clean_text = re.sub(r'(\w)\n(\w)', r'\1 \2', clean_text) # убираем переносы внутри слов
            clean_text = re.sub(r'\n\s*\n', '\n\n', clean_text).strip() # убираем лишние пустые строки
            data['description'] = clean_text

            # проверка на пустую статью
            if len(data['description']) < Min_len_text:
                return None
            return data
        else:
            return None
    except Exception:
        # При ошибках парсинга (не HTTP) возвращаем None
        return None

# _________________________________4. Сбор ссылок со страниц___________________________________________________

def scrape_page_for_links(page_url: str) -> set:
    headers = {'User-Agent': random.choice(User_agent)} # берем случайный ua
    links = set() # создаем пустое множество

    try:
        response = requests.get(page_url, headers=headers, timeout=15)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'lxml')
        link_elements = soup.select('table.table a') # все ссылки <a> внутри таблиц с классом table

        for link_element in link_elements:
            href = link_element.get('href')
            if href and 'https://1sn.ru/' in href:
                links.add(href)

        return links

    except requests.exceptions.RequestException as e:
        print(f"Ошибка запроса страницы {page_url}: {e}")
        return links
    except Exception as e:
        print(f"Критическая ошибка при парсинге ссылок: {e}")
        return links


# _________________________________5. Главная функция исполнения___________________________________________________

def run_pilot_scraper():
    # 1) Сбор всех ссылок
    all_article_links = set()
    print(f"Обработка {Max_obrabotka_page} страниц каждого раздела")

    for category in Razdel:
        for page in range(1, Max_obrabotka_page + 1):
            page_url = f"{SakhaNews_url}/rubric/{category}?page={page}"
            print(f"Категория '{category}': Страница {page}/{Max_obrabotka_page}...")

            links_on_page = scrape_page_for_links(page_url)

            # Проверка, что страница не пуста
            if not links_on_page and page > 1:
                print(f"Конец для '{category}' достигнут на странице {page}.")
                break

            new_links_count = len(links_on_page - all_article_links)
            all_article_links.update(links_on_page)

            print(f"Найдено новых ссылок: {new_links_count}. Всего: {len(all_article_links)}")

            # Пауза
            time.sleep(Zaderchka_page)

            if not links_on_page and page == 1:
                continue
            elif not links_on_page:
                break

    # Сохранение всех собранных ссылок в файл
    with open(Links, 'w', encoding='utf-8') as f:
        for link in sorted(list(all_article_links)):
            f.write(link + '\n')

    print(f" Всего собрано уникальных ссылок: {len(all_article_links)}")
    print(f"Ссылки сохранены в файле '{Links}'")

    # 2) Парсинг статей и бд
    if not all_article_links:
        print("\nНет ссылок для парсинга. Завершение работы.")
        return

    create_table()

    print(f"\n Парсинг {len(all_article_links)} статей и запись в бд")

    parsed_count = 0
    all_links_list = list(all_article_links)

    for i, url in enumerate(all_links_list):

        print(f"[{i + 1}/{len(all_links_list)}] Парсинг: {url}")

        article_data = parse_article(url)

        if article_data:
            if saving_article(article_data):
                parsed_count += 1
            else:
                print("Пропуск: Дубликат URL в БД.")

        # Rate Limit
        time.sleep(Zaderchka_stat)

    print(f"\n___Результат___")
    print(f"Всего обработано ссылок: {len(all_links_list)}")
    print(f"Всего новых статей добавлено в БД '{Db}': {parsed_count}")

if __name__ == "__main__":
    run_pilot_scraper()