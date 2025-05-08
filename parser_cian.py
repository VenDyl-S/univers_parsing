"""parser_cian.py — Парсер для ЦИАН
Отдельный класс для парсинга ЦИАН с использованием базы данных SQLite.
"""

import json
import logging
import random
import threading
import time
import re
import traceback
import os
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple, Any, Union

import requests
from bs4 import BeautifulSoup
from loguru import logger
from custom_exception import StopEventException
from db_service import SQLiteDBHandler

# Максимальное количество фотографий для отправки
MAX_PHOTOS = 3  # Ограничение - не более 3 фото

class CianParse:
    """
    HTML-парсер ЦИАН без GUI/Excel.
    По аналогии с AvitoParse только для парсинга ЦИАН и отправки уведомлений.
    Использует SQLite для хранения данных.
    """

    def __init__(
        self,
        url: list,
        keysword_list: list | None = None,
        keysword_black_list: list | None = None,
        count: int = 5,
        tg_token: str | None = None,
        chat_id: int | None = None,
        job_name: str | None = None,
        max_price: int = 0,
        min_price: int = 0,
        pause: int = 300,
        debug_mode: int = 0,
        proxy: str | None = None,
        proxy_change_url: str | None = None,
        stop_event: threading.Event | None = None,
        first_run: bool = False
    ) -> None:
        # --- публичные параметры ------------------------------------------------
        self.url_list = url
        self.keys_word = keysword_list or None
        self.keys_black_word = keysword_black_list or None
        self.count = count  # количество страниц для сканирования
        self.tg_token = tg_token
        self.chat_id = chat_id
        self.job_name = job_name
        self.max_price = int(max_price)
        self.min_price = int(min_price)
        self.pause = pause  # в секундах
        self.debug_mode = debug_mode
        self.proxy = proxy
        self.proxy_change_url = proxy_change_url
        self.first_run = first_run

        # --- служебные ----------------------------------------------------------
        self.url: str | None = None
        self.stop_event = stop_event or threading.Event()
        
        # Инициализируем обработчик базы данных SQLite
        self.db_handler = SQLiteDBHandler()
        
        # Отслеживание объявлений
        self.known_ads: Set[str] = set()  # Все известные объявления из БД
        self.current_scan_ads: Set[str] = set()  # Объявления текущего сканирования
        
        # Счетчики статистики - для отображения при остановке поиска
        self.total_new_ads: int = 0  # Общее количество новых объявлений
        self.total_notified_ads: int = 0  # Количество уведомлений (прошедших фильтры)
        
        # Создаем HTTP сессию для запросов
        self.session = requests.Session()
        self.update_headers()
        
        # Загружаем сохраненные объявления из БД
        self._load_known_ads()

    def _normalize_proxy(self, proxy_str: str) -> str:
        """
        Нормализует строку прокси к формату username:password@host:port
        Поддерживает форматы: 
        1. username:password@host:port
        2. host:port:username:password
        3. http://username:password@host:port
        4. socks5://username:password@host:port
        """
        if not proxy_str or proxy_str.strip() == "":
            return ""
        
        # Если прокси содержит точку с запятой, значит это список прокси
        if ';' in proxy_str:
            proxy_list = [p.strip() for p in proxy_str.split(';') if p.strip()]
            if proxy_list:
                # Выбираем случайный прокси из списка
                proxy_str = random.choice(proxy_list)
            else:
                return ""
        
        # Убираем протокол, если есть
        proxy_str = re.sub(r'^(https?|socks[45])://', '', proxy_str)
        
        # Если уже содержит @, то проверяем формат username:password@host:port
        if '@' in proxy_str:
            match = re.match(r'^([^:]+:[^@]+)@([^:]+:\d+)$', proxy_str)
            if match:
                return proxy_str  # Уже в нужном формате
        
        # Проверяем формат host:port:username:password
        parts = proxy_str.split(':')
        if len(parts) == 4:
            host, port, username, password = parts
            return f"{username}:{password}@{host}:{port}"
        
        # Если формат неизвестен, возвращаем исходную строку
        return proxy_str

    def update_headers(self):
        """Обновляет заголовки HTTP запроса и настройки прокси."""
        # Загружаем User-Agent из файла
        try:
            with open("user_agent_pc.txt", "r") as f:
                user_agents = [line.strip() for line in f if line.strip()]
            user_agent = random.choice(user_agents)
        except Exception as e:
            logger.error(f"Ошибка при чтении файла user_agent_pc.txt: {e}")
            # Если не удалось прочитать файл, используем значение по умолчанию
            user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36'
        
        self.headers = {
            'User-Agent': user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Referer': 'https://www.cian.ru/',
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'Sec-Ch-Ua': '"Chromium";v="122", "Not(A:Brand";v="24"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'Priority': 'u=0, i'
        }
        logger.info(f"Используется User-Agent: {user_agent}")
        
        # Добавление прокси, если передан
        if self.proxy:
            # Если прокси содержит точку с запятой, значит это список прокси
            if ';' in self.proxy:
                proxy_list = [p.strip() for p in self.proxy.split(';') if p.strip()]
                if proxy_list:
                    # Выбираем случайный прокси из списка
                    selected_proxy = random.choice(proxy_list)
                    # Нормализуем формат прокси
                    normalized_proxy = self._normalize_proxy(selected_proxy)
                    if normalized_proxy:
                        logger.info(f"Используется прокси: {normalized_proxy}")
                        # Для requests сессии нужно добавить протокол
                        self.session.proxies = {
                            "http": f"http://{normalized_proxy}",
                            "https": f"http://{normalized_proxy}"
                        }
            else:
                # Нормализуем формат прокси
                normalized_proxy = self._normalize_proxy(self.proxy)
                if normalized_proxy:
                    logger.info(f"Используется прокси: {normalized_proxy}")
                    # Для requests сессии нужно добавить протокол
                    self.session.proxies = {
                        "http": f"http://{normalized_proxy}",
                        "https": f"http://{normalized_proxy}"
                    }

    def handle_ip_block(self) -> bool:
        """Обрабатывает блокировку IP. Возвращает True если IP успешно изменен."""
        # Если настроена ссылка для смены IP
        if self.proxy_change_url:
            try:
                logger.info("Обнаружена блокировка IP. Пытаюсь сменить IP...")
                response = requests.get(self.proxy_change_url, timeout=15)
                if response.status_code == 200:
                    logger.info("IP успешно изменен. Продолжаю парсинг.")
                    # Короткая пауза для применения нового IP
                    time.sleep(3)
                    # После смены IP обновляем сессию
                    self.session = requests.Session()
                    self.update_headers()
                    return True
                else:
                    logger.error(f"Не удалось сменить IP. Код ответа: {response.status_code}")
            except Exception as e:
                logger.error(f"Ошибка при смене IP: {e}")
        
        # Если не удалось сменить IP или нет ссылки, делаем паузу
        logger.info("Делаю паузу из-за блокировки IP...")
        time.sleep(random.randint(300, 350))
        return False
            
    def _load_known_ads(self):
        """Загружает все известные ID объявлений из БД."""
        # Загружаем ID объявлений из истории сканирований
        for url in self.url_list:
            scan_ids = self.db_handler.get_cian_scan_ids(url)
            if scan_ids:
                self.known_ads.update(scan_ids)
        
        # Также добавляем ID из уже просмотренных объявлений
        cian_viewed = self.db_handler.list_all_cian_records()
        for record in cian_viewed:
            if record and record[0]:  # ID - первый элемент в кортеже
                self.known_ads.add(record[0])
        
        logger.info(f"ЦИАН: Загружено {len(self.known_ads)} известных объявлений из БД")
    
    def _save_scan_results(self):
        """Сохраняет результаты текущего сканирования в БД."""
        # Сохраняем объединенный набор ID для каждого URL
        all_ads = self.known_ads.union(self.current_scan_ads)
        for url in self.url_list:
            self.db_handler.save_cian_scan_ids(url, list(all_ads))
        
        logger.info(f"ЦИАН: Сохранено {len(all_ads)} объявлений в БД")
    
    def get_page(self, url: str) -> Optional[str]:
        """Получает HTML-страницу по URL с защитой от блокировок."""
        # Перед каждым запросом обновляем User-Agent
        self.update_headers()
        
        try:
            # Добавляем случайную задержку перед запросом
            time.sleep(random.uniform(2, 4))
            
            response = self.session.get(url, headers=self.headers, timeout=15)
            response.raise_for_status()
            
            # Проверяем, не блокировка ли это
            if "captcha" in response.text.lower() or "доступ ограничен" in response.text.lower():
                logger.warning("Обнаружена блокировка доступа на ЦИАН")
                if self.handle_ip_block():
                    # Повторяем запрос после смены IP
                    return self.get_page(url)
                else:
                    return None
            
            if self.debug_mode:
                with open('last_response.html', 'w', encoding='utf-8') as f:
                    f.write(response.text)
                logger.info(f"Сохранён последний ответ в last_response.html")
            
            return response.text
        except requests.RequestException as e:
            logger.error(f"Ошибка при запросе: {e}")
            return None

    def extract_json_data(self, html: str) -> List[Dict[str, Any]]:
        """Извлекает данные объявлений из JSON-структуры на странице."""
        offers = []
        try:
            # Ищем JSON данные в HTML
            soup = BeautifulSoup(html, 'html.parser')
            
            # Вариант 1: ищем window._cianConfig
            json_script_pattern = re.compile(r'window\._cianConfig\s*=\s*({.*?});', re.DOTALL)
            
            # Вариант 2: ищем window.__initialData
            initialdata_pattern = re.compile(r'window\.__initialData\s*=\s*({.*?});', re.DOTALL)
            
            # Поиск данных в скриптах
            scripts = soup.find_all('script')
            
            json_data = None
            config_data = None
            
            # Сначала ищем в window._cianConfig
            for script in scripts:
                if script.string and 'window._cianConfig' in script.string:
                    match = json_script_pattern.search(script.string)
                    if match:
                        try:
                            config_data = json.loads(match.group(1))
                            
                            # Ищем данные в разных местах JSON структуры
                            if 'data' in config_data and 'offerSearch' in config_data['data']:
                                if 'results' in config_data['data']['offerSearch']:
                                    json_data = config_data['data']['offerSearch']['results']
                                    logger.info(f"Найдены данные в config_data['data']['offerSearch']['results']")
                                    break
                            
                            # Альтернативный путь
                            if 'results' in config_data:
                                json_data = config_data['results']
                                logger.info(f"Найдены данные в config_data['results']")
                                break
                        except json.JSONDecodeError:
                            continue
            
            # Если не нашли в window._cianConfig, ищем в window.__initialData
            if not json_data:
                for script in scripts:
                    if script.string and 'window.__initialData' in script.string:
                        match = initialdata_pattern.search(script.string)
                        if match:
                            try:
                                initial_data = json.loads(match.group(1))
                                
                                # Ищем данные объявлений
                                for key, value in initial_data.items():
                                    if isinstance(value, dict) and 'value' in value and 'results' in value['value']:
                                        json_data = value['value']['results']
                                        logger.info(f"Найдены данные в initialData['{key}']['value']['results']")
                                        break
                            except json.JSONDecodeError:
                                continue
            
            # Сохраняем найденные данные для отладки
            if config_data and self.debug_mode:
                with open('cian_config_data.json', 'w', encoding='utf-8') as f:
                    json.dump(config_data, f, ensure_ascii=False, indent=2)
            
            if json_data:
                logger.info(f"Найдено {len(json_data)} объявлений в JSON данных")
                
                # Сохраняем объявления для отладки
                if self.debug_mode:
                    with open('cian_json_data.json', 'w', encoding='utf-8') as f:
                        json.dump(json_data, f, ensure_ascii=False, indent=2)
                
                # Обрабатываем каждое объявление
                for item in json_data:
                    try:
                        offer_id = item.get('id')
                        if not offer_id:
                            continue
                        
                        # Извлекаем информацию о метро
                        metro_info = self._extract_metro_info(item)
                        
                        offer = {
                            'id': str(offer_id),
                            'title': self._extract_title(item),
                            'price': self._extract_price(item),
                            'link': f"https://www.cian.ru/rent/flat/{offer_id}/",
                            'date': self._extract_date(item),
                            'description': self._extract_description(item),
                            'photos': self._extract_photos(item),
                            'timestamp': datetime.now().isoformat()
                        }
                        
                        offers.append(offer)
                    except Exception as e:
                        logger.error(f"Ошибка при обработке объявления {offer_id}: {e}")
                        continue
        except Exception as e:
            logger.error(f"Ошибка при извлечении JSON данных: {e}")
            logger.error(traceback.format_exc())
        
        return offers
    
    def _extract_title(self, item: Dict) -> str:
        """Извлекает заголовок объявления из JSON-данных."""
        title_parts = []
        
        try:
            # 1. Проверяем наличие готового заголовка
            if 'title' in item and item['title']:
                return item['title']
            
            # 2. Собираем заголовок из разных полей
            room_count = None
            room_type = None
            
            # Ищем количество комнат
            if 'roomsCount' in item:
                room_count = item['roomsCount']
            elif 'details' in item and 'roomsCount' in item['details']:
                room_count = item['details']['roomsCount']
            
            # Тип недвижимости (например, "1-комн. квартира")
            if room_count is not None:
                if room_count == 0:
                    room_type = 'Студия'
                elif room_count > 0:
                    room_type = f"{room_count}-комн. квартира"
            
            if room_type:
                title_parts.append(room_type)
            
            # Ищем площадь
            area = None
            if 'totalArea' in item:
                area = item['totalArea']
            elif 'details' in item and 'area' in item['details']:
                area = item['details']['area']
            
            if area:
                title_parts.append(f"{area} м²")
            
            # Ищем этаж и этажность
            floor = None
            max_floor = None
            
            if 'floorNumber' in item:
                floor = item['floorNumber']
            elif 'details' in item and 'floor' in item['details']:
                floor = item['details']['floor']
            
            if 'building' in item and 'floorsCount' in item['building']:
                max_floor = item['building']['floorsCount']
            elif 'details' in item and 'building' in item['details'] and 'floorsCount' in item['details']['building']:
                max_floor = item['details']['building']['floorsCount']
            
            if floor and max_floor:
                title_parts.append(f"{floor}/{max_floor} эт.")
            
            # Объединяем части заголовка
            if title_parts:
                return ", ".join(title_parts)
                
        except Exception as e:
            logger.error(f"Ошибка при извлечении заголовка: {e}")
        
        # Запасной вариант
        return "Квартира на Циан"
    
    def _extract_price(self, item: Dict) -> str:
        """Извлекает цену объявления из JSON-данных."""
        try:
            # Проверяем разные структуры данных
            price = None
            currency = "₽/мес."
            
            # Вариант 1: цена в bargainTerms
            if 'bargainTerms' in item and 'price' in item['bargainTerms']:
                price = item['bargainTerms']['price']
            
            # Вариант 2: цена в корне объекта
            elif 'price' in item:
                price = item['price']
            
            # Вариант 3: цена в details
            elif 'details' in item and 'price' in item['details']:
                price = item['details']['price']
            
            if price:
                return f"{price:,} {currency}".replace(',', ' ')
        except Exception as e:
            logger.error(f"Ошибка при извлечении цены: {e}")
        
        return "Цена не указана"
    
    def _extract_metro_info(self, item: Dict) -> Dict[str, str]:
        """Извлекает информацию о метро из JSON-данных."""
        result = {
            'metro_name': '',
            'metro_time': '',
            'transport_type': ''
        }
        
        try:
            # Вариант 1: Ищем информацию о метро в специальных полях
            if 'geo' in item and 'undergrounds' in item['geo'] and item['geo']['undergrounds']:
                undergrounds = item['geo']['undergrounds']
                if isinstance(undergrounds, list) and undergrounds:
                    first_metro = undergrounds[0]
                    
                    if 'name' in first_metro:
                        result['metro_name'] = first_metro['name']
                    
                    if 'time' in first_metro:
                        result['metro_time'] = f"{first_metro['time']}"
                    
                    if 'transportType' in first_metro:
                        transport_type = first_metro.get('transportType')
                        if transport_type == 'walk':
                            result['transport_type'] = 'пешком'
                        else:
                            result['transport_type'] = 'на транспорте'
            
            # Если метро не найдено, ищем в других полях
            if not result['metro_name'] and 'geo' in item:
                geo = item['geo']
                
                # Ищем в поле metroStation
                if 'metroStation' in geo and geo['metroStation']:
                    result['metro_name'] = geo['metroStation']
                
                # Ищем в поле metroTime
                if 'metroTime' in geo and geo['metroTime']:
                    result['metro_time'] = f"{geo['metroTime']}"
                
                # Ищем в поле metroTransportType
                if 'metroTransportType' in geo:
                    transport_type = geo.get('metroTransportType')
                    if transport_type == 'walk':
                        result['transport_type'] = 'пешком'
                    else:
                        result['transport_type'] = 'на транспорте'
        
        except Exception as e:
            logger.error(f"Ошибка при извлечении информации о метро: {e}")
        
        return result
    
    def _extract_date(self, item: Dict) -> str:
        """Извлекает дату публикации объявления из JSON-данных."""
        try:
            # Ищем поле с датой в разных форматах
            
            # Вариант 1: addedTimestamp (Unix timestamp)
            if 'addedTimestamp' in item and item['addedTimestamp']:
                timestamp = item['addedTimestamp']
                if isinstance(timestamp, int) or isinstance(timestamp, float):
                    date_obj = datetime.fromtimestamp(int(timestamp))
                    return date_obj.strftime('%d.%m.%Y %H:%M')
            
            # Вариант 2: creationDate (строка с датой)
            if 'creationDate' in item and item['creationDate']:
                return item['creationDate']
            
            # Вариант 3: publishedDate (строка с датой)
            if 'publishedDate' in item and item['publishedDate']:
                return item['publishedDate']
                
            # Вариант 4: просто поле date
            if 'date' in item and item['date']:
                return item['date']
                
        except Exception as e:
            logger.error(f"Ошибка при извлечении даты: {e}")
        
        return "Недавно опубликовано"
    
    def _extract_description(self, item: Dict) -> str:
        """Извлекает описание объявления из JSON-данных."""
        try:
            # Ищем описание в разных местах объекта
            
            # Вариант 1: поле description
            if 'description' in item and item['description']:
                return item['description']
                
            # Вариант 2: описание в details
            if 'details' in item and 'description' in item['details'] and item['details']['description']:
                return item['details']['description']
                
            # Вариант 3: в других возможных полях
            for key in item:
                if 'description' in key.lower() and isinstance(item[key], str) and len(item[key]) > 10:
                    return item[key]
                    
            # Ищем другие текстовые поля, которые могут содержать описание
            if 'notes' in item and isinstance(item['notes'], str) and len(item['notes']) > 10:
                return item['notes']
                
            if 'text' in item and isinstance(item['text'], str) and len(item['text']) > 10:
                return item['text']
                
        except Exception as e:
            logger.error(f"Ошибка при извлечении описания: {e}")
        
        return "Для получения полного описания откройте объявление"
    
    def _extract_photos(self, item: Dict) -> List[str]:
        """Извлекает URL фотографий объявления из JSON-данных."""
        photos = []
        
        try:
            # Полный список возможных путей к фотографиям в JSON
            if 'photos' in item and isinstance(item['photos'], list):
                for photo in item['photos']:
                    if isinstance(photo, dict):
                        if 'fullUrl' in photo:
                            photos.append(photo['fullUrl'])
                        elif 'url' in photo:
                            photos.append(photo['url'])
            
            # Фото в объекте offer
            elif 'offer' in item and 'photos' in item['offer'] and isinstance(item['offer']['photos'], list):
                for photo in item['offer']['photos']:
                    if isinstance(photo, dict):
                        if 'fullUrl' in photo:
                            photos.append(photo['fullUrl'])
                        elif 'url' in photo:
                            photos.append(photo['url'])
            
            # Одиночный URL фото
            elif 'fullUrl' in item:
                photos.append(item['fullUrl'])
            elif 'url' in item:
                photos.append(item['url'])
                
            # Фото в объекте main
            elif 'main' in item and 'photos' in item['main']:
                for photo in item['main']['photos']:
                    if isinstance(photo, dict):
                        if 'fullUrl' in photo:
                            photos.append(photo['fullUrl'])
                        elif 'url' in photo:
                            photos.append(photo['url'])
            
            # Фото в photoUrls
            elif 'photoUrls' in item and isinstance(item['photoUrls'], list):
                photos.extend(item['photoUrls'])
                
            # Thumbnails как вариант
            elif 'thumbnails' in item and isinstance(item['thumbnails'], list):
                for thumb in item['thumbnails']:
                    if isinstance(thumb, dict) and 'url' in thumb:
                        photos.append(thumb['url'])
                
            # Фото в images
            elif 'images' in item and isinstance(item['images'], list):
                for image in item['images']:
                    if isinstance(image, dict) and 'url' in image:
                        photos.append(image['url'])
                    elif isinstance(image, str):
                        photos.append(image)
            
            # Поиск по ключам, содержащим слово "photo" или "image"
            if not photos:
                for key in item:
                    if ('photo' in key.lower() or 'image' in key.lower()) and isinstance(item[key], str):
                        photos.append(item[key])
                        break
                    elif ('photo' in key.lower() or 'image' in key.lower()) and isinstance(item[key], list):
                        for img in item[key]:
                            if isinstance(img, str):
                                photos.append(img)
                            elif isinstance(img, dict) and 'url' in img:
                                photos.append(img['url'])
                        break
            
            # Логируем найденные фото
            if photos:
                logger.info(f"Найдено {len(photos)} фотографий")
                
                # Улучшаем URL фотографий, убирая параметры ресайза
                for i in range(len(photos)):
                    photos[i] = re.sub(r'w=\d+&h=\d+', '', photos[i])
                    photos[i] = re.sub(r'resize=\d+x\d+', '', photos[i])
            
        except Exception as e:
            logger.error(f"Ошибка при извлечении фотографий: {e}")
            logger.error(traceback.format_exc())
        
        # Ограничиваем количество фотографий до MAX_PHOTOS
        return photos[:MAX_PHOTOS]
    
    def parse_offers_html(self, html: str) -> List[Dict[str, Any]]:
        """Парсит объявления со страницы через HTML-селекторы."""
        offers = []
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Список селекторов для поиска карточек объявлений
            card_selectors = [
                'article[data-name="CardComponent"]',
                'div[data-name="CardComponent"]',
                'div[data-testid="offer-card"]',
                'div._93444fe79c--card--ibP42',
                'div.c6e8ba5398--main-info--oWcMk',
                'div[class*="--item--"]'
            ]
            
            # Ищем карточки объявлений по списку селекторов
            cards = []
            used_selector = None
            
            for selector in card_selectors:
                cards = soup.select(selector)
                if cards:
                    used_selector = selector
                    logger.info(f"Найдены объявления по селектору: {selector}, количество: {len(cards)}")
                    break
            
            if not cards:
                logger.warning("Не удалось найти карточки объявлений по известным селекторам")
                return []
            
            # Обрабатываем каждую карточку
            for card in cards:
                try:
                    # Извлекаем ID объявления
                    offer_id = None
                    
                    # Вариант 1: из атрибута data-id
                    if card.has_attr('data-id'):
                        offer_id = card['data-id']
                    
                    # Вариант 2: из ссылки
                    if not offer_id:
                        for a_tag in card.select('a[href*="/rent/flat/"]'):
                            href = a_tag.get('href', '')
                            id_match = re.search(r'/rent/flat/(\d+)/', href)
                            if id_match:
                                offer_id = id_match.group(1)
                                break
                    
                    # Пропускаем, если ID не найден
                    if not offer_id:
                        continue
                    
                    # Извлекаем ссылку на объявление
                    link = None
                    link_tag = card.select_one('a[href*="/rent/flat/"]')
                    if link_tag and link_tag.has_attr('href'):
                        link = link_tag['href']
                        if not link.startswith('http'):
                            link = f"https://www.cian.ru{link}"
                    else:
                        link = f"https://www.cian.ru/rent/flat/{offer_id}/"
                    
                    # Извлекаем заголовок
                    title = None
                    
                    # Список селекторов для заголовка
                    title_selectors = [
                        'span[data-mark="OfferTitle"]',
                        'span[data-testid="offer-card-title"]',
                        'div[data-testid="offer-title"]',
                        'div.c6e8ba5398--title--V_HFz'
                    ]
                    
                    for selector in title_selectors:
                        title_element = card.select_one(selector)
                        if title_element:
                            title = title_element.text.strip()
                            break
                    
                    # Альтернативный вариант: собираем из характеристик
                    if not title:
                        title_parts = []
                        
                        # Ищем данные о количестве комнат
                        rooms_element = card.select_one('div[data-testid="object-info"] > div:first-child')
                        if rooms_element:
                            title_parts.append(rooms_element.text.strip())
                        
                        # Ищем данные о площади
                        area_element = card.select_one('div[data-testid="object-info"] > div:nth-child(2)')
                        if area_element:
                            title_parts.append(area_element.text.strip())
                        
                        # Ищем данные об этаже
                        floor_element = card.select_one('div[data-testid="object-info"] > div:nth-child(3)')
                        if floor_element:
                            title_parts.append(floor_element.text.strip())
                        
                        if title_parts:
                            title = ", ".join(title_parts)
                    
                    # Если заголовок не найден, используем значение по умолчанию
                    if not title:
                        title = "Квартира на Циан"
                    
                    # Извлекаем цену
                    price = None
                    
                    # Список селекторов для цены
                    price_selectors = [
                        'span[data-mark="MainPrice"]',
                        'div[data-testid="price-container"]',
                        'div.c6e8ba5398--header--gZDnA span',
                        'span[data-testid="price-term"]'
                    ]
                    
                    for selector in price_selectors:
                        price_element = card.select_one(selector)
                        if price_element:
                            price = price_element.text.strip()
                            break
                    
                    # Если цена не найдена, используем значение по умолчанию
                    if not price:
                        price = "Цена не указана"
                    
                    # Извлекаем описание (обычно отсутствует в карточках)
                    description = None
                    
                    # Список селекторов для описания
                    description_selectors = [
                        'div[data-name="Description"]',
                        'div[data-testid="description"]',
                        'div.c6e8ba5398--description--YNhai',
                        'p.c6e8ba5398--description-text--YNhai'
                    ]
                    
                    for selector in description_selectors:
                        description_element = card.select_one(selector)
                        if description_element:
                            description = description_element.text.strip()
                            break
                    
                    # Если описание не найдено, используем значение по умолчанию
                    if not description:
                        description = "Для получения полного описания откройте объявление."
                    
                    # Извлекаем фотографии
                    photos = []
                    
                    # Список селекторов для фотографий
                    photo_selectors = [
                        'img[data-testid="offer-card-photo"]',
                        'img.c6e8ba5398--img--pLTgk',
                        'img[itemprop="image"]',
                        'img[data-name="PhotoSlider"]',
                        'img[alt*="фото"]',
                        'img[src*="cian.ru"]'
                    ]
                    
                    for selector in photo_selectors:
                        photo_elements = card.select(selector)
                        for photo in photo_elements:
                            if photo.has_attr('src'):
                                img_url = photo['src']
                                # Удаляем параметры ресайза для получения полного изображения
                                img_url = re.sub(r'w=\d+&h=\d+', '', img_url)
                                img_url = re.sub(r'resize=\d+x\d+', '', img_url)
                                photos.append(img_url)
                        if photos:
                            break
                    
                    # Ищем фото в стилях background-image
                    if not photos:
                        for element in card.select('[style*="background-image"]'):
                            style = element.get('style', '')
                            url_match = re.search(r'background-image:\s*url\([\'"]?(.*?)[\'"]?\)', style)
                            if url_match:
                                photos.append(url_match.group(1))
                    
                    # Создаем объект объявления
                    offer = {
                        'id': offer_id,
                        'title': title,
                        'price': price,
                        'link': link,
                        'description': description,
                        'photos': photos[:MAX_PHOTOS],  # Ограничиваем количество фото
                        'timestamp': datetime.now().isoformat()
                    }
                    
                    offers.append(offer)
                    
                except Exception as e:
                    logger.error(f"Ошибка при парсинге HTML объявления: {e}")
                    logger.error(traceback.format_exc())
            
            logger.info(f"Извлечено {len(offers)} объявлений из HTML")
            
        except Exception as e:
            logger.error(f"Ошибка при парсинге HTML: {e}")
            logger.error(traceback.format_exc())
        
        return offers
    
    def parse_offers(self, html: str) -> List[Dict[str, Any]]:
        """Комбинированный метод парсинга объявлений."""
        # Сначала пробуем получить данные из JSON
        json_offers = self.extract_json_data(html)
        
        # Если не получилось, пробуем парсить HTML
        if not json_offers:
            logger.info("Не удалось получить данные из JSON, пробуем парсить HTML...")
            html_offers = self.parse_offers_html(html)
            return html_offers
        
        return json_offers

    def _filter_ad(self, data: dict) -> bool:
        """Фильтрует объявление по ключевым словам и цене."""
        all_content = f"{data['title']} {data['description']}".lower()
        
        # Проверка цены
        try:
            # Извлекаем числовое значение цены
            price_text = data["price"]
            price_digits = ''.join(filter(str.isdigit, price_text))
            price = int(price_digits) if price_digits else 0
            price_ok = self.min_price <= price <= self.max_price if self.max_price > 0 else True
        except (ValueError, TypeError):
            price_ok = True  # если не удалось получить цену, не фильтруем
            
        # Проверка ключевых слов
        kw_ok = True
        if self.keys_word:
            kw_ok = any(k.lower() in all_content for k in self.keys_word)
        if kw_ok and self.keys_black_word:
            kw_ok = not any(k.lower() in all_content for k in self.keys_black_word)
            
        return price_ok and kw_ok

    def send_notification(self, data: dict):
        """Отправка уведомления о новом объявлении через API Telegram."""
        if not self.tg_token or not self.chat_id:
            logger.info("Не удалось отправить уведомление: не настроены параметры")
            return

        try:
            # Получаем заголовок поиска
            search_title = f"🏙️ ЦИАН - {self.job_name}" if self.job_name else "ЦИАН"
            
            # Формируем заголовок сообщения
            caption = []
            
            # Название поиска (жирным)
            caption.append(f"*{search_title}*")
            
            # Название объявления (жирным)
            caption.append(f"*{data.get('title', '-')}*")
            
            # Цена с emoji
            caption.append(f"💰 {data.get('price', 'Цена не указана')}")
            
            # Ссылка на объявление с emoji
            caption.append(f"🔗 [Ссылка на объявление]({data.get('link', '')})")
            
            # Описание
            if description := data.get('description'):
                # Ограничиваем длину описания
                max_desc_length = 900
                if len(description) > max_desc_length:
                    description = description[:max_desc_length] + "..."
                caption.append(f"📝 {description}")
            
            # Собираем текст сообщения с переносами строк между секциями
            # Между названием поиска и названием объявления нет пустой строки
            # Между названием объявления и ценой есть пустая строка
            # Между ценой и ссылкой нет пустой строки
            # Между ссылкой и описанием есть пустая строка
            text_parts = [caption[0], "\n", caption[1], "\n\n", caption[2], "\n", caption[3]]
            if len(caption) > 4:
                text_parts.extend(["\n\n", caption[4]])
            
            caption_text = "".join(text_parts)
            
            photos = data.get('photos', [])
            
            if photos:
                # Если есть фотографии, отправляем их (максимум MAX_PHOTOS)
                if len(photos) > 1:
                    # Отправляем как медиа-группу
                    media = []
                    for i, photo_url in enumerate(photos[:MAX_PHOTOS]):
                        # Только к первому фото добавляем описание
                        if i == 0:
                            media.append({
                                "type": "photo", 
                                "media": photo_url,
                                "caption": caption_text,
                                "parse_mode": "Markdown"
                            })
                        else:
                            media.append({"type": "photo", "media": photo_url})
                    
                    url = f"https://api.telegram.org/bot{self.tg_token}/sendMediaGroup"
                    response = requests.post(url, json={
                        "chat_id": self.chat_id,
                        "media": media
                    })
                else:
                    # Отправляем одно фото с подписью
                    url = f"https://api.telegram.org/bot{self.tg_token}/sendPhoto"
                    response = requests.post(url, json={
                        "chat_id": self.chat_id,
                        "photo": photos[0],
                        "caption": caption_text,
                        "parse_mode": "Markdown"
                    })
            else:
                # Если нет фотографий, отправляем текстовое сообщение
                url = f"https://api.telegram.org/bot{self.tg_token}/sendMessage"
                response = requests.post(url, json={
                    "chat_id": self.chat_id,
                    "text": caption_text,
                    "parse_mode": "Markdown",
                    "disable_web_page_preview": False
                })
            
            if response.status_code == 200:
                logger.info(f"Отправлено уведомление об объявлении: {data.get('title')} - {data.get('price')}")
                self.total_notified_ads += 1
                
                # Сохраняем объявление в базе данных
                try:
                    # Извлекаем числовую часть цены
                    price_str = data.get('price', '0')
                    price_digits = ''.join(filter(str.isdigit, price_str))
                    price = int(price_digits) if price_digits else 0
                    
                    # Сохраняем запись в БД
                    self.db_handler.add_cian_record(
                        ad_id=data.get('id', '0'),
                        price=price,
                        url=data.get('link', ''),
                        title=data.get('title', '')
                    )
                except Exception as e:
                    logger.error(f"Ошибка при сохранении объявления в БД: {e}")
            else:
                logger.error(f"Ошибка при отправке уведомления: {response.text}")
        except Exception as e:
            logger.error(f"Ошибка при отправке уведомления: {e}")
            logger.error(traceback.format_exc())

    def check_stop_event(self) -> None:
        """Проверяет сигнал остановки и выбрасывает исключение если он установлен."""
        if self.stop_event.is_set():
            raise StopEventException()

    def get_statistics(self) -> Dict[str, int]:
        """Возвращает статистику сканирования."""
        return {
            "total_new_ads": self.total_new_ads,
            "total_notified_ads": self.total_notified_ads
        }

    def parse(self) -> None:
        """Основной метод парсинга."""
        try:
            # Очищаем список текущего сканирования
            self.current_scan_ads = set()
            
            # Обрабатываем каждый базовый URL
            for base_url in self.url_list:
                if self.stop_event.is_set():
                    return
                
                try:
                    # Определяем количество страниц для сканирования
                    pages_to_scan = max(1, self.count)
                    logger.info(f"ЦИАН: Сканирование {pages_to_scan} страниц для {base_url}")
                    
                    # Обрабатываем все указанные страницы
                    for page_num in range(1, pages_to_scan + 1):
                        if self.stop_event.is_set():
                            return
                        
                        # Формируем URL страницы с пагинацией
                        page_url = base_url
                        if page_num > 1:
                            # Добавляем или заменяем параметр страницы в URL
                            if "p=" in base_url:
                                page_url = re.sub(r'p=\d+', f'p={page_num}', base_url)
                            else:
                                separator = "&" if "?" in base_url else "?"
                                page_url = f"{base_url}{separator}p={page_num}"
                            
                        logger.info(f"ЦИАН: Обработка страницы {page_num}/{pages_to_scan}: {page_url}")
                        
                        # Получаем HTML-страницу
                        html = self.get_page(page_url)
                        if not html:
                            logger.error(f"Не удалось получить страницу {page_url}")
                            continue
                        
                        # Парсим объявления
                        ads = self.parse_offers(html)
                        logger.info(f"Найдено {len(ads)} объявлений на странице {page_num}")
                        
                        # Сохраняем ID объявлений текущего сканирования
                        for ad in ads:
                            ad_id = ad['id']
                            self.current_scan_ads.add(ad_id)
                        
                        # Обрабатываем новые объявления
                        if not self.first_run:
                            for ad in ads:
                                ad_id = ad['id']
                                # Проверяем, новое ли это объявление
                                if ad_id not in self.known_ads:
                                    # Увеличиваем счетчик новых объявлений
                                    self.total_new_ads += 1
                                    logger.info(f"Найдено новое объявление: {ad['title']} (ID: {ad_id})")
                                    
                                    # Получаем числовую часть цены
                                    try:
                                        price_str = ad.get('price', '0')
                                        price_digits = ''.join(filter(str.isdigit, price_str))
                                        price = int(price_digits) if price_digits else 0
                                        
                                        # Дополнительно проверяем через БД для избежания дубликатов
                                        if not self.db_handler.cian_record_exists(ad_id, price):
                                            # Проверяем условия фильтрации
                                            if self._filter_ad(ad):
                                                # Отправляем уведомление
                                                self.send_notification(ad)
                                    except Exception as e:
                                        logger.error(f"Ошибка при проверке цены объявления: {e}")
                        
                        # Короткая пауза между запросами страниц
                        time.sleep(random.uniform(2, 4))
                        
                except StopEventException:
                    logger.info("ЦИАН: Парсинг остановлен по запросу")
                    return
                except Exception as e:
                    logger.error(f"Ошибка при обработке URL {base_url}: {e}")
                    logger.error(traceback.format_exc())
            
            # После обработки всех URL обновляем список известных объявлений
            self.known_ads.update(self.current_scan_ads)
            
            # Сохраняем результаты сканирования в БД
            self._save_scan_results()
            
            # Выводим информацию о результатах сканирования
            if self.first_run:
                logger.info(f"ЦИАН: Первичное сканирование завершено. Найдено объявлений: {len(self.current_scan_ads)}. "
                           f"При следующем запуске будут отображаться только новые объявления.")
            else:
                logger.info(f"ЦИАН: Сканирование завершено. Найдено {self.total_new_ads} новых объявлений, "
                           f"отправлено {self.total_notified_ads} уведомлений.")
        except Exception as e:
            logger.error(f"Общая ошибка при парсинге ЦИАН: {e}")
            logger.error(traceback.format_exc())
        finally:
            self.stop_event.clear()
            logger.info("ЦИАН: Парсинг завершен")