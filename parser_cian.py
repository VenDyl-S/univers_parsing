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

MAX_PHOTOS = 3

class CianParse:
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
        self.url_list = url
        self.keys_word = keysword_list or None
        self.keys_black_word = keysword_black_list or None
        self.count = count
        self.tg_token = tg_token
        self.chat_id = chat_id
        self.job_name = job_name
        self.max_price = int(max_price)
        self.min_price = int(min_price)
        self.pause = pause
        self.debug_mode = debug_mode
        self.proxy = proxy
        self.proxy_change_url = proxy_change_url
        self.first_run = first_run

        self.url: str | None = None
        self.stop_event = stop_event or threading.Event()
        
        self.db_handler = SQLiteDBHandler()
        
        self.known_ads: Set[str] = set()
        self.current_scan_ads: Set[str] = set()
        
        self.total_new_ads: int = 0
        self.total_notified_ads: int = 0
        
        self.session = requests.Session()
        self.update_headers()
        
        self._load_known_ads()

    def update_headers(self):
        try:
            with open("user_agent_pc.txt", "r") as f:
                user_agents = [line.strip() for line in f if line.strip()]
            user_agent = random.choice(user_agents)
        except Exception as e:
            logger.error(f"Ошибка при чтении файла user_agent_pc.txt: {e}")
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
        
        if self.proxy:
            logger.info(f"Используется прокси: {self.proxy}")
            self.session.proxies = {"http": self.proxy, "https": self.proxy}

    def _load_known_ads(self):
        for url in self.url_list:
            scan_ids = self.db_handler.get_cian_scan_ids(url)
            if scan_ids:
                self.known_ads.update(scan_ids)
        
        cian_viewed = self.db_handler.list_all_cian_records()
        for record in cian_viewed:
            if record and record[0]:
                self.known_ads.add(record[0])
        
        logger.info(f"ЦИАН: Загружено {len(self.known_ads)} известных объявлений из БД")
    
    def _save_scan_results(self):
        all_ads = self.known_ads.union(self.current_scan_ads)
        for url in self.url_list:
            self.db_handler.save_cian_scan_ids(url, list(all_ads))
        
        logger.info(f"ЦИАН: Сохранено {len(all_ads)} объявлений в БД")
    
    def get_page(self, url: str) -> Optional[str]:
        self.update_headers()
        
        try:
            time.sleep(random.uniform(2, 4))
            
            response = self.session.get(url, headers=self.headers, timeout=15)
            response.raise_for_status()
            
            if self.debug_mode:
                with open('last_response.html', 'w', encoding='utf-8') as f:
                    f.write(response.text)
                logger.info(f"Сохранён последний ответ в last_response.html")
            
            return response.text
        except requests.RequestException as e:
            logger.error(f"Ошибка при запросе: {e}")
            
            if self.proxy and ("timeout" in str(e).lower() or "connection" in str(e).lower()):
                logger.warning("Таймаут или проблема с соединением. Попытка смены IP...")
                if self.change_ip():
                    logger.info("IP успешно сменен, повторяем запрос...")
                    self.update_headers()
                    time.sleep(random.uniform(3, 5))
                    return self.get_page(url)
            
            return None

    def extract_json_data(self, html: str) -> List[Dict[str, Any]]:
        offers = []
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            json_script_pattern = re.compile(r'window\._cianConfig\s*=\s*({.*?});', re.DOTALL)
            initialdata_pattern = re.compile(r'window\.__initialData\s*=\s*({.*?});', re.DOTALL)
            
            scripts = soup.find_all('script')
            
            json_data = None
            config_data = None
            
            for script in scripts:
                if script.string and 'window._cianConfig' in script.string:
                    match = json_script_pattern.search(script.string)
                    if match:
                        try:
                            config_data = json.loads(match.group(1))
                            
                            if 'data' in config_data and 'offerSearch' in config_data['data']:
                                if 'results' in config_data['data']['offerSearch']:
                                    json_data = config_data['data']['offerSearch']['results']
                                    logger.info(f"Найдены данные в config_data['data']['offerSearch']['results']")
                                    break
                            
                            if 'results' in config_data:
                                json_data = config_data['results']
                                logger.info(f"Найдены данные в config_data['results']")
                                break
                        except json.JSONDecodeError:
                            continue
            
            if not json_data:
                for script in scripts:
                    if script.string and 'window.__initialData' in script.string:
                        match = initialdata_pattern.search(script.string)
                        if match:
                            try:
                                initial_data = json.loads(match.group(1))
                                
                                for key, value in initial_data.items():
                                    if isinstance(value, dict) and 'value' in value and 'results' in value['value']:
                                        json_data = value['value']['results']
                                        logger.info(f"Найдены данные в initialData['{key}']['value']['results']")
                                        break
                            except json.JSONDecodeError:
                                continue
            
            if config_data and self.debug_mode:
                with open('cian_config_data.json', 'w', encoding='utf-8') as f:
                    json.dump(config_data, f, ensure_ascii=False, indent=2)
            
            if json_data:
                logger.info(f"Найдено {len(json_data)} объявлений в JSON данных")
                
                if self.debug_mode:
                    with open('cian_json_data.json', 'w', encoding='utf-8') as f:
                        json.dump(json_data, f, ensure_ascii=False, indent=2)
                
                for item in json_data:
                    try:
                        offer_id = item.get('id')
                        if not offer_id:
                            continue
                        
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
        title_parts = []
        
        try:
            if 'title' in item and item['title']:
                return item['title']
            
            room_count = None
            room_type = None
            
            if 'roomsCount' in item:
                room_count = item['roomsCount']
            elif 'details' in item and 'roomsCount' in item['details']:
                room_count = item['details']['roomsCount']
            
            if room_count is not None:
                if room_count == 0:
                    room_type = 'Студия'
                elif room_count > 0:
                    room_type = f"{room_count}-комн. квартира"
            
            if room_type:
                title_parts.append(room_type)
            
            area = None
            if 'totalArea' in item:
                area = item['totalArea']
            elif 'details' in item and 'area' in item['details']:
                area = item['details']['area']
            
            if area:
                title_parts.append(f"{area} м²")
            
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
            
            if title_parts:
                return ", ".join(title_parts)
                
        except Exception as e:
            logger.error(f"Ошибка при извлечении заголовка: {e}")
        
        return "Квартира на Циан"
    
    def _extract_price(self, item: Dict) -> str:
        try:
            price = None
            currency = "₽/мес."
            
            if 'bargainTerms' in item and 'price' in item['bargainTerms']:
                price = item['bargainTerms']['price']
            
            elif 'price' in item:
                price = item['price']
            
            elif 'details' in item and 'price' in item['details']:
                price = item['details']['price']
            
            if price:
                return f"{price:,} {currency}".replace(',', ' ')
        except Exception as e:
            logger.error(f"Ошибка при извлечении цены: {e}")
        
        return "Цена не указана"
    
    def _extract_metro_info(self, item: Dict) -> Dict[str, str]:
        result = {
            'metro_name': '',
            'metro_time': '',
            'transport_type': ''
        }
        
        try:
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
            
            if not result['metro_name'] and 'geo' in item:
                geo = item['geo']
                
                if 'metroStation' in geo and geo['metroStation']:
                    result['metro_name'] = geo['metroStation']
                
                if 'metroTime' in geo and geo['metroTime']:
                    result['metro_time'] = f"{geo['metroTime']}"
                
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
        try:
            if 'addedTimestamp' in item and item['addedTimestamp']:
                timestamp = item['addedTimestamp']
                if isinstance(timestamp, int) or isinstance(timestamp, float):
                    date_obj = datetime.fromtimestamp(int(timestamp))
                    return date_obj.strftime('%d.%m.%Y %H:%M')
            
            if 'creationDate' in item and item['creationDate']:
                return item['creationDate']
            
            if 'publishedDate' in item and item['publishedDate']:
                return item['publishedDate']
                
            if 'date' in item and item['date']:
                return item['date']
                
        except Exception as e:
            logger.error(f"Ошибка при извлечении даты: {e}")
        
        return "Недавно опубликовано"
    
    def _extract_description(self, item: Dict) -> str:
        try:
            if 'description' in item and item['description']:
                return item['description']
                
            if 'details' in item and 'description' in item['details'] and item['details']['description']:
                return item['details']['description']
                
            for key in item:
                if 'description' in key.lower() and isinstance(item[key], str) and len(item[key]) > 10:
                    return item[key]
                    
            if 'notes' in item and isinstance(item['notes'], str) and len(item['notes']) > 10:
                return item['notes']
                
            if 'text' in item and isinstance(item['text'], str) and len(item['text']) > 10:
                return item['text']
                
        except Exception as e:
            logger.error(f"Ошибка при извлечении описания: {e}")
        
        return "Для получения полного описания откройте объявление"
    
    def _extract_photos(self, item: Dict) -> List[str]:
        photos = []
        
        try:
            if 'photos' in item and isinstance(item['photos'], list):
                for photo in item['photos']:
                    if isinstance(photo, dict):
                        if 'fullUrl' in photo:
                            photos.append(photo['fullUrl'])
                        elif 'url' in photo:
                            photos.append(photo['url'])
            
            elif 'offer' in item and 'photos' in item['offer'] and isinstance(item['offer']['photos'], list):
                for photo in item['offer']['photos']:
                    if isinstance(photo, dict):
                        if 'fullUrl' in photo:
                            photos.append(photo['fullUrl'])
                        elif 'url' in photo:
                            photos.append(photo['url'])
            
            elif 'fullUrl' in item:
                photos.append(item['fullUrl'])
            elif 'url' in item:
                photos.append(item['url'])
                
            elif 'main' in item and 'photos' in item['main']:
                for photo in item['main']['photos']:
                    if isinstance(photo, dict):
                        if 'fullUrl' in photo:
                            photos.append(photo['fullUrl'])
                        elif 'url' in photo:
                            photos.append(photo['url'])
            
            elif 'photoUrls' in item and isinstance(item['photoUrls'], list):
                photos.extend(item['photoUrls'])
                
            elif 'thumbnails' in item and isinstance(item['thumbnails'], list):
                for thumb in item['thumbnails']:
                    if isinstance(thumb, dict) and 'url' in thumb:
                        photos.append(thumb['url'])
                
            elif 'images' in item and isinstance(item['images'], list):
                for image in item['images']:
                    if isinstance(image, dict) and 'url' in image:
                        photos.append(image['url'])
                    elif isinstance(image, str):
                        photos.append(image)
            
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
            
            if photos:
                logger.info(f"Найдено {len(photos)} фотографий")
                
                for i in range(len(photos)):
                    photos[i] = re.sub(r'w=\d+&h=\d+', '', photos[i])
                    photos[i] = re.sub(r'resize=\d+x\d+', '', photos[i])
            
        except Exception as e:
            logger.error(f"Ошибка при извлечении фотографий: {e}")
            logger.error(traceback.format_exc())
        
        return photos[:MAX_PHOTOS]
    
    def parse_offers_html(self, html: str) -> List[Dict[str, Any]]:
        offers = []
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            card_selectors = [
                'article[data-name="CardComponent"]',
                'div[data-name="CardComponent"]',
                'div[data-testid="offer-card"]',
                'div._93444fe79c--card--ibP42',
                'div.c6e8ba5398--main-info--oWcMk',
                'div[class*="--item--"]'
            ]
            
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
            
            for card in cards:
                try:
                    offer_id = None
                    
                    if card.has_attr('data-id'):
                        offer_id = card['data-id']
                    
                    if not offer_id:
                        for a_tag in card.select('a[href*="/rent/flat/"]'):
                            href = a_tag.get('href', '')
                            id_match = re.search(r'/rent/flat/(\d+)/', href)
                            if id_match:
                                offer_id = id_match.group(1)
                                break
                    
                    if not offer_id:
                        continue
                    
                    link = None
                    link_tag = card.select_one('a[href*="/rent/flat/"]')
                    if link_tag and link_tag.has_attr('href'):
                        link = link_tag['href']
                        if not link.startswith('http'):
                            link = f"https://www.cian.ru{link}"
                    else:
                        link = f"https://www.cian.ru/rent/flat/{offer_id}/"
                    
                    title = None
                    
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
                    
                    if not title:
                        title_parts = []
                        
                        rooms_element = card.select_one('div[data-testid="object-info"] > div:first-child')
                        if rooms_element:
                            title_parts.append(rooms_element.text.strip())
                        
                        area_element = card.select_one('div[data-testid="object-info"] > div:nth-child(2)')
                        if area_element:
                            title_parts.append(area_element.text.strip())
                        
                        floor_element = card.select_one('div[data-testid="object-info"] > div:nth-child(3)')
                        if floor_element:
                            title_parts.append(floor_element.text.strip())
                        
                        if title_parts:
                            title = ", ".join(title_parts)
                    
                    if not title:
                        title = "Квартира на Циан"
                    
                    price = None
                    
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
                    
                    if not price:
                        price = "Цена не указана"
                    
                    description = None
                    
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
                    
                    if not description:
                        description = "Для получения полного описания откройте объявление."
                    
                    photos = []
                    
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
                                img_url = re.sub(r'w=\d+&h=\d+', '', img_url)
                                img_url = re.sub(r'resize=\d+x\d+', '', img_url)
                                photos.append(img_url)
                        if photos:
                            break
                    
                    if not photos:
                        for element in card.select('[style*="background-image"]'):
                            style = element.get('style', '')
                            url_match = re.search(r'background-image:\s*url\([\'"]?(.*?)[\'"]?\)', style)
                            if url_match:
                                photos.append(url_match.group(1))
                    
                    offer = {
                        'id': offer_id,
                        'title': title,
                        'price': price,
                        'link': link,
                        'description': description,
                        'photos': photos[:MAX_PHOTOS],
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
        json_offers = self.extract_json_data(html)
        
        if not json_offers:
            logger.info("Не удалось получить данные из JSON, пробуем парсить HTML...")
            html_offers = self.parse_offers_html(html)
            return html_offers
        
        return json_offers

    def _filter_ad(self, data: dict) -> bool:
        all_content = f"{data['title']} {data['description']}".lower()
        
        try:
            price_text = data["price"]
            price_digits = ''.join(filter(str.isdigit, price_text))
            price = int(price_digits) if price_digits else 0
            price_ok = self.min_price <= price <= self.max_price if self.max_price > 0 else True
        except (ValueError, TypeError):
            price_ok = True
            
        kw_ok = True
        if self.keys_word:
            kw_ok = any(k.lower() in all_content for k in self.keys_word)
        if kw_ok and self.keys_black_word:
            kw_ok = not any(k.lower() in all_content for k in self.keys_black_word)
            
        return price_ok and kw_ok

    def send_notification(self, data: dict):
        if not self.tg_token or not self.chat_id:
            logger.info("Не удалось отправить уведомление: не настроены параметры")
            return

        try:
            search_title = f"🏙️ ЦИАН - {self.job_name}" if self.job_name else "🏙️ ЦИАН"
            
            caption = []
            
            caption.append(f"*{search_title}*")
            caption.append(f"💡 *{data.get('title', '-')}*")
            caption.append(f"💰 {data.get('price', 'Цена не указана')}")
            caption.append(f"🔗 [Ссылка на объявление]({data.get('link', '')})")
            
            if description := data.get('description'):
                max_desc_length = 900
                if len(description) > max_desc_length:
                    description = description[:max_desc_length] + "..."
                caption.append(f"📝 {description}")
            
            text_parts = [caption[0], "\n", caption[1], "\n\n", caption[2], "\n", caption[3]]
            if len(caption) > 4:
                text_parts.extend(["\n\n", caption[4]])
            
            caption_text = "".join(text_parts)
            
            photos = data.get('photos', [])
            
            if photos:
                if len(photos) > 1:
                    media = []
                    for i, photo_url in enumerate(photos[:MAX_PHOTOS]):
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
                    url = f"https://api.telegram.org/bot{self.tg_token}/sendPhoto"
                    response = requests.post(url, json={
                        "chat_id": self.chat_id,
                        "photo": photos[0],
                        "caption": caption_text,
                        "parse_mode": "Markdown"
                    })
            else:
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
                
                try:
                    price_str = data.get('price', '0')
                    price_digits = ''.join(filter(str.isdigit, price_str))
                    price = int(price_digits) if price_digits else 0
                    
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
        if self.stop_event.is_set():
            raise StopEventException()
            
    def change_ip(self) -> bool:
        if not self.proxy:
            return False
        
        proxy_change_url = None
        try:
            if hasattr(self, 'proxy_change_url') and self.proxy_change_url:
                proxy_change_url = self.proxy_change_url
                
            elif self.chat_id:
                from db_service import SQLiteDBHandler
                db = SQLiteDBHandler()
                proxy_change_url = db.get_setting(self.chat_id, "proxy_change_url")
        except Exception as e:
            logger.error(f"Ошибка при получении URL смены IP: {e}")
            return False
        
        if not proxy_change_url:
            logger.warning("URL для смены IP не указан")
            return False
        
        try:
            logger.info(f"Отправка запроса на смену IP: {proxy_change_url}")
            res = requests.get(proxy_change_url, timeout=30)
            if res.status_code != 200:
                logger.error(f"Ошибка при запросе смены IP. Код ответа: {res.status_code}")
                return False
                
            logger.info(f"Ответ сервера при смене IP: {res.text.strip()}")
            
            time.sleep(5)
            
            try:
                test_session = requests.Session()
                if ":" in self.proxy and "@" in self.proxy:
                    test_session.proxies = {
                        "http": f"http://{self.proxy}",
                        "https": f"http://{self.proxy}"
                    }
                elif self.proxy.count(":") == 3:
                    parts = self.proxy.split(":")
                    if len(parts) == 4:
                        host, port, username, password = parts
                        formatted_proxy = f"{username}:{password}@{host}:{port}"
                        test_session.proxies = {
                            "http": f"http://{formatted_proxy}",
                            "https": f"http://{formatted_proxy}"
                        }
                
                self.session.proxies = test_session.proxies.copy()
                
                logger.info("Смена IP выполнена успешно")
                return True
            except Exception as e:
                logger.error(f"Ошибка при проверке нового IP: {e}")
                return False
                
        except Exception as err:
            logger.error(f"Не удалось сменить IP: {err}")
            return False

    def get_statistics(self) -> Dict[str, int]:
        return {
            "total_new_ads": self.total_new_ads,
            "total_notified_ads": self.total_notified_ads
        }

    def parse(self) -> None:
        try:
            self.current_scan_ads = set()
            
            for base_url in self.url_list:
                if self.stop_event.is_set():
                    return
                
                try:
                    pages_to_scan = max(1, self.count)
                    logger.info(f"ЦИАН: Сканирование {pages_to_scan} страниц для {base_url}")
                    
                    for page_num in range(1, pages_to_scan + 1):
                        if self.stop_event.is_set():
                            return
                        
                        page_url = base_url
                        if page_num > 1:
                            if "p=" in base_url:
                                page_url = re.sub(r'p=\d+', f'p={page_num}', base_url)
                            else:
                                separator = "&" if "?" in base_url else "?"
                                page_url = f"{base_url}{separator}p={page_num}"
                            
                        logger.info(f"ЦИАН: Обработка страницы {page_num}/{pages_to_scan}: {page_url}")
                        
                        html = self.get_page(page_url)
                        if not html:
                            logger.error(f"Не удалось получить страницу {page_url}")
                            continue
                        
                        ads = self.parse_offers(html)
                        logger.info(f"Найдено {len(ads)} объявлений на странице {page_num}")
                        
                        for ad in ads:
                            ad_id = ad['id']
                            self.current_scan_ads.add(ad_id)
                        
                        if not self.first_run:
                            for ad in ads:
                                ad_id = ad['id']
                                if ad_id not in self.known_ads:
                                    self.total_new_ads += 1
                                    logger.info(f"Найдено новое объявление: {ad['title']} (ID: {ad_id})")
                                    
                                    try:
                                        price_str = ad.get('price', '0')
                                        price_digits = ''.join(filter(str.isdigit, price_str))
                                        price = int(price_digits) if price_digits else 0
                                        
                                        if not self.db_handler.cian_record_exists(ad_id, price):
                                            if self._filter_ad(ad):
                                                self.send_notification(ad)
                                    except Exception as e:
                                        logger.error(f"Ошибка при проверке цены объявления: {e}")
                        
                        time.sleep(random.uniform(2, 4))
                        
                except StopEventException:
                    logger.info("ЦИАН: Парсинг остановлен по запросу")
                    return
                except Exception as e:
                    logger.error(f"Ошибка при обработке URL {base_url}: {e}")
                    logger.error(traceback.format_exc())
            
            self.known_ads.update(self.current_scan_ads)
            
            self._save_scan_results()
            
            if self.first_run:
                logger.info(f"ЦИАН: Первичное сканирование завершено. Найдено объявлений: {len(self.current_scan_ads)}. При следующем запуске будут отображаться только новые объявления.")
            else:
                logger.info(f"ЦИАН: Сканирование завершено. Найдено {self.total_new_ads} новых объявлений, отправлено {self.total_notified_ads} уведомлений.")
        except Exception as e:
            logger.error(f"Общая ошибка при парсинге ЦИАН: {e}")
            logger.error(traceback.format_exc())
        finally:
            self.stop_event.clear()
            logger.info("ЦИАН: Парсинг завершен")
