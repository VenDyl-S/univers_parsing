import os
import random
import threading
import time
import re
from typing import Dict, Set, List, Tuple, Optional
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import requests
from notifiers import get_notifier
from selenium.webdriver.common.by import By
from seleniumbase import SB
from loguru import logger

from db_service import SQLiteDBHandler
from custom_exception import StopEventException
from locator import LocatorAvito
from dotenv import load_dotenv

load_dotenv()


class UserAgentRotator:
    _instance = None
    _current_index = 0
    _user_agents = []
    
    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(UserAgentRotator, cls).__new__(cls)
            try:
                with open("user_agent_pc.txt", "r") as f:
                    cls._user_agents = [line.strip() for line in f if line.strip()]
            except Exception as e:
                logger.error(f"Error reading user_agent_pc.txt: {e}")
                cls._user_agents = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36"]
        return cls._instance
    
    def get_next(self):
        if not self._user_agents:
            return "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36"
        
        user_agent = self._user_agents[self._current_index]
        self._current_index = (self._current_index + 1) % len(self._user_agents)
        return user_agent


class AvitoParse:
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
        geo: str | None = None,
        debug_mode: int = 0,
        need_more_info: int = 1,
        proxy: str | None = None,
        proxy_change_url: str | None = None,
        stop_event: threading.Event | None = None,
        max_views: int | None = None,
        fast_speed: int = 0,
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
        self.max_views = max_views if max_views and max_views != 0 else None
        self.geo = geo
        self.debug_mode = debug_mode
        self.need_more_info = need_more_info
        self.proxy = proxy
        self.proxy_change_url = proxy_change_url
        self.fast_speed = fast_speed
        self.first_run = first_run

        self.url: str | None = None
        self.stop_event = stop_event or threading.Event()
        self.db_handler = SQLiteDBHandler()
        
        self.known_ads: Set[str] = set()  
        self.current_scan_ads: Set[str] = set()  
        
        self.total_new_ads: int = 0  
        self.total_notified_ads: int = 0  
        
        self.tg_notifier = None
        if self.tg_token and self.chat_id:
            self._setup_telegram_notifications()
            
        self._load_known_ads()

    def _normalize_proxy(self, proxy_str: str) -> str:
        if not proxy_str:
            return ""
            
        if '@' in proxy_str:
            return proxy_str
            
        parts = proxy_str.split(':')
        if len(parts) == 4:
            host, port, username, password = parts
            return f"{username}:{password}@{host}:{port}"
            
        return proxy_str
            
    def _load_known_ads(self):
        for url in self.url_list:
            ads_ids = self.db_handler.get_scan_ids(url)
            if ads_ids:
                self.known_ads.update(ads_ids)
        
        logger.info(f"Загружено {len(self.known_ads)} известных объявлений из БД")
    
    def _save_scan_results(self):
        all_ads = self.known_ads.union(self.current_scan_ads)
        for url in self.url_list:
            self.db_handler.save_scan_ids(url, list(all_ads))
        
        logger.info(f"Сохранено {len(all_ads)} объявлений в БД")
    
    def _setup_telegram_notifications(self):
        self.tg_notifier = get_notifier("telegram")
        logger.info(f"Настроено уведомление для чата ID: {self.chat_id}")

    def send_notification_with_photo(self, data: dict):
        if not self.chat_id or not self.tg_token:
            logger.info("Не удалось отправить уведомление: не настроены параметры.")
            return

        try:
            search_title = f"🏠 Авито - {self.job_name}" if self.job_name else "🏠 Авито"
            
            message_text = f"*{search_title}*\n"
            message_text += f"💡 *{data.get('name', '-')}*\n\n"
            message_text += f"💰 *{data.get('price', '-')}₽*\n"
            message_text += f"🔗 [Ссылка на объявление]({data.get('url')})\n\n"
            
            if description := data.get('description'):
                max_desc_length = 200
                if len(description) > max_desc_length:
                    description = description[:max_desc_length] + "..."
                message_text += f"📝 {description}"
            
            url = f"https://api.telegram.org/bot{self.tg_token}/sendPhoto"
            
            if "image_url" in data and data["image_url"]:
                photo_url = data["image_url"]
            else:
                photo_url = "https://upload.wikimedia.org/wikipedia/commons/thumb/a/ac/No_image_available.svg/1024px-No_image_available.svg.png"
            
            payload = {
                "chat_id": self.chat_id,
                "photo": photo_url,
                "caption": message_text,
                "parse_mode": "markdown",
                "disable_web_page_preview": True
            }
            
            response = requests.post(url, data=payload)
            if response.status_code == 200:
                logger.info(f"Отправлено уведомление об объявлении: {data.get('name')} - {data.get('price')}₽")
                self.total_notified_ads += 1
            else:
                logger.error(f"Ошибка при отправке уведомления: {response.text}")
                self._send_text_notification(message_text)
        except Exception as e:
            logger.error(f"Ошибка при отправке уведомления: {e}")
            self._send_text_notification(message_text)

    def _send_text_notification(self, message: str):
        """Отправка текстового уведомления в Telegram."""
        if not self.tg_notifier:
            return
            
        try:
            result = self.tg_notifier.notify(
                message=message,
                token=self.tg_token,
                chat_id=self.chat_id,
                parse_mode="markdown",
                disable_web_page_preview=True
            )
            if result.status == "Success":
                self.total_notified_ads += 1
        except Exception as e:
            logger.error(f"Ошибка при отправке текстового уведомления: {e}")

    @property
    def use_proxy(self) -> bool:
        return bool(self.proxy and self.proxy_change_url)

    def ip_block(self) -> None:
        if self.use_proxy and self.change_ip():
            UserAgentRotator._instance._current_index = 0
            return
        
        logger.info("Блок IP. Использую паузу 5‑6 минут…")
        time.sleep(random.randint(300, 350))
        UserAgentRotator._instance._current_index = 0

    def __get_url(self, url: str) -> bool:
        if "&s=" not in url:
            url += "&s=104"

        logger.info(f"Открываю страницу: {url}")
        try:
            self.driver.open(url)

            if "Доступ ограничен" in self.driver.get_title():
                self.ip_block()
                return self.__get_url(url)
                
            return True
        except Exception as e:
            logger.error(f"Ошибка при открытии страницы {url}: {e}")
            return False

    def __parse_page(self, url: str) -> List[Dict]:
        ads_data = []
        logger.info(f"Парсинг страницы {url}...")
        
        try:
            if not self.__get_url(url):
                logger.error(f"Не удалось загрузить страницу {url}")
                return ads_data
                
            is_rent_page = "/kvartiry/sdam/" in url.lower()
            is_sell_page = "/kvartiry/prodam/" in url.lower()
            is_apartments_page = is_rent_page or is_sell_page
            
            logger.info(f"Тип страницы: {'аренда квартир' if is_rent_page else 'продажа квартир' if is_sell_page else 'обычный поиск'}")
            
            if is_apartments_page:
                # Для страниц с квартирами
                titles = []
                selectors = [
                    "[data-marker='item']", 
                    "[data-marker='catalog-serp']", 
                    "div[data-marker='catalog-serp'] [data-marker='item']",
                    "div[data-marker='item-container']",
                    "div[data-marker='item-aligner']"
                ]
                
                for selector in selectors:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if elements:
                        titles = elements
                        logger.info(f"Найдено {len(elements)} объявлений по селектору {selector}")
                        break
            else:
                # Стандартный селектор для обычных страниц
                titles = [t for t in self.driver.find_elements(LocatorAvito.TITLES[1], by="css selector") 
                           if t.get_attribute("class") and "avitoSales" not in t.get_attribute("class")]
            
            if not titles:
                logger.warning("На странице не найдено объявлений")
                return ads_data
                
            self.remove_other_cities()

            for title in titles:
                try:
                    if is_apartments_page:
                        # Парсинг для страниц с квартирами
                        try:
                            # Пробуем разные селекторы для заголовка
                            name_element = None
                            name_selectors = [
                                "[data-marker='item-title']",
                                "[itemprop='name']",
                                "h3"
                            ]
                            
                            for selector in name_selectors:
                                elements = title.find_elements(By.CSS_SELECTOR, selector)
                                if elements:
                                    name_element = elements[0]
                                    break
                                    
                            if not name_element:
                                continue
                                
                            name = name_element.text
                        except Exception as e:
                            logger.error(f"Ошибка при получении заголовка: {e}")
                            continue
                        
                        # Описание
                        description = ''
                        try:
                            desc_selectors = [
                                "[data-marker='item-descr']",
                                "[data-marker='item-description']",
                                "[itemprop='description']"
                            ]
                            
                            for selector in desc_selectors:
                                elements = title.find_elements(By.CSS_SELECTOR, selector)
                                if elements:
                                    description = elements[0].text
                                    break
                        except Exception:
                            pass
                        
                        # URL
                        url_link = None
                        try:
                            url_selectors = [
                                "a[data-marker='item-title']",
                                "a[itemprop='url']",
                                "a[href*='/kvartiry/']"
                            ]
                            
                            for selector in url_selectors:
                                elements = title.find_elements(By.CSS_SELECTOR, selector)
                                if elements:
                                    url_link = elements[0].get_attribute("href")
                                    break
                                    
                            if not url_link:
                                continue
                        except Exception as e:
                            logger.error(f"Ошибка при получении URL: {e}")
                            continue
                        
                        # Цена
                        price = "0"
                        try:
                            price_selectors = [
                                "[data-marker='item-price']",
                                "[itemprop='price']",
                                "span.price-text"
                            ]
                            
                            for selector in price_selectors:
                                elements = title.find_elements(By.CSS_SELECTOR, selector)
                                if elements:
                                    price_text = elements[0].text
                                    price = ''.join(filter(str.isdigit, price_text))
                                    break
                        except Exception:
                            pass
                        
                        # ID объявления
                        ads_id = None
                        try:
                            ads_id = title.get_attribute("data-item-id")
                        except Exception:
                            pass
                        
                        if not ads_id and url_link:
                            match = re.search(r"_(\d+)$", url_link)
                            if match:
                                ads_id = match.group(1)
                                
                        if not ads_id:
                            continue
                    else:
                        # Стандартный парсинг для обычных страниц
                        try:
                            name = title.find_element(*LocatorAvito.NAME).text
                        except Exception:
                            continue  # не объявление

                        description = ''
                        if title.find_elements(*LocatorAvito.DESCRIPTIONS):
                            try:
                                description = title.find_element(*LocatorAvito.DESCRIPTIONS).text
                            except Exception:
                                pass

                        url_link = title.find_element(*LocatorAvito.URL).get_attribute("href")
                        try:
                            price = title.find_element(*LocatorAvito.PRICE).get_attribute("content")
                        except Exception:
                            price = "0"
                            
                        ads_id = title.get_attribute("data-item-id")

                        if not ads_id and url_link:
                            match = re.search(r"_(\d+)$", url_link)
                            if match:
                                ads_id = match.group(1)
                    
                    if not ads_id:
                        continue

                    self.current_scan_ads.add(ads_id)
                    
                    ads_data.append({
                        "name": name,
                        "description": description,
                        "url": url_link,
                        "price": price,
                        "id": ads_id,
                    })
                except Exception as e:
                    logger.error(f"Ошибка при парсинге объявления: {e}")
            
        except Exception as e:
            logger.error(f"Ошибка при парсинге страницы {url}: {e}")
        
        logger.info(f"Найдено объявлений на странице: {len(ads_data)}")
        return ads_data

    def __navigate_pages(self, base_url: str, num_pages: int) -> List[str]:
        urls = [base_url]
        
        for i in range(2, num_pages + 1):
            parts = urlparse(base_url)
            query = parse_qs(parts.query)
            query['p'] = [str(i)]
            query_string = urlencode(query, doseq=True)
            next_url = urlunparse(parts._replace(query=query_string))
            urls.append(next_url)
        
        return urls

    def remove_other_cities(self) -> None:
        try:
            divs = self.driver.find_elements(LocatorAvito.OTHER_GEO[1], by="css selector")
            if not divs:
                return
            parent = divs[0].find_element(By.XPATH, './..')
            self.driver.execute_script("arguments[0].remove();", parent)
            logger.info("Лишние города удалены")
        except Exception:
            pass

    def _filter_ad(self, data: dict) -> bool:
        all_content = f"{data['name']} {data['description']}".lower()
        
        try:
            price = int(data["price"])
            price_ok = self.min_price <= price <= self.max_price if self.max_price > 0 else True
        except (ValueError, TypeError):
            price_ok = True
            
        kw_ok = True
        if self.keys_word:
            kw_ok = any(k.lower() in all_content for k in self.keys_word)
        if kw_ok and self.keys_black_word:
            kw_ok = not any(k.lower() in all_content for k in self.keys_black_word)
            
        return price_ok and kw_ok
    
    def _process_new_ad(self, data: dict) -> None:
        if self.max_views == 0:
            try:
                full_data = self.__parse_full_page(data)
                
                if "views" in full_data:
                    views_text = full_data.get("views", "0")
                    views = int(''.join(filter(str.isdigit, views_text)))
                    
                    logger.info(f"Объявление {data['id']} - просмотры: {views}")
                    
                    if views == 0:
                        self.send_notification_with_photo(full_data)
                    else:
                        logger.info(f"Пропускаем объявление {data['id']} - есть просмотры ({views})")
                else:
                    logger.warning(f"Не удалось получить информацию о просмотрах для {data['id']}")
                    self.send_notification_with_photo(full_data)
            except Exception as e:
                logger.error(f"Ошибка при обработке объявления {data['id']}: {e}")
        else:
            if self.need_more_info:
                try:
                    full_data = self.__parse_full_page(data)
                    self.send_notification_with_photo(full_data)
                except Exception as e:
                    logger.error(f"Ошибка при получении изображения для {data['id']}: {e}")
                    self.send_notification_with_photo(data)
            else:
                self.send_notification_with_photo(data)

    def _extract_image_from_listing(self, data: dict) -> Optional[str]:
        try:
            ad_element = None
            titles = self.driver.find_elements(LocatorAvito.TITLES[1], by="css selector")
            for title in titles:
                if str(title.get_attribute("data-item-id")) == str(data["id"]):
                    ad_element = title
                    break
            
            if not ad_element:
                return None
            
            img_elements = ad_element.find_elements(By.CSS_SELECTOR, "img")
            for img in img_elements:
                src = img.get_attribute("src")
                if src and "https://" in src and (".jpg" in src or ".jpeg" in src or ".png" in src):
                    return src
        except Exception as e:
            logger.debug(f"Не удалось извлечь изображение из листинга: {e}")
        
        return None

    def __parse_full_page(self, data: dict) -> dict:
        try:
            self.driver.open(data["url"])
            
            if "Доступ ограничен" in self.driver.get_title():
                self.ip_block()
                return self.__parse_full_page(data)

            try:
                self.driver.wait_for_element_visible(LocatorAvito.TOTAL_VIEWS[1], by="css selector", timeout=10)
            except Exception:
                if "Доступ ограничен" in self.driver.get_title():
                    self.ip_block()
                    return self.__parse_full_page(data)
                return data

            try:
                if self.driver.find_elements(LocatorAvito.GEO[1], by="css selector"):
                    data["geo"] = self.driver.find_element(LocatorAvito.GEO[1], by="css selector").text.lower()
                    
                if self.driver.find_elements(LocatorAvito.TOTAL_VIEWS[1], by="css selector"):
                    views_text = self.driver.find_element(LocatorAvito.TOTAL_VIEWS[1], by="css selector").text
                    data["views"] = views_text.split()[0] if views_text else "0"
                    
                try:
                    img_selectors = [
                        "div[data-marker='item-view/gallery'] img",
                        "div[data-marker='gallery-img'] img",
                        "div.gallery-img-wrapper img", 
                        ".gallery-img-frame img"
                    ]
                    
                    for selector in img_selectors:
                        img_elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                        if img_elements:
                            for img in img_elements:
                                src = img.get_attribute("src")
                                if src and "https://" in src and not src.endswith("svg"):
                                    data["image_url"] = src
                                    logger.info(f"Найдено изображение: {src}")
                                    break
                            if "image_url" in data:
                                break
                    
                    if "image_url" not in data:
                        slides = self.driver.find_elements(*LocatorAvito.GALLERY_SLIDES)
                        for slide in slides:
                            style = slide.get_attribute("style")
                            if style and "url(" in style:
                                url_match = re.search(r'url\("(.+?)"\)', style)
                                if url_match:
                                    data["image_url"] = url_match.group(1)
                                    logger.info(f"Найдено изображение в слайдере: {data['image_url']}")
                                    break
                except Exception as e:
                    logger.debug(f"Не удалось получить URL изображения: {e}")
                    
            except Exception as e:
                logger.error(f"Ошибка при парсинге страницы объявления: {e}")
        except Exception as e:
            logger.error(f"Ошибка при открытии страницы объявления {data['url']}: {e}")
            
        return data

    def check_stop_event(self) -> None:
        if self.stop_event.is_set():
            raise StopEventException()

    def change_ip(self) -> bool:
        if not self.proxy or not self.proxy_change_url:
            return False
        
        try:
            session = requests.Session()
            
            if ":" in self.proxy and "@" in self.proxy:
                proxies = {
                    "http": f"http://{self.proxy}",
                    "https": f"http://{self.proxy}"
                }
            elif self.proxy.count(":") == 3:
                parts = self.proxy.split(":")
                if len(parts) == 4:
                    host, port, username, password = parts
                    formatted_proxy = f"{username}:{password}@{host}:{port}"
                    proxies = {
                        "http": f"http://{formatted_proxy}",
                        "https": f"http://{formatted_proxy}"
                    }
            else:
                logger.error(f"Неверный формат прокси: {self.proxy}")
                return False
                
            session.proxies.update(proxies)
            
            try:
                original_response = session.get("https://api.ipify.org", timeout=15)
                if original_response.status_code != 200:
                    logger.error(f"Не удалось получить исходный IP. Код ответа: {original_response.status_code}")
                    return False
                
                original_ip = original_response.text
                logger.info(f"Текущий IP: {original_ip}")
            except Exception as e:
                logger.error(f"Ошибка при получении исходного IP: {e}")
                return False
                
            logger.info(f"Отправка запроса на смену IP: {self.proxy_change_url}")
            res = requests.get(self.proxy_change_url, timeout=30)
            if res.status_code != 200:
                logger.error(f"Ошибка при запросе смены IP. Код ответа: {res.status_code}")
                return False
                
            logger.info(f"Ответ сервера при смене IP: {res.text.strip()}")
            
            start_time = time.time()
            deadline = start_time + 60
            attempts = 0
            
            while time.time() < deadline:
                try:
                    time.sleep(5)
                    check_response = session.get("https://api.ipify.org", timeout=15)
                    if check_response.status_code != 200:
                        logger.warning(f"Код ответа при проверке IP: {check_response.status_code}")
                        attempts += 1
                        continue
                    
                    new_ip = check_response.text
                    if new_ip != original_ip:
                        logger.info(f"IP успешно изменен с {original_ip} на {new_ip}")
                        # Сбрасываем индекс User-Agent к началу списка
                        UserAgentRotator._instance._current_index = 0
                        return True
                        
                    logger.info(f"Попытка {attempts+1}: IP пока не изменился. Текущий IP: {new_ip}")
                    attempts += 1
                except Exception as e:
                    logger.error(f"Ошибка при проверке нового IP (попытка {attempts+1}): {e}")
                    attempts += 1
            
            logger.warning(f"IP не изменился после {attempts} попыток. Старый IP: {original_ip}")
            UserAgentRotator._instance._current_index = 0
            return False
            
        except Exception as err:
            logger.error(f"Не удалось сменить IP: {err}")
            UserAgentRotator._instance._current_index = 0
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
                    page_urls = self.__navigate_pages(base_url, self.count)
                    
                    current_proxy = None
                    if self.proxy:
                        proxy_list = [p.strip() for p in self.proxy.split(';') if p.strip()]
                        if proxy_list:
                            selected_proxy = random.choice(proxy_list)
                            current_proxy = self._normalize_proxy(selected_proxy)
                            logger.info(f"Используется прокси: {current_proxy}")
                    
                    ua_rotator = UserAgentRotator()
                    with SB(
                        uc=False,
                        headed=bool(self.debug_mode),
                        headless2=not bool(self.debug_mode),
                        page_load_strategy="eager",
                        block_images=False,
                        agent=ua_rotator.get_next(),
                        proxy=current_proxy,
                        sjw=bool(self.fast_speed),
                    ) as self.driver:
                        all_ads = []
                        for page_url in page_urls:
                            try:
                                if self.stop_event.is_set():
                                    return
                                page_ads = self.__parse_page(page_url)
                                all_ads.extend(page_ads)
                                time.sleep(random.randint(2, 4))
                            except StopEventException:
                                logger.info("Парсинг остановлен по запросу")
                                return
                            except Exception as e:
                                logger.error(f"Ошибка при обработке страницы {page_url}: {e}")
                        
                        if not self.first_run:
                            for ad_data in all_ads:
                                ad_id = ad_data["id"]
                                if ad_id not in self.known_ads:
                                    self.total_new_ads += 1
                                    logger.info(f"Найдено новое объявление: {ad_data['name']} (ID: {ad_id})")
                                    
                                    if self._filter_ad(ad_data):
                                        self._process_new_ad(ad_data)
                                        
                except Exception as e:
                    logger.error(f"Ошибка при обработке URL {base_url}: {e}")
            
            self.known_ads.update(self.current_scan_ads)
            self._save_scan_results()
            
            if self.first_run:
                logger.info(f"Первичное сканирование завершено. Найдено объявлений: {len(self.current_scan_ads)}. При следующем запуске будут отображаться только новые объявления.")
            else:
                logger.info(f"Сканирование завершено. Найдено {self.total_new_ads} новых объявлений, отправлено {self.total_notified_ads} уведомлений.")
            
        except Exception as e:
            logger.error(f"Общая ошибка при парсинге: {e}")
        finally:
            self.stop_event.clear()
            logger.info("Парсинг завершен")