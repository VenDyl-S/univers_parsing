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


class AvitoParse:
    """
    Head‑less HTML‑парсер Avito без GUI/Excel.
    Нужен только для отправки уведомлений и
    отметки просмотренных объявлений в SQLite.
    Подготовлен для дальнейшего расширения
    (ЦИАН, РЖД) – вся логика инкапсулирована.
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
        # --- публичные параметры ------------------------------------------------
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

        # --- служебные ----------------------------------------------------------
        self.url: str | None = None
        self.stop_event = stop_event or threading.Event()
        self.db_handler = SQLiteDBHandler()
        
        # Отслеживание объявлений
        self.known_ads: Set[str] = set()  # Все известные объявления из БД
        self.current_scan_ads: Set[str] = set()  # Объявления текущего сканирования
        
        # Счетчики статистики - для отображения при остановке поиска
        self.total_new_ads: int = 0  # Общее количество новых объявлений
        self.total_notified_ads: int = 0  # Количество уведомлений (прошедших фильтры)
        
        # Создаем Telegram уведомитель если предоставлены токен и ID чата
        self.tg_notifier = None
        if self.tg_token and self.chat_id:
            self._setup_telegram_notifications()
            
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
            
    def _load_known_ads(self):
        """Загружает все известные ID объявлений из БД."""
        for url in self.url_list:
            ads_ids = self.db_handler.get_scan_ids(url)
            if ads_ids:
                self.known_ads.update(ads_ids)
        
        logger.info(f"Загружено {len(self.known_ads)} известных объявлений из БД")
    
    def _save_scan_results(self):
        """Сохраняет результаты текущего сканирования в БД."""
        # Сохраняем объединенный набор ID для каждого URL
        all_ads = self.known_ads.union(self.current_scan_ads)
        for url in self.url_list:
            self.db_handler.save_scan_ids(url, list(all_ads))
        
        logger.info(f"Сохранено {len(all_ads)} объявлений в БД")
    
    # --------------------------------------------------------------------- notifications
    def _setup_telegram_notifications(self):
        """Настройка отправителя уведомлений в Telegram."""
        self.tg_notifier = get_notifier("telegram")
        logger.info(f"Настроено уведомление для чата ID: {self.chat_id}")

    def send_notification_with_photo(self, data: dict):
        """Отправка уведомления с фото в Telegram."""
        if not self.chat_id or not self.tg_token:
            logger.info("Не удалось отправить уведомление: не настроены параметры.")
            return

        try:
            # Получаем заголовок поиска
            search_title = f"Авито - {self.job_name}" if self.job_name else "Авито"
            
            # Готовим текст сообщения с пустыми строками и эмодзи
            message_text = f"*{search_title}*\n"  # Добавляем заголовок поиска
            message_text += f"*{data.get('name', '-')}*\n\n"  # Название объявления и пустая строка после него
            message_text += f"💰 *{data.get('price', '-')}₽*\n"  # Добавляем эмодзи кошелька
            message_text += f"🔗 [Ссылка на объявление]({data.get('url')})\n\n"  # Добавляем эмодзи ссылки и пустую строку
            
            if description := data.get('description'):
                # Ограничиваем длину описания
                max_desc_length = 200
                if len(description) > max_desc_length:
                    description = description[:max_desc_length] + "..."
                message_text += f"📝 {description}"  # Добавляем эмодзи для описания
            
            # Для отправки с фото используем прямой API запрос
            url = f"https://api.telegram.org/bot{self.tg_token}/sendPhoto"
            
            if "image_url" in data and data["image_url"]:
                photo_url = data["image_url"]
            else:
                # Если нет изображения, используем заглушку
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
                # Увеличиваем счетчик отправленных уведомлений
                self.total_notified_ads += 1
            else:
                logger.error(f"Ошибка при отправке уведомления: {response.text}")
                # Если не удалось отправить с фото, пробуем отправить текстовое сообщение
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
                # Увеличиваем счетчик отправленных уведомлений
                self.total_notified_ads += 1
        except Exception as e:
            logger.error(f"Ошибка при отправке текстового уведомления: {e}")

    # --------------------------------------------------------------------- utils
    @property
    def use_proxy(self) -> bool:
        return bool(self.proxy and self.proxy_change_url)

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
                    return True
                else:
                    logger.error(f"Не удалось сменить IP. Код ответа: {response.status_code}")
            except Exception as e:
                logger.error(f"Ошибка при смене IP: {e}")
        
        # Если не удалось сменить IP или нет ссылки, делаем паузу
        logger.info("Блок IP. Использую паузу 5‑6 минут…")
        time.sleep(random.randint(300, 350))
        return False

    def ip_block(self) -> None:
        """Handle IP‑ban from Avito."""
        if self.use_proxy and self.change_ip():
            return
        logger.info("Блок IP. Использую паузу 5‑6 минут…")
        time.sleep(random.randint(300, 350))

    # ------------------------------------------------------------------- parsing
    def __get_url(self, url: str) -> bool:
        """Открывает url и возвращает True если страница успешно загружена."""
        # Гарантируем параметр s=104
        if "&s=" not in url:
            url += "&s=104"

        logger.info(f"Открываю страницу: {url}")
        try:
            self.driver.open(url)

            if "Доступ ограничен" in self.driver.get_title():
                self.handle_ip_block()
                return self.__get_url(url)
                
            return True
        except Exception as e:
            logger.error(f"Ошибка при открытии страницы {url}: {e}")
            return False

    def __parse_page(self, url: str) -> List[Dict]:
        """Парсит объявления со страницы."""
        ads_data = []
        logger.info(f"Парсинг страницы {url}...")
        
        try:
            if not self.__get_url(url):
                logger.error(f"Не удалось загрузить страницу {url}")
                return ads_data
                
            titles = [t for t in self.driver.find_elements(LocatorAvito.TITLES[1], by="css selector") if "avitoSales" not in t.get_attribute("class")]
            if not titles:
                logger.warning("На странице не найдено объявлений")
                return ads_data
                
            self.remove_other_cities()

            for title in titles:
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

                url = title.find_element(*LocatorAvito.URL).get_attribute("href")
                try:
                    price = title.find_element(*LocatorAvito.PRICE).get_attribute("content")
                except Exception:
                    price = "0"
                    
                ads_id = title.get_attribute("data-item-id")

                if not ads_id and url:
                    match = re.search(r"_(\d+)$", url)
                    if match:
                        ads_id = match.group(1)
                if not ads_id:
                    continue

                # Добавляем в текущий скан
                self.current_scan_ads.add(ads_id)
                
                # Добавляем данные объявления
                ads_data.append({
                    "name": name,
                    "description": description,
                    "url": url,
                    "price": price,
                    "id": ads_id,
                })
        except Exception as e:
            logger.error(f"Ошибка при парсинге страницы {url}: {e}")
        
        return ads_data

    def __navigate_pages(self, base_url: str, num_pages: int) -> List[str]:
        """Генерирует список URL-адресов для указанного количества страниц."""
        urls = [base_url]
        
        # Если нужно больше одной страницы, генерируем URL-адреса для дополнительных страниц
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
        """Фильтрует объявление по ключевым словам и цене."""
        all_content = f"{data['name']} {data['description']}".lower()
        
        # Проверка цены
        try:
            price = int(data["price"])
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
    
    def _process_new_ad(self, data: dict) -> None:
        """Обрабатывает новое объявление: получает дополнительную информацию и отправляет уведомление."""
        # Проверяем, нужно ли получать дополнительную информацию об объявлении
        if self.max_views == 0:  # Если включен режим "Только новые"
            try:
                # Получаем полную информацию для проверки просмотров
                full_data = self.__parse_full_page(data)
                
                # Проверяем количество просмотров
                if "views" in full_data:
                    views_text = full_data.get("views", "0")
                    # Извлекаем числовое значение из строки
                    views = int(''.join(filter(str.isdigit, views_text)))
                    
                    # Логируем информацию о просмотрах
                    logger.info(f"Объявление {data['id']} - просмотры: {views}")
                    
                    # Отправляем уведомление только если просмотров 0
                    if views == 0:
                        self.send_notification_with_photo(full_data)
                    else:
                        logger.info(f"Пропускаем объявление {data['id']} - есть просмотры ({views})")
                else:
                    # Если не удалось получить информацию о просмотрах, отправляем уведомление
                    logger.warning(f"Не удалось получить информацию о просмотрах для {data['id']}")
                    self.send_notification_with_photo(full_data)
            except Exception as e:
                logger.error(f"Ошибка при обработке объявления {data['id']}: {e}")
        else:
            # Для обычного режима (не "Только новые") отправляем уведомление сразу
            if self.need_more_info:
                # Если нужна дополнительная информация (например, изображения),
                # получаем её без проверки просмотров
                try:
                    # Здесь получаем только изображение без захода на страницу объявления
                    # Например, можно получить из HTML карточки объявления
                    image_url = self._extract_image_from_listing(data)
                    if image_url:
                        data["image_url"] = image_url
                except Exception as e:
                    logger.error(f"Ошибка при получении изображения для {data['id']}: {e}")
            
            # Отправляем уведомление
            self.send_notification_with_photo(data)

    def _extract_image_from_listing(self, data: dict) -> Optional[str]:
        """
        Пытается извлечь URL изображения из HTML карточки объявления,
        без захода на страницу объявления
        """
        # В данной реализации у нас нет доступа к HTML карточки после её обработки
        # Поэтому возвращаем None, и будет использована заглушка изображения
        return None

    def __parse_full_page(self, data: dict) -> dict:
        """Открывает страницу объявления и получает дополнительную информацию"""
        try:
            self.driver.open(data["url"])
            
            if "Доступ ограничен" in self.driver.get_title():
                self.handle_ip_block()
                return self.__parse_full_page(data)

            try:
                self.driver.wait_for_element_visible(LocatorAvito.TOTAL_VIEWS[1], by="css selector", timeout=10)
            except Exception:
                if "Доступ ограничен" in self.driver.get_title():
                    self.handle_ip_block()
                    return self.__parse_full_page(data)
                return data  # информация не критична

            # Собираем дополнительную информацию
            try:
                if self.driver.find_elements(LocatorAvito.GEO[1], by="css selector"):
                    data["geo"] = self.driver.find_element(LocatorAvito.GEO[1], by="css selector").text.lower()
                    
                if self.driver.find_elements(LocatorAvito.TOTAL_VIEWS[1], by="css selector"):
                    views_text = self.driver.find_element(LocatorAvito.TOTAL_VIEWS[1], by="css selector").text
                    data["views"] = views_text.split()[0] if views_text else "0"
                    
                # Получаем URL изображения, если есть
                try:
                    # Ищем изображения на странице
                    img_elements = self.driver.find_elements(By.CSS_SELECTOR, "div[data-marker='item-view/gallery'] img")
                    
                    if img_elements:
                        for img in img_elements:
                            src = img.get_attribute("src")
                            if src and "https://" in src and not src.endswith("svg"):
                                data["image_url"] = src
                                break
                except Exception as e:
                    logger.debug(f"Не удалось получить URL изображения: {e}")
                    
            except Exception as e:
                logger.error(f"Ошибка при парсинге страницы объявления: {e}")
        except Exception as e:
            logger.error(f"Ошибка при открытии страницы объявления {data['url']}: {e}")
            
        return data

    # ---------------------------------------------------------------- control
    def check_stop_event(self) -> None:
        if self.stop_event.is_set():
            raise StopEventException()

    def change_ip(self) -> bool:
        if not self.proxy_change_url:
            return False
        try:
            res = requests.get(self.proxy_change_url, timeout=30)
            if res.status_code == 200:
                logger.info("IP прокси изменён")
                return True
        except Exception as err:
            logger.debug("Не удалось сменить IP: %s", err)
        return False
        
    def get_statistics(self) -> Dict[str, int]:
        """Возвращает статистику сканирования."""
        return {
            "total_new_ads": self.total_new_ads,
            "total_notified_ads": self.total_notified_ads
        }

    # ============================================================== public API
    def parse(self) -> None:
        """Основной метод парсинга"""
        try:
            # Очищаем список текущего сканирования
            self.current_scan_ads = set()
            
            # Обрабатываем каждый базовый URL
            for base_url in self.url_list:
                if self.stop_event.is_set():
                    return
                
                try:
                    # Генерируем список URL для указанного количества страниц
                    page_urls = self.__navigate_pages(base_url, self.count)
                    
                    # Обработка прокси - выбираем случайный прокси из списка, если они заданы через точку с запятой
                    current_proxy = None
                    if self.proxy:
                        proxy_list = [p.strip() for p in self.proxy.split(';') if p.strip()]
                        if proxy_list:
                            selected_proxy = random.choice(proxy_list)
                            # Нормализуем формат прокси
                            current_proxy = self._normalize_proxy(selected_proxy)
                            logger.info(f"Используется прокси: {current_proxy}")
                    
                    with SB(
                        uc=False,
                        headed=bool(self.debug_mode),
                        headless2=not bool(self.debug_mode),
                        page_load_strategy="eager",
                        block_images=False,
                        agent=random.choice(open("user_agent_pc.txt").read().splitlines()),
                        proxy=current_proxy,  # Используем выбранный прокси
                        sjw=bool(self.fast_speed),
                    ) as self.driver:
                        # Сканируем все страницы и собираем объявления
                        all_ads = []
                        for page_url in page_urls:
                            try:
                                if self.stop_event.is_set():
                                    return
                                page_ads = self.__parse_page(page_url)
                                all_ads.extend(page_ads)
                                # Короткая пауза между страницами
                                time.sleep(random.randint(2, 4))
                            except StopEventException:
                                logger.info("Парсинг остановлен по запросу")
                                return
                            except Exception as e:
                                logger.error(f"Ошибка при обработке страницы {page_url}: {e}")
                        
                        # После сканирования всех страниц обрабатываем найденные объявления
                        if not self.first_run:
                            # В первом запуске только собираем объявления, не отправляем уведомления
                            for ad_data in all_ads:
                                ad_id = ad_data["id"]
                                # Проверяем, новое ли это объявление
                                if ad_id not in self.known_ads:
                                    # Увеличиваем счетчик новых объявлений
                                    self.total_new_ads += 1
                                    logger.info(f"Найдено новое объявление: {ad_data['name']} (ID: {ad_id})")
                                    
                                    # Проверяем условия фильтрации
                                    if self._filter_ad(ad_data):
                                        # Отправляем уведомление
                                        self._process_new_ad(ad_data)
                                        
                except Exception as e:
                    logger.error(f"Ошибка при обработке URL {base_url}: {e}")
            
            # После обработки всех URL сохраняем результаты в БД
            # Обновляем known_ads, добавляя в него новые объявления
            self.known_ads.update(self.current_scan_ads)
            self._save_scan_results()
            
            # Выводим информацию о результатах сканирования
            if self.first_run:
                logger.info("Первичное сканирование завершено. Найдено объявлений: " + 
                           f"{len(self.current_scan_ads)}. При следующем запуске будут " + 
                           "отображаться только новые объявления.")
            else:
                logger.info(f"Сканирование завершено. Найдено {self.total_new_ads} новых " + 
                           f"объявлений, отправлено {self.total_notified_ads} уведомлений.")
            
        except Exception as e:
            logger.error(f"Общая ошибка при парсинге: {e}")
        finally:
            self.stop_event.clear()
            logger.info("Парсинг завершен")