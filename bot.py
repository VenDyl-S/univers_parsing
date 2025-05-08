"""bot.py — Telegram‑меню + Avito‑парсер + ЦИАН-парсер
Полноценный пример с иерархическим меню и двумя парсерами:
Авито и ЦИАН (aiogram 3.7+, InlineKeyboard).

Платформы: Avito, ЦИАН, РЖД(заглушка).
"""

import asyncio
import json
import os
import re
from contextlib import suppress
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple, Optional

from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, StateFilter
from aiogram.filters.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    FSInputFile,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from loguru import logger
import requests
from bs4 import BeautifulSoup
from seleniumbase import SB

from db_service import SQLiteDBHandler
from parser_cls import AvitoParse
from parser_cian import CianParse  # Импортируем парсер ЦИАН

TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN:
    raise RuntimeError("Нужен BOT_TOKEN в .env")

# Инициализируем бота и диспетчер с хранилищем состояний
bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
router = Router()
dp.include_router(router)

scheduler = AsyncIOScheduler()
DB = SQLiteDBHandler()

# ---------------------------------------------------------------------------
# STATES
# ---------------------------------------------------------------------------
class SearchStates(StatesGroup):
    waiting_for_urls = State()
    waiting_for_name = State()
    waiting_for_price = State()
    waiting_for_pages = State()
    waiting_for_pause = State()
    waiting_for_keywords = State()
    waiting_for_blacklist = State()
    waiting_for_proxy = State()
    waiting_for_stop_id = State()
    waiting_for_proxy_confirm = State()

# ---------------------------------------------------------------------------
# DATA‑CLASSES
# ---------------------------------------------------------------------------
@dataclass(slots=True)
class SearchJob:
    sid: int
    user_id: int
    platform: str
    urls: List[str]
    settings: Dict[str, Any]
    stop_event: asyncio.Event
    first_run: bool = True
    name: str = ""
    # Добавляем ссылку на парсер для доступа к статистике
    parser: Any = None
    # Добавляем поля для хранения общей статистики за все итерации (кроме первичного сканирования)
    total_new_ads: int = 0
    total_notified_ads: int = 0

ACTIVE: dict[int, SearchJob] = {}

# ---------------------------------------------------------------------------
# PROXY UTILS
# ---------------------------------------------------------------------------
def normalize_proxy_format(proxy_str: str) -> str:
    """
    Нормализует прокси в разных форматах к единому формату username:password@host:port
    
    Поддерживаемые форматы:
    - username:password@host:port
    - host:port:username:password
    - http://username:password@host:port
    - socks5://username:password@host:port
    """
    if not proxy_str or proxy_str.strip() == "":
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
    
    # Если не смогли распознать формат, возвращаем как есть
    return proxy_str

async def test_proxy(proxy_str: str, message: Optional[Message] = None) -> Dict[str, Any]:
    """
    Проверяет работоспособность прокси
    
    Возвращает словарь с результатами проверок:
    {
        'success': bool,  # Общий результат
        'ip_check': {
            'success': bool,
            'message': str,
            'new_ip': str
        },
        'avito_check': {
            'success': bool,
            'message': str
        },
        'cian_check': {
            'success': bool,
            'message': str
        }
    }
    """
    results = {
        'success': False,
        'ip_check': {'success': False, 'message': 'Не удалось проверить IP', 'new_ip': None},
        'avito_check': {'success': False, 'message': 'Не удалось проверить Авито'},
        'cian_check': {'success': False, 'message': 'Не удалось проверить ЦИАН'}
    }
    
    # Нормализуем формат прокси
    normalized_proxy = normalize_proxy_format(proxy_str)
    
    if not normalized_proxy:
        return results
    
    # Прогресс-сообщение
    if message:
        await message.reply("🔄 Проверка прокси началась...")
    
    # 1. Проверка IP через api.ipify.org
    try:
        # Получаем текущий IP без прокси для сравнения
        try:
            current_ip = requests.get('https://api.ipify.org', timeout=10).text
        except:
            current_ip = "неизвестен"
        
        # Формируем прокси для requests
        proxies = {
            'http': f'http://{normalized_proxy}',
            'https': f'http://{normalized_proxy}'
        }
        
        # Проверяем IP через прокси
        r = requests.get('https://api.ipify.org', proxies=proxies, timeout=10)
        if r.status_code == 200:
            new_ip = r.text
            if new_ip != current_ip:
                results['ip_check'] = {
                    'success': True, 
                    'message': f'IP успешно изменен: {new_ip}',
                    'new_ip': new_ip
                }
                
                # Прогресс-сообщение
                if message:
                    await message.reply(f"✅ IP успешно изменен: {new_ip}")
            else:
                results['ip_check'] = {
                    'success': False, 
                    'message': 'IP не изменился после применения прокси',
                    'new_ip': new_ip
                }
                
                if message:
                    await message.reply(f"⚠️ IP не изменился: {new_ip}")
    except Exception as e:
        results['ip_check']['message'] = f'Ошибка при проверке IP: {str(e)}'
        if message:
            await message.reply(f"❌ {results['ip_check']['message']}")
        
    # 2. Проверка доступа к Авито через SeleniumBase
    try:
        # Прогресс-сообщение
        if message:
            await message.reply("🔄 Проверка доступа к Авито...")
            
        with SB(
            uc=False,
            headed=False,
            headless2=True,
            page_load_strategy="eager",
            block_images=True,
            proxy=normalized_proxy
        ) as driver:
            driver.open('https://www.avito.ru/sankt-peterburg?cd=1&q=samsung&s=104')
            
            # Проверяем, что не получили ошибку "Доступ ограничен"
            if "Доступ ограничен" not in driver.get_title():
                results['avito_check'] = {
                    'success': True,
                    'message': 'Доступ к Авито работает'
                }
                if message:
                    await message.reply("✅ Доступ к Авито работает")
            else:
                results['avito_check'] = {
                    'success': False,
                    'message': 'Доступ к Авито ограничен (блокировка)'
                }
                if message:
                    await message.reply("❌ Доступ к Авито ограничен (блокировка)")
    except Exception as e:
        results['avito_check']['message'] = f'Ошибка при проверке Авито: {str(e)}'
        if message:
            await message.reply(f"❌ {results['avito_check']['message']}")
    
    # 3. Проверка доступа к ЦИАН через requests
    try:
        # Прогресс-сообщение
        if message:
            await message.reply("🔄 Проверка доступа к ЦИАН...")
            
        # Используем разный User-Agent чтобы избежать блокировки
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7'
        }
        
        cian_url = 'https://www.cian.ru/cat.php?currency=2&deal_type=rent&engine_version=2&maxprice=999999&offer_type=flat&region=1&sort=creation_date_desc&type=4'
        r = requests.get(cian_url, proxies=proxies, headers=headers, timeout=15)
        
        if r.status_code == 200:
            # Проверяем, что получили страницу с результатами, а не с блокировкой
            soup = BeautifulSoup(r.text, 'html.parser')
            if 'captcha' not in r.text.lower() and 'доступ ограничен' not in r.text.lower():
                results['cian_check'] = {
                    'success': True,
                    'message': 'Доступ к ЦИАН работает'
                }
                if message:
                    await message.reply("✅ Доступ к ЦИАН работает")
            else:
                results['cian_check'] = {
                    'success': False,
                    'message': 'Доступ к ЦИАН ограничен (блокировка)'
                }
                if message:
                    await message.reply("❌ Доступ к ЦИАН ограничен (блокировка)")
    except Exception as e:
        results['cian_check']['message'] = f'Ошибка при проверке ЦИАН: {str(e)}'
        if message:
            await message.reply(f"❌ {results['cian_check']['message']}")
    
    # Определяем общий результат проверки
    results['success'] = results['ip_check']['success'] and (results['avito_check']['success'] or results['cian_check']['success'])
    
    # Итоговый результат
    if message:
        if results['success']:
            await message.reply("✅ Прокси успешно прошел проверку и может быть использован.")
        else:
            await message.reply("❌ Прокси не прошел проверку. Пожалуйста, проверьте настройки и формат.")
    
    return results

async def test_ip_change_url(url: str, message: Optional[Message] = None) -> Dict[str, Any]:
    """Проверяет работоспособность URL для смены IP."""
    results = {
        'success': False,
        'message': 'Не удалось проверить URL смены IP',
        'old_ip': None,
        'new_ip': None
    }
    
    if not url or not url.startswith('http'):
        return results
    
    # Прогресс-сообщение
    if message:
        await message.reply("🔄 Проверка URL для смены IP...")
    
    try:
        # Получаем текущий IP
        try:
            current_ip = requests.get('https://api.ipify.org', timeout=10).text
            results['old_ip'] = current_ip
        except:
            current_ip = "неизвестен"
        
        # Вызываем URL для смены IP
        response = requests.get(url, timeout=15)
        if response.status_code == 200:
            # Ждем немного, чтобы IP изменился
            await asyncio.sleep(3)
            
            # Проверяем новый IP
            try:
                new_ip = requests.get('https://api.ipify.org', timeout=10).text
                results['new_ip'] = new_ip
                
                if new_ip != current_ip and current_ip != "неизвестен":
                    results['success'] = True
                    results['message'] = f'IP успешно изменен с {current_ip} на {new_ip}'
                    
                    if message:
                        await message.reply(f"✅ IP успешно изменен: {current_ip} -> {new_ip}")
                else:
                    results['message'] = f'IP не изменился после вызова URL: {new_ip}'
                    
                    if message:
                        await message.reply(f"⚠️ IP не изменился: {new_ip}")
            except Exception as e:
                results['message'] = f'Ошибка при проверке нового IP: {str(e)}'
                
                if message:
                    await message.reply(f"❌ {results['message']}")
        else:
            results['message'] = f'URL вернул код ошибки: {response.status_code}'
            
            if message:
                await message.reply(f"❌ Ошибка при вызове URL: код {response.status_code}")
    except Exception as e:
        results['message'] = f'Ошибка при вызове URL: {str(e)}'
        
        if message:
            await message.reply(f"❌ {results['message']}")
    
    return results

# ---------------------------------------------------------------------------
# KEYBOARDS
# ---------------------------------------------------------------------------

def kb_main() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="🏠 Авито", callback_data="menu:avito")  # Дом
    b.button(text="🏙️ ЦИАН", callback_data="menu:cian")    # Городской пейзаж
    b.button(text="📋 Показать поиски", callback_data="menu:show_searches")  # Список
    b.button(text="🔑 Прокси", callback_data="menu:proxy")  # Ключ
    b.adjust(2)
    return b.as_markup()


def kb_avito() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="Запустить поиск", callback_data="avito:start")
    b.button(text="Показать параметры", callback_data="avito:show")
    b.button(text="Изменить параметры", callback_data="avito:edit")
    b.button(text="⬅︎ Назад", callback_data="back:main")
    b.adjust(1)
    return b.as_markup()


def kb_cian() -> InlineKeyboardMarkup:
    """Клавиатура для меню ЦИАН (аналогичная Авито)."""
    b = InlineKeyboardBuilder()
    b.button(text="Запустить поиск", callback_data="cian:start")
    b.button(text="Показать параметры", callback_data="cian:show")
    b.button(text="Изменить параметры", callback_data="cian:edit")
    b.button(text="⬅︎ Назад", callback_data="back:main")
    b.adjust(1)
    return b.as_markup()


def kb_edit_params_avito() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="Цена", callback_data="edit:price")
    b.button(text="Страниц", callback_data="edit:pages")
    b.button(text="Пауза", callback_data="edit:pause")
    b.button(text="Только новые", callback_data="edit:new")
    b.button(text="Ключевые слова", callback_data="edit:kw")
    b.button(text="Чёрный список", callback_data="edit:black")
    b.button(text="⬅︎ Назад", callback_data="back:avito")
    b.adjust(2)
    return b.as_markup()


def kb_edit_params_cian() -> InlineKeyboardMarkup:
    """Клавиатура для редактирования параметров ЦИАН."""
    b = InlineKeyboardBuilder()
    b.button(text="Цена", callback_data="edit_cian:price")
    b.button(text="Страниц", callback_data="edit_cian:pages")
    b.button(text="Пауза", callback_data="edit_cian:pause")
    b.button(text="Ключевые слова", callback_data="edit_cian:kw")
    b.button(text="Чёрный список", callback_data="edit_cian:black")
    b.button(text="⬅︎ Назад", callback_data="back:cian")
    b.adjust(2)
    return b.as_markup()


def kb_yes_no() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="Да", callback_data="new_only:1")
    b.button(text="Нет", callback_data="new_only:0")
    b.adjust(2)
    return b.as_markup()


def kb_proxy() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="Изменить", callback_data="proxy:add")
    b.button(text="Проверить", callback_data="proxy:check")  # Новая кнопка
    b.button(text="Удалить", callback_data="proxy:del")
    b.button(text="⬅︎ Назад", callback_data="back:main")
    b.adjust(2)
    return b.as_markup()


def kb_confirm_proxy_delete() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="Да, удалить", callback_data="proxy_confirm:yes")
    b.button(text="Отмена", callback_data="proxy_confirm:no")
    b.adjust(2)
    return b.as_markup()


def kb_search_list() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="Остановить поиск", callback_data="action:stop_search")
    b.button(text="⬅︎ Назад", callback_data="back:main")
    b.adjust(1)
    return b.as_markup()

# ---------------------------------------------------------------------------
# SETTINGS HELPERS
# ---------------------------------------------------------------------------
DEFAULT_AVITO = {
    "min_price": 0,
    "max_price": 9_999_999,
    "pages": 5,
    "pause": 120,
    "new_only": False,
    "keywords": [],
    "blacklist": [],
    "proxy": None,
    "proxy_change_url": None,
}

DEFAULT_CIAN = {
    "min_price": 0,
    "max_price": 9_999_999,
    "pages": 5,  # 5 страниц по умолчанию
    "pause": 300,  # 5 минут по умолчанию
    "keywords": [],
    "blacklist": [],
    "proxy": None,
    "proxy_change_url": None,
}


def _parse_list(txt: str) -> List[str]:
    return [w.strip() for w in txt.split(";") if w.strip()]


def user_settings(uid: int, platform: str = "avito") -> Dict[str, Any]:
    """Получает настройки пользователя для указанной платформы."""
    if platform == "avito":
        s = DEFAULT_AVITO.copy()
    else:
        s = DEFAULT_CIAN.copy()
    
    # Получаем настройки из базы данных
    settings_prefix = f"{platform}_" if platform != "avito" else ""
    user_settings = DB.list_settings(uid)
    
    # Фильтруем настройки по платформе
    platform_settings = {}
    for key, value in user_settings.items():
        if platform == "avito" and not key.startswith("cian_"):
            platform_settings[key] = value
        elif platform != "avito" and key.startswith(settings_prefix):
            # Удаляем префикс для ключей ЦИАН
            clean_key = key[len(settings_prefix):]
            platform_settings[clean_key] = value
    
    # Обновляем значения по умолчанию
    s.update(platform_settings)
    
    # Преобразуем типы для числовых и булевых значений
    if "min_price" in s:
        s["min_price"] = int(s["min_price"])
    if "max_price" in s:
        s["max_price"] = int(s["max_price"])
    if "pages" in s:
        s["pages"] = int(s["pages"])
    if "pause" in s:
        s["pause"] = int(s["pause"])
    if "new_only" in s:
        s["new_only"] = bool(int(s.get("new_only", 0))) if isinstance(s.get("new_only"), str) else bool(s["new_only"])
    
    # Преобразуем строковые списки в списки
    if "keywords" in s:
        s["keywords"] = _parse_list(s["keywords"]) if isinstance(s["keywords"], str) else s["keywords"]
    if "blacklist" in s:
        s["blacklist"] = _parse_list(s["blacklist"]) if isinstance(s["blacklist"], str) else s["blacklist"]
    
    # Добавляем прокси и прокси_изменения_URL глобально для всех платформ
    s["proxy"] = user_settings.get("proxy", None)  
    s["proxy_change_url"] = user_settings.get("proxy_change_url", None)
    
    return s


def save(uid: int, key: str, value: Any, platform: str = "avito"):
    """Сохраняет настройку в базу данных с учетом платформы."""
    if platform != "avito":
        key = f"{platform}_{key}"
    DB.set_setting(uid, key, str(value))

# ---------------------------------------------------------------------------
# /start
# ---------------------------------------------------------------------------
@router.message(Command("start"))
async def cmd_start(m: Message):
    await m.answer(f"Здравствуйте, {m.from_user.first_name}! На что желаете поохотиться сегодня?", reply_markup=kb_main())

# ---------------------------------------------------------------------------
# MAIN MENU CALLBACKS
# ---------------------------------------------------------------------------
@router.callback_query(F.data == "menu:avito")
async def cb_avito_menu(cq: CallbackQuery):
    await cq.message.edit_text("Не забудьте проверить параметры перед новым поиском.", reply_markup=kb_avito())
    await cq.answer()

@router.callback_query(F.data == "menu:cian")
async def cb_cian_menu(cq: CallbackQuery):
    await cq.message.edit_text("Не забудьте проверить параметры перед новым поиском ЦИАН.", reply_markup=kb_cian())
    await cq.answer()

@router.callback_query(F.data == "menu:proxy")
async def cb_proxy(cq: CallbackQuery, state: FSMContext):
    # Получаем текущие настройки прокси
    user_id = cq.from_user.id
    proxy = DB.get_setting(user_id, "proxy") or "Не настроено"
    proxy_change_url = DB.get_setting(user_id, "proxy_change_url") or "Не настроено"
    
    # Формируем текст для отображения текущих настроек
    proxy_text = (
        "<b>Настройки прокси</b>\n\n"
        f"<b>Прокси в формате username:password@host:port или host:port:username:password:</b>\n{proxy}\n\n"
        f"<b>Ссылка для изменения IP, в формате https://changeip.mobileproxy.space/?proxy_key=***:</b>\n{proxy_change_url}"
    )
    
    await cq.message.edit_text(proxy_text, reply_markup=kb_proxy(), parse_mode="HTML")
    await cq.answer()

@router.callback_query(F.data == "menu:show_searches")
async def cb_show_searches(cq: CallbackQuery):
    rows = DB.list_active_searches(cq.from_user.id)
    if not rows:
        await cq.answer("Активных поисков нет", show_alert=True)
        return
    
    searches_list = []
    for row in rows:
        i, url = row[0], row[1]  # ID и URL всегда есть
        # Получаем платформу из настроек JSON
        settings_json = json.loads(row[2])
        platform = settings_json.get('platform', 'avito')
        platform_icon = "🏠" if platform == "avito" else "🏙️" if platform == "cian" else "🚆"
        
        # Проверяем, есть ли поле name в результате запроса
        name = row[3] if len(row) > 3 and row[3] else ""
        display_name = f"#{i}" if not name else f"#{i}-{name}"
        
        # Добавляем ссылку без предпросмотра (используем HTML тег code для предотвращения предпросмотра)
        searches_list.append(f"{platform_icon} {display_name}: <code>{url}</code>")
    
    text = "Активные поиски:\n" + "\n".join(searches_list)
    await cq.message.edit_text(text, reply_markup=kb_search_list(), parse_mode="HTML")
    await cq.answer()

@router.callback_query(F.data == "action:stop_search")
async def cb_action_stop_search(cq: CallbackQuery, state: FSMContext):
    rows = DB.list_active_searches(cq.from_user.id)
    if not rows:
        await cq.answer("Активных поисков нет", show_alert=True)
        return
    
    searches_list = []
    for row in rows:
        i, url = row[0], row[1]  # ID и URL всегда есть
        # Получаем платформу из настроек JSON
        settings_json = json.loads(row[2])
        platform = settings_json.get('platform', 'avito')
        platform_icon = "🏠" if platform == "avito" else "🏙️" if platform == "cian" else "🚆"
        
        # Проверяем, есть ли поле name в результате запроса
        name = row[3] if len(row) > 3 and row[3] else ""
        display_name = f"#{i}" if not name else f"#{i}-{name}"
        
        # Добавляем ссылку без предпросмотра
        searches_list.append(f"{platform_icon} {display_name}: <code>{url}</code>")
    
    text = "Активные поиски:\n" + "\n".join(searches_list) + "\n\nУкажите номер поиска, который нужно остановить:"
    await cq.message.edit_text(text, parse_mode="HTML")
    await cq.answer()
    await state.set_state(SearchStates.waiting_for_stop_id)

@router.callback_query(F.data.startswith("back:"))
async def cb_back(cq: CallbackQuery):
    dest = cq.data.split(":", 1)[1]
    if dest == "main":
        await cq.message.edit_text(f"Здравствуйте, {cq.from_user.first_name}! На что желаете поохотиться сегодня?", reply_markup=kb_main())
    elif dest == "avito":
        await cq.message.edit_text("Не забудьте проверить параметры перед новым поиском.", reply_markup=kb_avito())
    elif dest == "cian":
        await cq.message.edit_text("Не забудьте проверить параметры перед новым поиском ЦИАН.", reply_markup=kb_cian())
    elif dest == "edit":
        await cq.message.edit_text("Изменить параметры", reply_markup=kb_edit_params_avito())
    elif dest == "edit_cian":
        await cq.message.edit_text("Изменить параметры ЦИАН", reply_markup=kb_edit_params_cian())
    await cq.answer()

# ---------------------------------------------------------------------------
# AVITO ACTIONS
# ---------------------------------------------------------------------------
@router.callback_query(F.data == "avito:show")
async def cb_avito_show(cq: CallbackQuery):
    s = user_settings(cq.from_user.id, "avito")
    txt = (
        f"<b>Параметры Авито</b>\n"
        f"Цена: {s['min_price']}–{s['max_price']}\n"
        f"Страниц: {s['pages']}\n"
        f"Пауза: {s['pause']} сек\n"
        f"Только новые: {'Да' if s['new_only'] else 'Нет'}\n"
        f"KW: {', '.join(s['keywords']) or '-'}\n"
        f"BL: {', '.join(s['blacklist']) or '-'}"
    )
    await cq.message.edit_text(txt, reply_markup=kb_avito())
    await cq.answer()

@router.callback_query(F.data == "avito:start")
async def cb_avito_start(cq: CallbackQuery, state: FSMContext):
    await state.update_data(platform="avito")
    await cq.message.edit_text("Пришлите ссылку (или несколько через пробел)")
    await state.set_state(SearchStates.waiting_for_urls)
    await cq.answer()

@router.callback_query(F.data == "avito:edit")
async def cb_avito_edit(cq: CallbackQuery):
    await cq.message.edit_text("Изменить параметры", reply_markup=kb_edit_params_avito())
    await cq.answer()

# ---------------------------------------------------------------------------
# CIAN ACTIONS
# ---------------------------------------------------------------------------
@router.callback_query(F.data == "cian:show")
async def cb_cian_show(cq: CallbackQuery):
    s = user_settings(cq.from_user.id, "cian")
    txt = (
        f"<b>Параметры ЦИАН</b>\n"
        f"Цена: {s['min_price']}–{s['max_price']}\n"
        f"Страниц: {s['pages']}\n"
        f"Пауза: {s['pause']} сек\n"
        f"KW: {', '.join(s['keywords']) or '-'}\n"
        f"BL: {', '.join(s['blacklist']) or '-'}"
    )
    await cq.message.edit_text(txt, reply_markup=kb_cian())
    await cq.answer()

@router.callback_query(F.data == "cian:start")
async def cb_cian_start(cq: CallbackQuery, state: FSMContext):
    await state.update_data(platform="cian")
    await cq.message.edit_text("Пришлите ссылку на поиск ЦИАН (или несколько через пробел)")
    await state.set_state(SearchStates.waiting_for_urls)
    await cq.answer()

@router.callback_query(F.data == "cian:edit")
async def cb_cian_edit(cq: CallbackQuery):
    await cq.message.edit_text("Изменить параметры ЦИАН", reply_markup=kb_edit_params_cian())
    await cq.answer()

# ---------------------------------------------------------------------------
# ОБРАБОТКА ВВОДА URLs И ЗАПУСК ПОИСКА
# ---------------------------------------------------------------------------
@router.message(StateFilter(SearchStates.waiting_for_urls))
async def handle_urls(message: Message, state: FSMContext):
    urls = message.text.split()
    if not urls:
        await message.reply("Вы не указали ни одной ссылки. Пожалуйста, отправьте ссылки для поиска.")
        return
    
    data = await state.get_data()
    platform = data.get("platform", "avito")
    
    await state.update_data(urls=urls)
    await message.reply("Укажите имя для поиска или отправьте пустое сообщение для автоматического имени:")
    await state.set_state(SearchStates.waiting_for_name)

@router.message(StateFilter(SearchStates.waiting_for_name))
async def handle_search_name(message: Message, state: FSMContext):
    name = message.text.strip()
    data = await state.get_data()
    urls = data.get("urls", [])
    platform = data.get("platform", "avito")
    
    if not urls:
        await message.reply("Ошибка: ссылки не найдены. Попробуйте заново.")
        await state.clear()
        
        # Возвращаем пользователя в соответствующее меню
        if platform == "cian":
            await bot.send_message(message.chat.id, "Не забудьте проверить параметры перед новым поиском ЦИАН.", reply_markup=kb_cian())
        else:
            await bot.send_message(message.chat.id, "Не забудьте проверить параметры перед новым поиском.", reply_markup=kb_avito())
        return
    
    # Получаем настройки для выбранной платформы
    st = user_settings(message.from_user.id, platform)
    sid = DB.add_search(message.from_user.id, platform, urls, st, name)
    ev = asyncio.Event()
    
    # Создаем SearchJob с флагом first_run = True
    job = SearchJob(sid, message.from_user.id, platform, urls, st, ev, first_run=True, name=name)
    ACTIVE[sid] = job
    
    # Информируем пользователя о начале поиска
    platform_name = "ЦИАН" if platform == "cian" else "Авито"
    await message.reply(f"Запускаю поиск {platform_name} #{sid}{'-'+name if name else ''}...")
    
    # Запускаем первичное сканирование
    if platform == "cian":
        await run_cian(job)
    else:
        await run_avito(job)
    
    # После первичного сканирования обновляем флаг и запускаем регулярную задачу
    job.first_run = False
    
    # Используем pause из настроек для интервала
    interval_seconds = st["pause"]
    
    # Запускаем периодическую задачу в зависимости от платформы
    if platform == "cian":
        scheduler.add_job(run_cian, "interval", seconds=interval_seconds, args=[job], id=str(sid))
    else:
        scheduler.add_job(run_avito, "interval", seconds=interval_seconds, args=[job], id=str(sid))
    
    display_name = f"Поиск {platform_name} #{sid}" if not name else f"Поиск {platform_name} #{sid}-{name}"
    await message.reply(f"{display_name} запущен.\nПервичное сканирование завершено. Теперь будут приходить уведомления только о новых объявлениях.")
    await state.clear()
    
    # Возвращаем пользователя в соответствующее меню
    if platform == "cian":
        await bot.send_message(message.chat.id, "Не забудьте проверить параметры перед новым поиском ЦИАН.", reply_markup=kb_cian())
    else:
        await bot.send_message(message.chat.id, "Не забудьте проверить параметры перед новым поиском.", reply_markup=kb_avito())

# ---------------------------------------------------------------------------
# EDIT INDIVIDUAL PARAM (AVITO)
# ---------------------------------------------------------------------------
@router.callback_query(F.data.startswith("edit:"))
async def cb_edit_param(cq: CallbackQuery, state: FSMContext):
    param = cq.data.split(":", 1)[1]
    
    if param == "new":
        # Для параметра "Только новые" используем кнопки Да/Нет
        await cq.message.edit_text("Показывать только новые объявления?", reply_markup=kb_yes_no())
        await cq.answer()
        return
    
    if param == "kw":
        # Получаем текущие ключевые слова
        s = user_settings(cq.from_user.id)
        current_kw = ", ".join(s["keywords"]) if s["keywords"] else "не задано"
        
        kb = InlineKeyboardBuilder()
        kb.button(text="Изменить", callback_data="kw:edit")
        kb.button(text="Очистить", callback_data="kw:clear")
        kb.button(text="⬅︎ Назад", callback_data="back:edit")
        kb.adjust(2)
        
        await cq.message.edit_text(f"<b>Ключевые слова</b>\nТекущие: {current_kw}\n\nВыберите действие:", reply_markup=kb.as_markup())
        await cq.answer()
        return
    
    if param == "black":
        # Получаем текущий черный список
        s = user_settings(cq.from_user.id)
        current_bl = ", ".join(s["blacklist"]) if s["blacklist"] else "не задано"
        
        kb = InlineKeyboardBuilder()
        kb.button(text="Изменить", callback_data="black:edit")
        kb.button(text="Очистить", callback_data="black:clear")
        kb.button(text="⬅︎ Назад", callback_data="back:edit")
        kb.adjust(2)
        
        await cq.message.edit_text(f"<b>Черный список</b>\nТекущие слова: {current_bl}\n\nВыберите действие:", reply_markup=kb.as_markup())
        await cq.answer()
        return
    
    prompts = {
        "price": ("Введите цену в формате: мин.цена; макс.цена (Пример: 1000; 5000)", SearchStates.waiting_for_price),
        "pages": ("Введите количество страниц для сканирования:", SearchStates.waiting_for_pages),
        "pause": ("Введите паузу между сканированиями в секундах:", SearchStates.waiting_for_pause),
    }
    
    # Добавляем кнопку назад
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅︎ Назад", callback_data="back:edit")
    
    prompt_text, next_state = prompts.get(param, ("Введите значение:", None))
    if next_state:
        await state.update_data(edit_param=param, platform="avito")
        await state.set_state(next_state)
        await cq.message.edit_text(prompt_text, reply_markup=kb.as_markup())
        await cq.answer()

# ---------------------------------------------------------------------------
# EDIT INDIVIDUAL PARAM (CIAN)
# ---------------------------------------------------------------------------
@router.callback_query(F.data.startswith("edit_cian:"))
async def cb_edit_param_cian(cq: CallbackQuery, state: FSMContext):
    param = cq.data.split(":", 1)[1]
    
    if param == "kw":
        # Получаем текущие ключевые слова
        s = user_settings(cq.from_user.id, "cian")
        current_kw = ", ".join(s["keywords"]) if s["keywords"] else "не задано"
        
        kb = InlineKeyboardBuilder()
        kb.button(text="Изменить", callback_data="kw_cian:edit")
        kb.button(text="Очистить", callback_data="kw_cian:clear")
        kb.button(text="⬅︎ Назад", callback_data="back:edit_cian")
        kb.adjust(2)
        
        await cq.message.edit_text(f"<b>Ключевые слова (ЦИАН)</b>\nТекущие: {current_kw}\n\nВыберите действие:", reply_markup=kb.as_markup())
        await cq.answer()
        return
    
    if param == "black":
        # Получаем текущий черный список
        s = user_settings(cq.from_user.id, "cian")
        current_bl = ", ".join(s["blacklist"]) if s["blacklist"] else "не задано"
        
        kb = InlineKeyboardBuilder()
        kb.button(text="Изменить", callback_data="black_cian:edit")
        kb.button(text="Очистить", callback_data="black_cian:clear")
        kb.button(text="⬅︎ Назад", callback_data="back:edit_cian")
        kb.adjust(2)
        
        await cq.message.edit_text(f"<b>Черный список (ЦИАН)</b>\nТекущие слова: {current_bl}\n\nВыберите действие:", reply_markup=kb.as_markup())
        await cq.answer()
        return
    
    prompts = {
        "price": ("Введите цену в формате: мин.цена; макс.цена (Пример: 1000; 5000)", SearchStates.waiting_for_price),
        "pages": ("Введите количество страниц для сканирования:", SearchStates.waiting_for_pages),
        "pause": ("Введите паузу между сканированиями в секундах:", SearchStates.waiting_for_pause),
    }
    
    # Добавляем кнопку назад
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅︎ Назад", callback_data="back:edit_cian")
    
    prompt_text, next_state = prompts.get(param, ("Введите значение:", None))
    if next_state:
        await state.update_data(edit_param=param, platform="cian")
        await state.set_state(next_state)
        await cq.message.edit_text(prompt_text, reply_markup=kb.as_markup())
        await cq.answer()

# ---------------------------------------------------------------------------
# ОБРАБОТЧИКИ РЕДАКТИРОВАНИЯ НАСТРОЕК ЦИАН
# ---------------------------------------------------------------------------
@router.callback_query(F.data == "kw_cian:clear")
async def cb_kw_cian_clear(cq: CallbackQuery):
    save(cq.from_user.id, "keywords", "", platform="cian")
    await cq.message.edit_text("Ключевые слова ЦИАН очищены!")
    await asyncio.sleep(1)
    await cq.message.edit_text("Изменить параметры ЦИАН", reply_markup=kb_edit_params_cian())
    await cq.answer()

@router.callback_query(F.data == "kw_cian:edit")
async def cb_kw_cian_edit(cq: CallbackQuery, state: FSMContext):
    await state.update_data(platform="cian")
    await state.set_state(SearchStates.waiting_for_keywords)
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅︎ Назад", callback_data="back:edit_cian")
    await cq.message.edit_text("Введите ключевые слова через точку с запятой (;):", reply_markup=kb.as_markup())
    await cq.answer()

@router.callback_query(F.data == "black_cian:clear")
async def cb_black_cian_clear(cq: CallbackQuery):
    save(cq.from_user.id, "blacklist", "", platform="cian")
    await cq.message.edit_text("Черный список ЦИАН очищен!")
    await asyncio.sleep(1)
    await cq.message.edit_text("Изменить параметры ЦИАН", reply_markup=kb_edit_params_cian())
    await cq.answer()

@router.callback_query(F.data == "black_cian:edit")
async def cb_black_cian_edit(cq: CallbackQuery, state: FSMContext):
    await state.update_data(platform="cian")
    await state.set_state(SearchStates.waiting_for_blacklist)
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅︎ Назад", callback_data="back:edit_cian")
    await cq.message.edit_text("Введите слова для черного списка через точку с запятой (;):", reply_markup=kb.as_markup())
    await cq.answer()

# Обработчики для кнопок ключевых слов и черного списка (Авито)
@router.callback_query(F.data == "kw:clear")
async def cb_kw_clear(cq: CallbackQuery):
    save(cq.from_user.id, "keywords", "")
    await cq.message.edit_text("Ключевые слова очищены!")
    await asyncio.sleep(1)
    await cq.message.edit_text("Изменить параметры", reply_markup=kb_edit_params_avito())
    await cq.answer()

@router.callback_query(F.data == "kw:edit")
async def cb_kw_edit(cq: CallbackQuery, state: FSMContext):
    await state.update_data(platform="avito")
    await state.set_state(SearchStates.waiting_for_keywords)
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅︎ Назад", callback_data="back:edit")
    await cq.message.edit_text("Введите ключевые слова через точку с запятой (;):", reply_markup=kb.as_markup())
    await cq.answer()

@router.callback_query(F.data == "black:clear")
async def cb_black_clear(cq: CallbackQuery):
    save(cq.from_user.id, "blacklist", "")
    await cq.message.edit_text("Черный список очищен!")
    await asyncio.sleep(1)
    await cq.message.edit_text("Изменить параметры", reply_markup=kb_edit_params_avito())
    await cq.answer()

@router.callback_query(F.data == "black:edit")
async def cb_black_edit(cq: CallbackQuery, state: FSMContext):
    await state.update_data(platform="avito")
    await state.set_state(SearchStates.waiting_for_blacklist)
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅︎ Назад", callback_data="back:edit")
    await cq.message.edit_text("Введите слова для черного списка через точку с запятой (;):", reply_markup=kb.as_markup())
    await cq.answer()

@router.callback_query(F.data.startswith("new_only:"))
async def cb_new_only(cq: CallbackQuery):
    value = int(cq.data.split(":", 1)[1])
    save(cq.from_user.id, "new_only", value)
    
    if value:
        await cq.message.edit_text("⚠️ <b>Внимание!</b> Для корректной работы функции 'Только новые' необходимо настроить прокси в главном меню.\n\nПараметр 'Только новые' установлен на: Да\n\nПараметры успешно сохранены!")
    else:
        await cq.message.edit_text(f"Параметр 'Только новые' установлен на: Нет\n\nПараметры успешно сохранены!")
    
    await asyncio.sleep(3)  # Даем пользователю время прочитать сообщение
    await cq.message.edit_text("Изменить параметры", reply_markup=kb_edit_params_avito())
    await cq.answer()

# ---------------------------------------------------------------------------
# ОБРАБОТЧИКИ ВВОДА ЗНАЧЕНИЙ ПАРАМЕТРОВ
# ---------------------------------------------------------------------------
@router.message(StateFilter(SearchStates.waiting_for_price))
async def handle_price(message: Message, state: FSMContext):
    txt = message.text.strip()
    data = await state.get_data()
    platform = data.get("platform", "avito")
    back_command = "back:edit_cian" if platform == "cian" else "back:edit"
    
    try:
        parts = txt.replace(";", " ").split()
        if len(parts) != 2:
            # Добавляем кнопку назад
            kb = InlineKeyboardBuilder()
            kb.button(text="⬅︎ Назад", callback_data=back_command)
            await message.reply("Неверный формат. Введите два числа, разделенных точкой с запятой (;). Пример: 1000; 5000", reply_markup=kb.as_markup())
            return
        
        mn, mx = map(int, parts)
        save(message.from_user.id, "min_price", mn, platform)
        save(message.from_user.id, "max_price", mx, platform)
        await message.reply(f"Параметр 'Цена' успешно обновлен: {mn}–{mx}₽")
        await state.clear()
        
        # Возвращаем соответствующую клавиатуру
        if platform == "cian":
            await bot.send_message(message.chat.id, "Изменить параметры ЦИАН", reply_markup=kb_edit_params_cian())
        else:
            await bot.send_message(message.chat.id, "Изменить параметры", reply_markup=kb_edit_params_avito())
    except ValueError:
        # Добавляем кнопку назад
        kb = InlineKeyboardBuilder()
        kb.button(text="⬅︎ Назад", callback_data=back_command)
        await message.reply("Неверный формат. Необходимо ввести числа. Пример: 1000; 5000", reply_markup=kb.as_markup())

@router.message(StateFilter(SearchStates.waiting_for_pages))
async def handle_pages(message: Message, state: FSMContext):
    txt = message.text.strip()
    data = await state.get_data()
    platform = data.get("platform", "avito")
    back_command = "back:edit_cian" if platform == "cian" else "back:edit"
    
    try:
        pages = int(txt)
        if pages < 1:
            # Добавляем кнопку назад
            kb = InlineKeyboardBuilder()
            kb.button(text="⬅︎ Назад", callback_data=back_command)
            await message.reply("Количество страниц должно быть положительным числом.", reply_markup=kb.as_markup())
            return
        save(message.from_user.id, "pages", pages, platform)
        await message.reply(f"Параметр 'Страницы' успешно обновлен: {pages}")
        await state.clear()
        
        # Возвращаем пользователя в соответствующее меню редактирования
        if platform == "cian":
            await bot.send_message(message.chat.id, "Изменить параметры ЦИАН", reply_markup=kb_edit_params_cian())
        else:
            await bot.send_message(message.chat.id, "Изменить параметры", reply_markup=kb_edit_params_avito())
    except ValueError:
        # Добавляем кнопку назад
        kb = InlineKeyboardBuilder()
        kb.button(text="⬅︎ Назад", callback_data=back_command)
        await message.reply("Неверный формат. Введите целое число.", reply_markup=kb.as_markup())

@router.message(StateFilter(SearchStates.waiting_for_pause))
async def handle_pause(message: Message, state: FSMContext):
    txt = message.text.strip()
    data = await state.get_data()
    platform = data.get("platform", "avito")
    back_command = "back:edit_cian" if platform == "cian" else "back:edit"
    
    try:
        pause = int(txt)
        if pause < 10:
            # Добавляем кнопку назад
            kb = InlineKeyboardBuilder()
            kb.button(text="⬅︎ Назад", callback_data=back_command)
            await message.reply("Пауза должна быть не менее 10 секунд.", reply_markup=kb.as_markup())
            return
        save(message.from_user.id, "pause", pause, platform)
        await message.reply(f"Параметр 'Пауза' успешно обновлен: {pause} сек")
        await state.clear()
        
        # Возвращаем соответствующую клавиатуру
        if platform == "cian":
            await bot.send_message(message.chat.id, "Изменить параметры ЦИАН", reply_markup=kb_edit_params_cian())
        else:
            await bot.send_message(message.chat.id, "Изменить параметры", reply_markup=kb_edit_params_avito())
    except ValueError:
        # Добавляем кнопку назад
        kb = InlineKeyboardBuilder()
        kb.button(text="⬅︎ Назад", callback_data=back_command)
        await message.reply("Неверный формат. Введите целое число секунд.", reply_markup=kb.as_markup())

@router.message(StateFilter(SearchStates.waiting_for_keywords))
async def handle_keywords(message: Message, state: FSMContext):
    txt = message.text.strip()
    data = await state.get_data()
    platform = data.get("platform", "avito")
    
    keywords = _parse_list(txt)
    save(message.from_user.id, "keywords", txt, platform)
    keyword_list = ", ".join(keywords) if keywords else "список пуст"
    await message.reply(f"Параметр 'Ключевые слова' успешно обновлен: {keyword_list}")
    await state.clear()
    
    # Возвращаем соответствующую клавиатуру
    if platform == "cian":
        await bot.send_message(message.chat.id, "Изменить параметры ЦИАН", reply_markup=kb_edit_params_cian())
    else:
        await bot.send_message(message.chat.id, "Изменить параметры", reply_markup=kb_edit_params_avito())

@router.message(StateFilter(SearchStates.waiting_for_blacklist))
async def handle_blacklist(message: Message, state: FSMContext):
    txt = message.text.strip()
    data = await state.get_data()
    platform = data.get("platform", "avito")
    
    blacklist = _parse_list(txt)
    save(message.from_user.id, "blacklist", txt, platform)
    blacklist_items = ", ".join(blacklist) if blacklist else "список пуст"
    await message.reply(f"Параметр 'Чёрный список' успешно обновлен: {blacklist_items}")
    await state.clear()
    
    # Возвращаем соответствующую клавиатуру
    if platform == "cian":
        await bot.send_message(message.chat.id, "Изменить параметры ЦИАН", reply_markup=kb_edit_params_cian())
    else:
        await bot.send_message(message.chat.id, "Изменить параметры", reply_markup=kb_edit_params_avito())

# ---------------------------------------------------------------------------
# PROXY HANDLERS
# ---------------------------------------------------------------------------
@router.callback_query(F.data == "proxy:add")
async def cb_proxy_add(cq: CallbackQuery, state: FSMContext):
    # Получаем активные поиски
    rows = DB.list_active_searches(cq.from_user.id)
    active_searches_text = ""
    
    if rows:
        searches_list = []
        for row in rows:
            i, url = row[0], row[1]
            settings_json = json.loads(row[2])
            platform = settings_json.get('platform', 'avito')
            name = row[3] if len(row) > 3 and row[3] else ""
            display_name = f"#{i}" if not name else f"#{i}-{name}"
            searches_list.append(f"{display_name}")
        
        active_searches_text = "\n\n<b>Активные поиски, которые будут использовать новый прокси:</b>\n" + ", ".join(searches_list)
    
    # Отображаем инструкцию для ввода прокси
    text = (
        "Введите прокси в одном из форматов:\n\n"
        "1. username:password@server:port - для обычного прокси\n"
        "2. server:port:username:password - альтернативный формат\n"
        "3. https://changeip.mobileproxy.space/?proxy_key=*** - для API смены IP\n\n"
        "Для множественных прокси в формате #1 или #2 используйте разделитель ;\n"
        "Вы можете одновременно использовать обычный прокси и URL для смены IP.\n"
        f"{active_searches_text}"
    )
    await cq.message.edit_text(text, parse_mode="HTML")
    await state.set_state(SearchStates.waiting_for_proxy)
    await cq.answer()

@router.callback_query(F.data == "proxy:check")
async def cb_proxy_check(cq: CallbackQuery):
    user_id = cq.from_user.id
    proxy = DB.get_setting(user_id, "proxy")
    proxy_change_url = DB.get_setting(user_id, "proxy_change_url")
    
    if not proxy and not proxy_change_url:
        await cq.answer("Прокси не настроены. Нечего проверять.", show_alert=True)
        return
    
    await cq.message.edit_text("🔄 Начинаю проверку настроенных прокси...")
    
    all_checks_success = True
    
    # Проверяем основной прокси, если он настроен
    if proxy and proxy != "Не настроено":
        proxy_list = [p.strip() for p in proxy.split(';') if p.strip()]
        for proxy_str in proxy_list:
            normalized_proxy = normalize_proxy_format(proxy_str)
            # Добавляем прогресс-сообщение
            progress_msg = await cq.message.reply(f"🔄 Проверка прокси: {normalized_proxy}...")
            
            # Проверяем прокси
            test_result = await test_proxy(normalized_proxy, progress_msg)
            if not test_result['success']:
                all_checks_success = False
                
            # Итоговый результат проверки прокси
            if test_result['success']:
                await progress_msg.edit_text(f"✅ Прокси {normalized_proxy} успешно прошел проверку!")
            else:
                await progress_msg.edit_text(f"❌ Прокси {normalized_proxy} не прошел проверку:\n"
                                            f"- IP тест: {'успешно' if test_result['ip_check']['success'] else 'неудачно'}\n"
                                            f"- Авито тест: {'успешно' if test_result['avito_check']['success'] else 'неудачно'}\n"
                                            f"- ЦИАН тест: {'успешно' if test_result['cian_check']['success'] else 'неудачно'}")
    
    # Проверяем URL для смены IP, если он настроен
    if proxy_change_url and proxy_change_url != "Не настроено":
        # Добавляем прогресс-сообщение
        ip_progress_msg = await cq.message.reply(f"🔄 Проверка URL для смены IP...")
        
        # Проверяем URL
        ip_result = await test_ip_change_url(proxy_change_url, ip_progress_msg)
        if not ip_result['success']:
            all_checks_success = False
    
    # Получаем текущие настройки прокси
    proxy = DB.get_setting(user_id, "proxy") or "Не настроено"
    proxy_change_url = DB.get_setting(user_id, "proxy_change_url") or "Не настроено"
    
    # Формируем текст для отображения текущих настроек
    proxy_text = (
        "<b>Настройки прокси</b>\n\n"
        f"<b>Прокси в формате username:password@host:port или host:port:username:password:</b>\n{proxy}\n\n"
        f"<b>Ссылка для изменения IP, в формате https://changeip.mobileproxy.space/?proxy_key=***:</b>\n{proxy_change_url}\n\n"
        f"<b>Результат проверки:</b> {'✅ Все проверки успешны' if all_checks_success else '⚠️ Проверка завершена с предупреждениями'}"
    )
    
    await cq.message.edit_text(proxy_text, reply_markup=kb_proxy(), parse_mode="HTML")
    await cq.answer()

@router.message(StateFilter(SearchStates.waiting_for_proxy))
async def handle_proxy(message: Message, state: FSMContext):
    proxy_text = message.text.strip()
    user_id = message.from_user.id
    
    # Проверяем, содержит ли текст URL смены IP
    ip_change_url = None
    regular_proxy = None
    
    # Разделяем ввод на прокси и URL для смены IP
    lines = [line.strip() for line in proxy_text.split('\n') if line.strip()]
    for line in lines:
        if line.startswith("http") and ("renew-ip" in line.lower() or "proxy_key" in line.lower()):
            ip_change_url = line
        else:
            if regular_proxy:
                regular_proxy += ";" + line
            else:
                regular_proxy = line
    
    # Если введен только URL для смены IP
    if ip_change_url and not regular_proxy:
        # Проверяем работоспособность ссылки на смену IP
        await message.reply("🔄 Проверка URL для смены IP...")
        test_result = await test_ip_change_url(ip_change_url, message)
        
        if test_result['success']:
            DB.set_setting(user_id, "proxy_change_url", ip_change_url)
            
            # Обновляем настройки для всех активных поисков
            for job_id, job in ACTIVE.items():
                if job.user_id == user_id:
                    job.settings["proxy_change_url"] = ip_change_url
            
            await message.reply("✅ Ссылка API для смены IP успешно сохранена и может использоваться с прокси.")
        else:
            DB.set_setting(user_id, "proxy_change_url", ip_change_url)
            await message.reply(f"⚠️ Ссылка API для смены IP сохранена, но при проверке возникли проблемы: {test_result['message']}")
    
    # Если введен только обычный прокси
    if regular_proxy and not ip_change_url:
        # Проверяем каждый прокси в списке
        proxy_list = [p.strip() for p in regular_proxy.split(';') if p.strip()]
        all_failed = True
        
        await message.reply(f"🔄 Начинаю проверку прокси. Найдено {len(proxy_list)} прокси для проверки...")
        
        working_proxies = []
        
        for proxy in proxy_list:
            normalized_proxy = normalize_proxy_format(proxy)
            
            # Добавляем прогресс-сообщение
            progress_msg = await message.reply(f"🔄 Проверка прокси: {normalized_proxy}...")
            
            test_result = await test_proxy(normalized_proxy, progress_msg)
            
            if test_result['success']:
                all_failed = False
                working_proxies.append(normalized_proxy)
                await progress_msg.edit_text(f"✅ Прокси {normalized_proxy} успешно прошел проверку!")
            else:
                await progress_msg.edit_text(f"❌ Прокси {normalized_proxy} не прошел проверку:\n"
                                           f"- IP тест: {'успешно' if test_result['ip_check']['success'] else 'неудачно'}\n"
                                           f"- Авито тест: {'успешно' if test_result['avito_check']['success'] else 'неудачно'}\n"
                                           f"- ЦИАН тест: {'успешно' if test_result['cian_check']['success'] else 'неудачно'}")
        
        if not all_failed:
            # Сохраняем все прокси, ставя рабочие в начало списка
            final_proxy_list = working_proxies + [p for p in proxy_list if normalize_proxy_format(p) not in working_proxies]
            all_proxies = ";".join(final_proxy_list)
            
            DB.set_setting(user_id, "proxy", all_proxies)
            
            # Обновляем настройки для всех активных поисков
            for job_id, job in ACTIVE.items():
                if job.user_id == user_id:
                    job.settings["proxy"] = all_proxies
            
            # Сохраняем для всех платформ
            DB.set_setting(user_id, "cian_proxy", all_proxies)
            
            if working_proxies:
                await message.reply(f"✅ Найдено {len(working_proxies)} рабочих прокси из {len(proxy_list)}. Все прокси сохранены.")
            else:
                await message.reply("⚠️ Прокси сохранены, но ни один не прошел полную проверку. Рекомендуется проверить настройки.")
        else:
            # Сохраняем прокси даже если все не прошли проверку
            all_proxies = ";".join(proxy_list)
            DB.set_setting(user_id, "proxy", all_proxies)
            
            # Обновляем настройки для всех активных поисков
            for job_id, job in ACTIVE.items():
                if job.user_id == user_id:
                    job.settings["proxy"] = all_proxies
            
            # Сохраняем для всех платформ
            DB.set_setting(user_id, "cian_proxy", all_proxies)
            
            await message.reply("⚠️ Не удалось полностью проверить ни один из предоставленных прокси. Прокси сохранены, но могут не работать корректно.")
    
    # Если введен и прокси, и URL для смены IP
    if regular_proxy and ip_change_url:
        # Сначала сохраняем оба значения
        DB.set_setting(user_id, "proxy", regular_proxy)
        DB.set_setting(user_id, "proxy_change_url", ip_change_url)
        
        # Обновляем настройки для всех активных поисков
        for job_id, job in ACTIVE.items():
            if job.user_id == user_id:
                job.settings["proxy"] = regular_proxy
                job.settings["proxy_change_url"] = ip_change_url
        
        # Сохраняем для всех платформ
        DB.set_setting(user_id, "cian_proxy", regular_proxy)
        
        await message.reply("✅ Сохранены и прокси, и ссылка для смены IP. Они будут использоваться совместно.")
        
        # Теперь проверяем их
        await message.reply("🔄 Проверка сохраненных настроек...")
        
        # Проверяем URL для смены IP
        ip_result = await test_ip_change_url(ip_change_url, message)
        
        # Проверяем основной прокси
        proxy_list = [p.strip() for p in regular_proxy.split(';') if p.strip()]
        if proxy_list:
            normalized_proxy = normalize_proxy_format(proxy_list[0])
            test_result = await test_proxy(normalized_proxy, message)
            
            if test_result['success'] and ip_result['success']:
                await message.reply("✅ Проверка успешно завершена! И прокси, и URL для смены IP работают корректно.")
            else:
                await message.reply("⚠️ Проверка завершена с предупреждениями. Рекомендуется проверить настройки.")
    
    await state.clear()
    
    # Получаем текущие настройки прокси для отображения
    proxy = DB.get_setting(user_id, "proxy") or "Не настроено"
    proxy_change_url = DB.get_setting(user_id, "proxy_change_url") or "Не настроено"
    
    # Формируем текст для отображения текущих настроек
    proxy_text = (
        "<b>Настройки прокси</b>\n\n"
        f"<b>Прокси в формате username:password@host:port или host:port:username:password:</b>\n{proxy}\n\n"
        f"<b>Ссылка для изменения IP, в формате https://changeip.mobileproxy.space/?proxy_key=***:</b>\n{proxy_change_url}"
    )
    
    await bot.send_message(message.chat.id, proxy_text, reply_markup=kb_proxy(), parse_mode="HTML")

@router.callback_query(F.data == "proxy:del")
async def cb_proxy_del(cq: CallbackQuery, state: FSMContext):
    # Получаем текущие настройки прокси
    user_id = cq.from_user.id
    proxy = DB.get_setting(user_id, "proxy") or "Не настроено"
    proxy_change_url = DB.get_setting(user_id, "proxy_change_url") or "Не настроено"
    
    # Проверяем, есть ли что удалять
    if proxy == "Не настроено" and proxy_change_url == "Не настроено":
        await cq.answer("Прокси не настроены, нечего удалять", show_alert=True)
        return
    
    # Получаем активные поиски
    rows = DB.list_active_searches(user_id)
    active_searches_text = ""
    
    if rows:
        searches_list = []
        for row in rows:
            i, url = row[0], row[1]
            settings_json = json.loads(row[2])
            platform = settings_json.get('platform', 'avito')
            name = row[3] if len(row) > 3 and row[3] else ""
            display_name = f"#{i}" if not name else f"#{i}-{name}"
            searches_list.append(f"{display_name}")
        
        active_searches_text = "\n\n<b>Активные поиски, которые будут работать без прокси:</b>\n" + ", ".join(searches_list)
    
    # Запрашиваем подтверждение удаления
    text = (
        "<b>Удаление настроек прокси</b>\n\n"
        f"<b>Прокси:</b> {proxy}\n"
        f"<b>Ссылка API:</b> {proxy_change_url}"
        f"{active_searches_text}\n\n"
        "Вы действительно хотите удалить все настройки прокси?"
    )
    
    await cq.message.edit_text(text, reply_markup=kb_confirm_proxy_delete(), parse_mode="HTML")
    await state.set_state(SearchStates.waiting_for_proxy_confirm)
    await cq.answer()

@router.callback_query(F.data.startswith("proxy_confirm:"))
async def cb_proxy_confirm(cq: CallbackQuery, state: FSMContext):
    action = cq.data.split(":", 1)[1]
    user_id = cq.from_user.id
    
    if action == "yes":
        # Удаляем настройки прокси из базы данных
        DB.delete_setting(user_id, "proxy")
        DB.delete_setting(user_id, "cian_proxy")
        DB.delete_setting(user_id, "proxy_change_url")
        
        # Обновляем настройки для всех активных поисков
        for job_id, job in ACTIVE.items():
            if job.user_id == user_id:
                job.settings["proxy"] = None
                job.settings["proxy_change_url"] = None
        
        await cq.message.edit_text("Все настройки прокси удалены. Активные поиски теперь работают без прокси.")
        await asyncio.sleep(2)  # Даем время прочитать сообщение
        
        # Вернуться к главному меню
        await cq.message.edit_text(f"Здравствуйте, {cq.from_user.first_name}! На что желаете поохотиться сегодня?", reply_markup=kb_main())
    else:
        # Отмена удаления, возврат к просмотру настроек прокси
        # Получаем текущие настройки прокси
        proxy = DB.get_setting(user_id, "proxy") or "Не настроено"
        proxy_change_url = DB.get_setting(user_id, "proxy_change_url") or "Не настроено"
        
        # Формируем текст для отображения текущих настроек
        proxy_text = (
            "<b>Настройки прокси</b>\n\n"
            f"<b>Прокси в формате username:password@host:port:</b>\n{proxy}\n\n"
            f"<b>Ссылка для изменения IP, в формате https://changeip.mobileproxy.space/?proxy_key=***:</b>\n{proxy_change_url}"
        )
        
        await cq.message.edit_text(proxy_text, reply_markup=kb_proxy(), parse_mode="HTML")
    
    await state.clear()
    await cq.answer()

# ---------------------------------------------------------------------------
# STOP SEARCH HANDLING
# ---------------------------------------------------------------------------
@router.message(StateFilter(SearchStates.waiting_for_stop_id))
async def handle_stop_search(message: Message, state: FSMContext):
    try:
        search_id = int(message.text.strip())
    except ValueError:
        await message.reply("Необходимо указать числовой ID поиска.")
        await state.clear()
        await bot.send_message(message.chat.id, f"Здравствуйте, {message.from_user.first_name}! На что желаете поохотиться сегодня?", reply_markup=kb_main())
        return
    
    # Check if the search exists and belongs to this user
    rows = DB.list_active_searches(message.from_user.id)
    search_exists = any(row[0] == search_id for row in rows)
    
    if not search_exists:
        await message.reply(f"Поиск #{search_id} не найден или не принадлежит вам.")
        await state.clear()
        await bot.send_message(message.chat.id, f"Здравствуйте, {message.from_user.first_name}! На что желаете поохотиться сегодня?", reply_markup=kb_main())
        return
    
    # Show confirmation buttons
    kb = InlineKeyboardBuilder()
    kb.button(text="Да", callback_data=f"stop_confirm:yes:{search_id}")
    kb.button(text="Нет", callback_data="stop_confirm:no:0")
    kb.adjust(2)
    
    await message.reply(f"Вы уверены, что хотите остановить поиск #{search_id}?", reply_markup=kb.as_markup())
    await state.clear()

@router.callback_query(F.data.startswith("stop_confirm:"))
async def cb_stop_confirm(cq: CallbackQuery):
    parts = cq.data.split(":", 2)
    if len(parts) != 3:
        await cq.answer("Ошибка в данных подтверждения")
        return
    
    _, action, sid_str = parts
    
    if action == "no":
        await cq.message.edit_text("Отмена остановки поиска.")
        await cq.answer()
        await bot.send_message(cq.message.chat.id, f"Здравствуйте, {cq.from_user.first_name}! На что желаете поохотиться сегодня?", reply_markup=kb_main())
        return
    
    # Handle "yes" confirmation
    try:
        sid = int(sid_str)
        
        # Stop the job
        if sid in ACTIVE:
            job = ACTIVE[sid]
            job.stop_event.set()
            
            # Получаем статистику из объекта job
            stats_text = ""
            try:
                # Используем общую статистику за все итерации
                if job.total_new_ads > 0 or job.total_notified_ads > 0:
                    stats_text = (
                        f"\n\n<b>Статистика сканирования:</b>\n"
                        f"• Найдено новых объявлений: {job.total_new_ads}\n"
                        f"• Отправлено уведомлений (прошли фильтры): {job.total_notified_ads}"
                    )
            except Exception as e:
                logger.error(f"Ошибка при получении статистики: {e}")
            
            with suppress(Exception):
                scheduler.remove_job(str(sid))
            
            # Mark as inactive in DB
            DB.deactivate_search(sid)
            
            platform_name = job.platform.upper()
            await cq.message.edit_text(f"Поиск {platform_name} #{sid} остановлен.{stats_text}", parse_mode="HTML")
            await cq.answer()
        else:
            await cq.message.edit_text(f"Поиск #{sid} не найден в активных задачах.")
            await cq.answer()
    except Exception as e:
        await cq.answer(f"Ошибка: {e}", show_alert=True)
    
    await bot.send_message(cq.message.chat.id, f"Здравствуйте, {cq.from_user.first_name}! На что желаете поохотиться сегодня?", reply_markup=kb_main())

# ---------------------------------------------------------------------------
# BACKGROUND TASK - AVITO
# ---------------------------------------------------------------------------
async def run_avito(job: SearchJob):
    s = job.settings
    
    # Если настроен прокси, подготовим его
    proxy_to_use = s.get("proxy")
    proxy_change_url = s.get("proxy_change_url")
    
    # Если настроена ссылка для смены IP, пытаемся сменить IP перед парсингом
    if proxy_change_url:
        try:
            logger.info(f"Пытаюсь изменить IP через API: {proxy_change_url}")
            response = requests.get(proxy_change_url, timeout=15)
            if response.status_code == 200:
                logger.info("IP успешно изменен через API")
                # Небольшая пауза для применения нового IP
                await asyncio.sleep(3)
            else:
                logger.error(f"Ошибка при смене IP: код {response.status_code}")
        except Exception as e:
            logger.error(f"Ошибка при смене IP: {e}")
    
    parser = AvitoParse(
        url=job.urls,
        count=s["pages"],
        proxy=proxy_to_use,
        proxy_change_url=proxy_change_url,
        min_price=s["min_price"],
        max_price=s["max_price"],
        keysword_list=s["keywords"],
        keysword_black_list=s["blacklist"],
        max_views=0 if s["new_only"] else None,
        stop_event=job.stop_event,
        need_more_info=1,
        tg_token=TOKEN,
        chat_id=job.user_id,
        job_name=f"#{job.sid}" if not job.name else f"#{job.sid}-{job.name}",
        first_run=job.first_run
    )
    
    # Сохраняем ссылку на парсер в job для доступа к статистике
    job.parser = parser
    
    # Запускаем парсинг
    await asyncio.get_running_loop().run_in_executor(None, parser.parse)
    
    # Если это не первый запуск, обновляем общую статистику
    if not job.first_run:
        stats = parser.get_statistics()
        job.total_new_ads += stats.get('total_new_ads', 0)
        job.total_notified_ads += stats.get('total_notified_ads', 0)
        logger.info(f"Поиск #{job.sid}: обновлена статистика. Всего найдено: {job.total_new_ads}, отправлено: {job.total_notified_ads}")

# ---------------------------------------------------------------------------
# BACKGROUND TASK - CIAN
# ---------------------------------------------------------------------------
async def run_cian(job: SearchJob):
    s = job.settings
    
    # Если настроен прокси, подготовим его
    proxy_to_use = s.get("proxy")
    proxy_change_url = s.get("proxy_change_url")
    
    # Если настроена ссылка для смены IP, пытаемся сменить IP перед парсингом
    if proxy_change_url:
        try:
            logger.info(f"Пытаюсь изменить IP через API: {proxy_change_url}")
            response = requests.get(proxy_change_url, timeout=15)
            if response.status_code == 200:
                logger.info("IP успешно изменен через API")
                # Небольшая пауза для применения нового IP
                await asyncio.sleep(3)
            else:
                logger.error(f"Ошибка при смене IP: код {response.status_code}")
        except Exception as e:
            logger.error(f"Ошибка при смене IP: {e}")
    
    parser = CianParse(
        url=job.urls,
        count=s.get("pages", 5),  # Количество страниц для сканирования
        proxy=proxy_to_use,
        min_price=s.get("min_price", 0),
        max_price=s.get("max_price", 9_999_999),
        keysword_list=s.get("keywords", []),
        keysword_black_list=s.get("blacklist", []),
        pause=s.get("pause", 300),  # Пауза между сканированиями
        stop_event=job.stop_event,
        tg_token=TOKEN,
        chat_id=job.user_id,
        job_name=f"#{job.sid}" if not job.name else f"#{job.sid}-{job.name}",
        first_run=job.first_run
    )
    
    # Сохраняем ссылку на парсер в job для доступа к статистике
    job.parser = parser
    
    # Запускаем парсинг
    await asyncio.get_running_loop().run_in_executor(None, parser.parse)
    
    # Если это не первый запуск, обновляем общую статистику
    if not job.first_run:
        stats = parser.get_statistics()
        job.total_new_ads += stats.get('total_new_ads', 0)
        job.total_notified_ads += stats.get('total_notified_ads', 0)
        logger.info(f"Поиск ЦИАН #{job.sid}: обновлена статистика. Всего найдено: {job.total_new_ads}, отправлено: {job.total_notified_ads}")

# ---------------------------------------------------------------------------
# RESTORE JOBS
# ---------------------------------------------------------------------------
async def _restore():
    for row in DB.list_active_searches():
        sid = row[0]
        urls = row[1].split()
        st = json.loads(row[2])
        
        # Получаем платформу из настроек
        platform = st.get('platform', 'avito')
        
        # Проверяем, есть ли поле name в результате запроса
        name = row[3] if len(row) > 3 else ""
        
        ev = asyncio.Event()
        # При восстановлении задачи ставим first_run = False, так как начальное сканирование уже было
        ACTIVE[sid] = SearchJob(sid, 0, platform, urls, st, ev, first_run=False, name=name)
        
        # Запускаем задачу в зависимости от платформы
        if platform == "cian":
            scheduler.add_job(run_cian, "interval", seconds=st.get("pause", 300), args=[ACTIVE[sid]], id=str(sid))
        else:
            scheduler.add_job(run_avito, "interval", seconds=st.get("pause", 120), args=[ACTIVE[sid]], id=str(sid))

# ---------------------------------------------------------------------------
async def main():
    # При запуске очищаем историю сканирований Авито
    DB.clean_scan_history()
    
    # Очищаем историю сканирований ЦИАН
    DB.clean_cian_scan_history()
    
    # Очищаем данные просмотренных объявлений ЦИАН 
    DB.clean_cian_viewed()
    
    # Очищаем активные поиски
    DB.clean_active_searches()
    
    # Радикальный сброс счетчика поисков - пересоздаем таблицу searches
    success = DB.reset_search_counter()
    if success:
        logger.info("Счетчик поисков успешно сброшен до 1")
    else:
        logger.warning("Не удалось сбросить счетчик поисков")
    
    # Восстанавливаем старые поиски, только если это нужно
    # await _restore()  # Закомментировано для обнуления поисков при перезапуске
    
    scheduler.start()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())