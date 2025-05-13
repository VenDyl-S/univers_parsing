"""bot.py — Telegram‑меню + Avito‑парсер + ЦИАН-парсер"""

import asyncio
import json
import os
import time
from contextlib import suppress
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

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

from db_service import SQLiteDBHandler
from parser_avito import AvitoParse
from parser_cian import CianParse

TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN:
    raise RuntimeError("Нужен BOT_TOKEN в .env")

bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
router = Router()
dp.include_router(router)

scheduler = AsyncIOScheduler()
DB = SQLiteDBHandler()

class SearchStates(StatesGroup):
    waiting_for_urls = State()
    waiting_for_name = State()
    waiting_for_price = State()
    waiting_for_pages = State()
    waiting_for_pause = State()
    waiting_for_keywords = State()
    waiting_for_blacklist = State()
    waiting_for_proxy = State()
    waiting_for_proxy_url = State()
    waiting_for_stop_id = State()
    waiting_for_proxy_confirm = State()

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
    parser: Any = None
    total_new_ads: int = 0
    total_notified_ads: int = 0

ACTIVE: dict[int, SearchJob] = {}

def kb_main() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="🏠 Авито", callback_data="menu:avito")
    b.button(text="🏙️ ЦИАН", callback_data="menu:cian")
    b.button(text="📋 Показать поиски", callback_data="menu:show_searches")
    b.button(text="🔑 Прокси", callback_data="menu:proxy")
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
    b.button(text="Удалить", callback_data="proxy:del")
    b.button(text="Проверить", callback_data="proxy:check")
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

DEFAULT_AVITO = {
    "min_price": 0,
    "max_price": 9_999_999,
    "pages": 5,
    "pause": 120,
    "new_only": False,
    "keywords": [],
    "blacklist": [],
    "proxy": None,
}

DEFAULT_CIAN = {
    "min_price": 0,
    "max_price": 9_999_999,
    "pages": 5,
    "pause": 300,
    "keywords": [],
    "blacklist": [],
    "proxy": None,
}


def _parse_list(txt: str) -> List[str]:
    return [w.strip() for w in txt.split(";") if w.strip()]


def user_settings(uid: int, platform: str = "avito") -> Dict[str, Any]:
    """Получает настройки пользователя для указанной платформы."""
    if platform == "avito":
        s = DEFAULT_AVITO.copy()
    else:
        s = DEFAULT_CIAN.copy()
    
    settings_prefix = f"{platform}_" if platform != "avito" else ""
    user_settings = DB.list_settings(uid)
    
    platform_settings = {}
    for key, value in user_settings.items():
        if platform == "avito" and not key.startswith("cian_"):
            platform_settings[key] = value
        elif platform != "avito" and key.startswith(settings_prefix):
            clean_key = key[len(settings_prefix):]
            platform_settings[clean_key] = value
    
    s.update(platform_settings)
    
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
    
    if "keywords" in s:
        s["keywords"] = _parse_list(s["keywords"]) if isinstance(s["keywords"], str) else s["keywords"]
    if "blacklist" in s:
        s["blacklist"] = _parse_list(s["blacklist"]) if isinstance(s["blacklist"], str) else s["blacklist"]
    
    return s


def save(uid: int, key: str, value: Any, platform: str = "avito"):
    """Сохраняет настройку в базу данных с учетом платформы."""
    if platform != "avito":
        key = f"{platform}_{key}"
    DB.set_setting(uid, key, str(value))

@router.message(Command("start"))
async def cmd_start(m: Message):
    await m.answer(f"Здравствуйте, {m.from_user.first_name}! На что желаете поохотиться сегодня?", reply_markup=kb_main())

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
    verified = "Да" if DB.get_setting(user_id, "proxy_verified") == "1" else "Нет"
    
    # Формируем текст для отображения текущих настроек
    proxy_text = (
        "<b>Настройки прокси</b>\n\n"
        f"<b>Прокси в одном из форматов:</b>\n• username:password@server:port\n• server:port:username:password\n\n"
        f"<b>Текущий прокси:</b>\n{proxy}\n\n"
        f"<b>Ссылка для изменения IP:</b>\n{proxy_change_url}\n\n"
        f"<b>Проверен:</b> {verified}"
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
        i, url = row[0], row[1]
        settings_json = json.loads(row[2])
        platform = settings_json.get('platform', 'avito')
        platform_icon = "🏠" if platform == "avito" else "🏙️" if platform == "cian" else "🚆"
        
        name = row[3] if len(row) > 3 and row[3] else ""
        display_name = f"#{i}" if not name else f"#{i}-{name}"
        
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
        i, url = row[0], row[1]
        settings_json = json.loads(row[2])
        platform = settings_json.get('platform', 'avito')
        platform_icon = "🏠" if platform == "avito" else "🏙️" if platform == "cian" else "🚆"
        
        name = row[3] if len(row) > 3 and row[3] else ""
        display_name = f"#{i}" if not name else f"#{i}-{name}"
        
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
        
        if platform == "cian":
            await bot.send_message(message.chat.id, "Не забудьте проверить параметры перед новым поиском ЦИАН.", reply_markup=kb_cian())
        else:
            await bot.send_message(message.chat.id, "Не забудьте проверить параметры перед новым поиском.", reply_markup=kb_avito())
        return
    
    st = user_settings(message.from_user.id, platform)
    sid = DB.add_search(message.from_user.id, platform, urls, st, name)
    ev = asyncio.Event()
    
    job = SearchJob(sid, message.from_user.id, platform, urls, st, ev, first_run=True, name=name)
    ACTIVE[sid] = job
    
    platform_name = "ЦИАН" if platform == "cian" else "Авито"
    await message.reply(f"Запускаю поиск {platform_name} #{sid}{'-'+name if name else ''}...")
    
    if platform == "cian":
        await run_cian(job)
    else:
        await run_avito(job)
    
    job.first_run = False
    
    interval_seconds = st["pause"]
    
    if platform == "cian":
        scheduler.add_job(run_cian, "interval", seconds=interval_seconds, args=[job], id=str(sid))
    else:
        scheduler.add_job(run_avito, "interval", seconds=interval_seconds, args=[job], id=str(sid))
    
    display_name = f"Поиск {platform_name} #{sid}" if not name else f"Поиск {platform_name} #{sid}-{name}"
    await message.reply(f"{display_name} запущен.\nПервичное сканирование завершено. Теперь будут приходить уведомления только о новых объявлениях.")
    await state.clear()
    
    if platform == "cian":
        await bot.send_message(message.chat.id, "Не забудьте проверить параметры перед новым поиском ЦИАН.", reply_markup=kb_cian())
    else:
        await bot.send_message(message.chat.id, "Не забудьте проверить параметры перед новым поиском.", reply_markup=kb_avito())

@router.callback_query(F.data.startswith("edit:"))
async def cb_edit_param(cq: CallbackQuery, state: FSMContext):
    param = cq.data.split(":", 1)[1]
    
    if param == "new":
        await cq.message.edit_text("Показывать только новые объявления?", reply_markup=kb_yes_no())
        await cq.answer()
        return
    
    if param == "kw":
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
    
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅︎ Назад", callback_data="back:edit")
    
    prompt_text, next_state = prompts.get(param, ("Введите значение:", None))
    if next_state:
        await state.update_data(edit_param=param, platform="avito")
        await state.set_state(next_state)
        await cq.message.edit_text(prompt_text, reply_markup=kb.as_markup())
        await cq.answer()

@router.callback_query(F.data.startswith("edit_cian:"))
async def cb_edit_param_cian(cq: CallbackQuery, state: FSMContext):
    param = cq.data.split(":", 1)[1]
    
    if param == "kw":
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
    
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅︎ Назад", callback_data="back:edit_cian")
    
    prompt_text, next_state = prompts.get(param, ("Введите значение:", None))
    if next_state:
        await state.update_data(edit_param=param, platform="cian")
        await state.set_state(next_state)
        await cq.message.edit_text(prompt_text, reply_markup=kb.as_markup())
        await cq.answer()

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
    
    await asyncio.sleep(3)
    await cq.message.edit_text("Изменить параметры", reply_markup=kb_edit_params_avito())
    await cq.answer()

@router.message(StateFilter(SearchStates.waiting_for_price))
async def handle_price(message: Message, state: FSMContext):
    txt = message.text.strip()
    data = await state.get_data()
    platform = data.get("platform", "avito")
    back_command = "back:edit_cian" if platform == "cian" else "back:edit"
    
    try:
        parts = txt.replace(";", " ").split()
        if len(parts) != 2:
            kb = InlineKeyboardBuilder()
            kb.button(text="⬅︎ Назад", callback_data=back_command)
            await message.reply("Неверный формат. Введите два числа, разделенных точкой с запятой (;). Пример: 1000; 5000", reply_markup=kb.as_markup())
            return
        
        mn, mx = map(int, parts)
        save(message.from_user.id, "min_price", mn, platform)
        save(message.from_user.id, "max_price", mx, platform)
        await message.reply(f"Параметр 'Цена' успешно обновлен: {mn}–{mx}₽")
        await state.clear()
        
        if platform == "cian":
            await bot.send_message(message.chat.id, "Изменить параметры ЦИАН", reply_markup=kb_edit_params_cian())
        else:
            await bot.send_message(message.chat.id, "Изменить параметры", reply_markup=kb_edit_params_avito())
    except ValueError:
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
            kb = InlineKeyboardBuilder()
            kb.button(text="⬅︎ Назад", callback_data=back_command)
            await message.reply("Количество страниц должно быть положительным числом.", reply_markup=kb.as_markup())
            return
        save(message.from_user.id, "pages", pages, platform)
        await message.reply(f"Параметр 'Страницы' успешно обновлен: {pages}")
        await state.clear()
        
        if platform == "cian":
            await bot.send_message(message.chat.id, "Изменить параметры ЦИАН", reply_markup=kb_edit_params_cian())
        else:
            await bot.send_message(message.chat.id, "Изменить параметры", reply_markup=kb_edit_params_avito())
    except ValueError:
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
        if pause < 1:  # Убираем минимальное ограничение, было: if pause < 10
            kb = InlineKeyboardBuilder()
            kb.button(text="⬅︎ Назад", callback_data=back_command)
            await message.reply("Пауза должна быть положительным числом.", reply_markup=kb.as_markup())
            return
        save(message.from_user.id, "pause", pause, platform)
        await message.reply(f"Параметр 'Пауза' успешно обновлен: {pause} сек")
        await state.clear()
        
        if platform == "cian":
            await bot.send_message(message.chat.id, "Изменить параметры ЦИАН", reply_markup=kb_edit_params_cian())
        else:
            await bot.send_message(message.chat.id, "Изменить параметры", reply_markup=kb_edit_params_avito())
    except ValueError:
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
    
    if platform == "cian":
        await bot.send_message(message.chat.id, "Изменить параметры ЦИАН", reply_markup=kb_edit_params_cian())
    else:
        await bot.send_message(message.chat.id, "Изменить параметры", reply_markup=kb_edit_params_avito())

@router.callback_query(F.data == "proxy:add")
async def cb_proxy_add(cq: CallbackQuery, state: FSMContext):
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
    
    text = (
        "Шаг 1 из 2: Введите прокси в одном из форматов:\n\n"
        "- username:password@server:port\n"
        "- host:port:username:password\n\n"
        f"{active_searches_text}"
    )
    await cq.message.edit_text(text, parse_mode="HTML")
    await state.set_state(SearchStates.waiting_for_proxy)
    await cq.answer()

@router.message(StateFilter(SearchStates.waiting_for_proxy))
async def handle_proxy(message: Message, state: FSMContext):
    proxy_text = message.text.strip()
    user_id = message.from_user.id
    
    await state.update_data(proxy=proxy_text)
    
    text = (
        "Шаг 2 из 2: Введите ссылку для смены IP, например:\n\n"
        "https://changeip.mobileproxy.space/?proxy_key=***\n\n"
        "Или отправьте 'пропустить', если не используете смену IP."
    )
    await message.reply(text)
    
    await state.set_state(SearchStates.waiting_for_proxy_url)

@router.message(StateFilter(SearchStates.waiting_for_proxy_url))
async def handle_proxy_url(message: Message, state: FSMContext):
    proxy_url = message.text.strip()
    user_id = message.from_user.id
    
    # Получаем сохраненный прокси из состояния
    data = await state.get_data()
    proxy_text = data.get("proxy", "")
    
    # Проверяем, не хочет ли пользователь пропустить этот шаг
    if proxy_url.lower() in ["пропустить", "skip", "-"]:
        proxy_url = ""
    
    # Сохраняем настройки в базу данных
    DB.set_setting(user_id, "proxy", proxy_text)
    DB.set_setting(user_id, "proxy_change_url", proxy_url)
    
    # Сбрасываем статус проверки прокси при его изменении
    DB.set_setting(user_id, "proxy_verified", "0")
    
    # Обновляем настройки для всех активных поисков
    for job_id, job in ACTIVE.items():
        if job.user_id == user_id:
            job.settings["proxy"] = proxy_text
            job.settings["proxy_change_url"] = proxy_url
    
    # Сохраняем для всех платформ
    DB.set_setting(user_id, "cian_proxy", proxy_text)
    
    # Уведомляем пользователя о сохранении
    await message.reply("Настройки прокси успешно сохранены.")
    
    # Показываем текущие настройки прокси
    proxy = DB.get_setting(user_id, "proxy") or "Не настроено"
    proxy_change_url = DB.get_setting(user_id, "proxy_change_url") or "Не настроено"
    verified = "Да" if DB.get_setting(user_id, "proxy_verified") == "1" else "Нет"
    
    proxy_text = (
        "<b>Настройки прокси</b>\n\n"
        f"<b>Прокси в формате username:password@server:port:</b>\n{proxy}\n\n"
        f"<b>Ссылка для изменения IP, в формате https://changeip.mobileproxy.space/?proxy_key=***:</b>\n{proxy_change_url}\n\n"
        f"<b>Проверен:</b> {verified}"
    )
    
    await message.answer(proxy_text, reply_markup=kb_proxy(), parse_mode="HTML")
    
    # Очищаем состояние
    await state.clear()

async def verify_proxy(user_id: int, message_obj) -> None:
    """
    Проверяет работу прокси и смену IP
    message_obj: может быть объектом сообщения или запросом callback
    """
    proxy = DB.get_setting(user_id, "proxy") or ""
    proxy_url = DB.get_setting(user_id, "proxy_change_url") or ""
    
    # Проверяем, настроен ли прокси
    if not proxy:
        if hasattr(message_obj, 'message'):
            await message_obj.message.edit_text("Прокси не настроен. Сначала добавьте прокси.")
        else:
            await bot.send_message(message_obj.chat.id, "Прокси не настроен. Сначала добавьте прокси.")
        return
    
    # Создаем сообщение о начале проверки
    if hasattr(message_obj, 'message'):
        status_message = await message_obj.message.edit_text("🔄 Проверка прокси...\n\nПолучение текущего IP...")
    else:
        status_message = await bot.send_message(message_obj.chat.id, "🔄 Проверка прокси...\n\nПолучение текущего IP...")
    
    try:
        # Создаем сессию с прокси
        session = requests.Session()
        if ":" in proxy and "@" in proxy:
            # Формат username:password@server:port
            proxies = {
                "http": f"http://{proxy}",
                "https": f"http://{proxy}"
            }
        elif proxy.count(":") == 3:
            # Формат host:port:username:password
            parts = proxy.split(":")
            if len(parts) == 4:
                host, port, username, password = parts
                formatted_proxy = f"{username}:{password}@{host}:{port}"
                proxies = {
                    "http": f"http://{formatted_proxy}",
                    "https": f"http://{formatted_proxy}"
                }
        else:
            await status_message.edit_text("❌ Неверный формат прокси. Используйте один из форматов:\n"
                                         "• username:password@server:port\n"
                                         "• server:port:username:password")
            return
        
        session.proxies.update(proxies)
        
        # Получаем исходный IP
        try:
            start_time = time.time()
            original_ip_response = session.get("https://api.ipify.org", timeout=15)
            if original_ip_response.status_code != 200:
                await status_message.edit_text(f"❌ Не удалось получить исходный IP. "
                                            f"Код ответа: {original_ip_response.status_code}")
                return
                
            original_ip = original_ip_response.text
            await status_message.edit_text(f"🔄 Проверка прокси...\n\nТекущий IP: {original_ip}\n\n"
                                        f"Выполняется смена IP...")
            
            # Если URL для смены IP не указан
            if not proxy_url:
                await status_message.edit_text(f"⚠️ Прокси работает, но URL для смены IP не указан.\n\n"
                                            f"IP: {original_ip}\n\n"
                                            f"Для полноценной работы рекомендуется указать URL смены IP.")
                return
                
            # Выполняем запрос для смены IP
            try:
                change_ip_response = requests.get(proxy_url, timeout=15)
                if change_ip_response.status_code != 200:
                    await status_message.edit_text(f"❌ Не удалось сменить IP. "
                                                f"Код ответа: {change_ip_response.status_code}\n"
                                                f"Текущий IP: {original_ip}")
                    return
                
                # Обновляем сообщение с таймером
                await status_message.edit_text(f"🔄 Проверка прокси...\n\n"
                                            f"Исходный IP: {original_ip}\n\n"
                                            f"Выполнена отправка запроса на смену IP.\n"
                                            f"Ожидание новой сессии... (0:00)")
                
                # Проверяем новый IP с таймаутом в 1 минуту
                timeout_time = start_time + 60
                new_ip = original_ip
                attempts = 0
                
                # Цикл ожидания смены IP
                while new_ip == original_ip and time.time() < timeout_time:
                    elapsed = int(time.time() - start_time)
                    mins = elapsed // 60
                    secs = elapsed % 60
                    timer = f"{mins}:{secs:02d}"
                    
                    await status_message.edit_text(f"🔄 Проверка прокси...\n\n"
                                                f"Исходный IP: {original_ip}\n\n"
                                                f"Ожидание смены IP... ({timer})")
                    
                    # Пауза между проверками
                    await asyncio.sleep(5)
                    
                    try:
                        new_ip_response = session.get("https://api.ipify.org", timeout=10)
                        if new_ip_response.status_code == 200:
                            new_ip = new_ip_response.text
                            if new_ip != original_ip:
                                break
                        attempts += 1
                    except Exception as e:
                        attempts += 1
                        logger.error(f"Ошибка при проверке нового IP: {e}")
                        continue
                
                # Проверяем результат смены IP
                elapsed_time = time.time() - start_time
                
                if new_ip != original_ip:
                    # Сохраняем информацию о успешной проверке
                    DB.set_setting(user_id, "proxy_verified", "1")
                    DB.set_setting(user_id, "proxy_last_check", str(int(time.time())))
                    
                    await status_message.edit_text(f"✅ Прокси работает! IP успешно изменен.\n\n"
                                                f"Исходный IP: {original_ip}\n"
                                                f"Новый IP: {new_ip}\n"
                                                f"Время смены: {elapsed_time:.1f} сек.")
                else:
                    # Сохраняем информацию о неуспешной проверке
                    DB.set_setting(user_id, "proxy_verified", "0")
                    
                    await status_message.edit_text(f"❌ Не удалось сменить IP в течение 1 минуты.\n\n"
                                                f"Исходный IP: {original_ip}\n\n"
                                                f"Проверьте правильность URL для смены IP.")
            except Exception as e:
                await status_message.edit_text(f"❌ Ошибка при смене IP: {str(e)}\n\n"
                                            f"Текущий IP: {original_ip}")
        except requests.RequestException as e:
            await status_message.edit_text(f"❌ Ошибка при получении IP: {str(e)}")
            
    except Exception as e:
        await status_message.edit_text(f"❌ Ошибка при проверке прокси: {str(e)}")

@router.callback_query(F.data == "proxy:check")
async def cb_proxy_check(cq: CallbackQuery):
    await verify_proxy(cq.from_user.id, cq)
    await cq.answer()

@router.callback_query(F.data == "proxy:verify_now")
async def cb_proxy_verify_now(cq: CallbackQuery):
    await verify_proxy(cq.from_user.id, cq)
    await cq.answer()

@router.callback_query(F.data == "proxy:verify_later")
async def cb_proxy_verify_later(cq: CallbackQuery):
    # Просто показываем настройки прокси
    proxy = DB.get_setting(cq.from_user.id, "proxy") or "Не настроено"
    proxy_change_url = DB.get_setting(cq.from_user.id, "proxy_change_url") or "Не настроено"
    verified = "Да" if DB.get_setting(cq.from_user.id, "proxy_verified") == "1" else "Нет"
    
    proxy_text = (
        "<b>Настройки прокси</b>\n\n"
        f"<b>Прокси в формате username:password@server:port:</b>\n{proxy}\n\n"
        f"<b>Ссылка для изменения IP, в формате https://changeip.mobileproxy.space/?proxy_key=***:</b>\n{proxy_change_url}\n\n"
        f"<b>Проверен:</b> {verified}"
    )
    
    await cq.message.edit_text(proxy_text, reply_markup=kb_proxy(), parse_mode="HTML")
    await cq.answer()

@router.callback_query(F.data == "proxy:del")
async def cb_proxy_del(cq: CallbackQuery, state: FSMContext):
    user_id = cq.from_user.id
    proxy = DB.get_setting(user_id, "proxy") or "Не настроено"
    proxy_change_url = DB.get_setting(user_id, "proxy_change_url") or "Не настроено"
    
    if proxy == "Не настроено" and proxy_change_url == "Не настроено":
        await cq.answer("Прокси не настроены, нечего удалять", show_alert=True)
        return
    
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
        DB.delete_setting(user_id, "proxy")
        DB.delete_setting(user_id, "cian_proxy")
        DB.delete_setting(user_id, "proxy_change_url")
        DB.delete_setting(user_id, "proxy_verified")
        
        for job_id, job in ACTIVE.items():
            if job.user_id == user_id:
                job.settings["proxy"] = None
                job.settings["proxy_change_url"] = None
        
        await cq.message.edit_text("Все настройки прокси удалены. Активные поиски теперь работают без прокси.")
        await asyncio.sleep(2)
        
        await cq.message.edit_text(f"Здравствуйте, {cq.from_user.first_name}! На что желаете поохотиться сегодня?", reply_markup=kb_main())
    else:
        proxy = DB.get_setting(user_id, "proxy") or "Не настроено"
        proxy_change_url = DB.get_setting(user_id, "proxy_change_url") or "Не настроено"
        verified = "Да" if DB.get_setting(user_id, "proxy_verified") == "1" else "Нет"
        
        proxy_text = (
            "<b>Настройки прокси</b>\n\n"
            f"<b>Прокси в формате username:password@server:port:</b>\n{proxy}\n\n"
            f"<b>Ссылка для изменения IP, в формате https://changeip.mobileproxy.space/?proxy_key=***:</b>\n{proxy_change_url}\n\n"
            f"<b>Проверен:</b> {verified}"
        )
        
        await cq.message.edit_text(proxy_text, reply_markup=kb_proxy(), parse_mode="HTML")
    
    await state.clear()
    await cq.answer()

@router.message(StateFilter(SearchStates.waiting_for_stop_id))
async def handle_stop_search(message: Message, state: FSMContext):
    try:
        search_id = int(message.text.strip())
    except ValueError:
        await message.reply("Необходимо указать числовой ID поиска.")
        await state.clear()
        await bot.send_message(message.chat.id, f"Здравствуйте, {message.from_user.first_name}! На что желаете поохотиться сегодня?", reply_markup=kb_main())
        return
    
    rows = DB.list_active_searches(message.from_user.id)
    search_exists = any(row[0] == search_id for row in rows)
    
    if not search_exists:
        await message.reply(f"Поиск #{search_id} не найден или не принадлежит вам.")
        await state.clear()
        await bot.send_message(message.chat.id, f"Здравствуйте, {message.from_user.first_name}! На что желаете поохотиться сегодня?", reply_markup=kb_main())
        return
    
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
    
    try:
        sid = int(sid_str)
        
        if sid in ACTIVE:
            job = ACTIVE[sid]
            job.stop_event.set()
            
            stats_text = ""
            try:
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

async def run_avito(job: SearchJob):
    s = job.settings
    parser = AvitoParse(
        url=job.urls,
        count=s["pages"],
        proxy=s["proxy"],
        proxy_change_url=s.get("proxy_change_url"),  # Добавляем URL смены IP
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
    
    job.parser = parser
    
    await asyncio.get_running_loop().run_in_executor(None, parser.parse)
    
    if not job.first_run:
        stats = parser.get_statistics()
        job.total_new_ads += stats.get('total_new_ads', 0)
        job.total_notified_ads += stats.get('total_notified_ads', 0)
        logger.info(f"Поиск #{job.sid}: обновлена статистика. Всего найдено: {job.total_new_ads}, отправлено: {job.total_notified_ads}")

async def run_cian(job: SearchJob):
    s = job.settings
    parser = CianParse(
        url=job.urls,
        count=s.get("pages", 5),
        proxy=s.get("proxy"),
        proxy_change_url=s.get("proxy_change_url"),  # Передаем URL смены IP
        min_price=s.get("min_price", 0),
        max_price=s.get("max_price", 9_999_999),
        keysword_list=s.get("keywords", []),
        keysword_black_list=s.get("blacklist", []),
        pause=s.get("pause", 300),
        stop_event=job.stop_event,
        tg_token=TOKEN,
        chat_id=job.user_id,
        job_name=f"#{job.sid}" if not job.name else f"#{job.sid}-{job.name}",
        first_run=job.first_run
    )
    
    job.parser = parser
    
    await asyncio.get_running_loop().run_in_executor(None, parser.parse)
    
    if not job.first_run:
        stats = parser.get_statistics()
        job.total_new_ads += stats.get('total_new_ads', 0)
        job.total_notified_ads += stats.get('total_notified_ads', 0)
        logger.info(f"Поиск ЦИАН #{job.sid}: обновлена статистика. Всего найдено: {job.total_new_ads}, отправлено: {job.total_notified_ads}")

async def _restore():
    for row in DB.list_active_searches():
        sid = row[0]
        urls = row[1].split()
        st = json.loads(row[2])
        
        platform = st.get('platform', 'avito')
        
        name = row[3] if len(row) > 3 else ""
        
        ev = asyncio.Event()
        ACTIVE[sid] = SearchJob(sid, 0, platform, urls, st, ev, first_run=False, name=name)
        
        if platform == "cian":
            scheduler.add_job(run_cian, "interval", seconds=st.get("pause", 300), args=[ACTIVE[sid]], id=str(sid))
        else:
            scheduler.add_job(run_avito, "interval", seconds=st.get("pause", 120), args=[ACTIVE[sid]], id=str(sid))

async def main():
    DB.clean_scan_history()
    DB.clean_cian_scan_history()
    DB.clean_cian_viewed()
    DB.clean_active_searches()
    
    success = DB.reset_search_counter()
    if success:
        logger.info("Счетчик поисков успешно сброшен до 1")
    else:
        logger.warning("Не удалось сбросить счетчик поисков")
    
    scheduler.start()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())