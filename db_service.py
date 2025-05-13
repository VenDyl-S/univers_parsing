import json
import sqlite3
import os
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Tuple, Optional

DB_FILE = Path(__file__).parent / "database.db"


class SQLiteDBHandler:
    _instance = None
    _lock: Lock = Lock()

    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, db_path: Path | str = DB_FILE) -> None:
        if getattr(self, "_initialized", False):
            return
        self.db_path = str(db_path)
        self._create_tables()
        self._migrate_database()
        self._initialized = True

    def _create_tables(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            c = conn.cursor()
            # Основные таблицы Авито
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS viewed (
                    id    INTEGER,
                    price INTEGER,
                    PRIMARY KEY (id, price)
                )
                """
            )
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS settings (
                    user_id INTEGER,
                    key     TEXT,
                    value   TEXT,
                    PRIMARY KEY (user_id, key)
                )
                """
            )
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS searches (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id       INTEGER,
                    platform      TEXT,
                    urls          TEXT,
                    settings_json TEXT,
                    active        INTEGER DEFAULT 1,
                    name          TEXT
                )
                """
            )
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS scan_history (
                    url       TEXT,
                    ad_ids    TEXT,
                    timestamp INTEGER DEFAULT (strftime('%s', 'now')),
                    PRIMARY KEY (url)
                )
                """
            )
            
            # Таблицы для ЦИАН
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS cian_viewed (
                    id           TEXT,
                    price        INTEGER,
                    url          TEXT,
                    title        TEXT,
                    timestamp    INTEGER DEFAULT (strftime('%s', 'now')),
                    PRIMARY KEY (id, price)
                )
                """
            )
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS cian_scan_history (
                    url       TEXT,
                    ad_ids    TEXT,
                    timestamp INTEGER DEFAULT (strftime('%s', 'now')),
                    PRIMARY KEY (url)
                )
                """
            )
            conn.commit()
    
    def _migrate_database(self) -> None:
        """Выполняет миграцию базы данных, добавляя новые столбцы при необходимости."""
        with sqlite3.connect(self.db_path) as conn:
            c = conn.cursor()
            
            c.execute("PRAGMA table_info(searches)")
            columns = [column[1] for column in c.fetchall()]
            
            if "name" not in columns:
                c.execute("ALTER TABLE searches ADD COLUMN name TEXT")
                conn.commit()
            
            c.execute("SELECT id, settings_json FROM searches WHERE active=1")
            rows = c.fetchall()
            for row in rows:
                search_id, settings_json = row
                if settings_json:
                    try:
                        settings = json.loads(settings_json)
                        if "platform" not in settings:
                            settings["platform"] = "avito"
                            c.execute(
                                "UPDATE searches SET settings_json=? WHERE id=?",
                                (json.dumps(settings, ensure_ascii=False), search_id)
                            )
                    except (json.JSONDecodeError, TypeError):
                        pass
            conn.commit()

    def add_record(self, record_id: int, price: int) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("INSERT OR IGNORE INTO viewed(id, price) VALUES (?, ?)", (record_id, price))
            conn.commit()

    def record_exists(self, record_id: int, price: int) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute("SELECT 1 FROM viewed WHERE id=? AND price=?", (record_id, price))
            return cur.fetchone() is not None

    def list_all_viewed_records(self) -> List[Tuple]:
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute("SELECT id, price FROM viewed")
            return cur.fetchall()

    def get_setting(self, user_id: int, key: str) -> Optional[str]:
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute("SELECT value FROM settings WHERE user_id=? AND key=?", (user_id, key))
            row = cur.fetchone()
            return row[0] if row else None

    def set_setting(self, user_id: int, key: str, value: str) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO settings(user_id, key, value) VALUES(?, ?, ?) "
                "ON CONFLICT(user_id, key) DO UPDATE SET value=excluded.value",
                (user_id, key, value),
            )
            conn.commit()

    def delete_setting(self, user_id: int, key: str) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM settings WHERE user_id=? AND key=?", (user_id, key))
            conn.commit()

    def list_settings(self, user_id: int) -> Dict[str, str]:
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute("SELECT key, value FROM settings WHERE user_id=?", (user_id,))
            return {k: v for k, v in cur.fetchall()}

    def get_scan_ids(self, url: str) -> List[str]:
        """Получить список ID объявлений для заданного URL из последнего сканирования"""
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute("SELECT ad_ids FROM scan_history WHERE url=?", (url,))
            result = cur.fetchone()
            if result and result[0]:
                return json.loads(result[0])
            return []
    
    def save_scan_ids(self, url: str, ids: List[str]) -> None:
        """Сохранить список ID объявлений для URL"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO scan_history(url, ad_ids) VALUES(?, ?)",
                (url, json.dumps(ids))
            )
            conn.commit()
    
    def clean_scan_history(self) -> None:
        """Очистить историю сканирований"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM scan_history")
            conn.commit()

    def add_cian_record(self, ad_id: str, price: int, url: str = "", title: str = "") -> None:
        """Добавляет запись об объявлении ЦИАН в базу"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT OR IGNORE INTO cian_viewed(id, price, url, title) VALUES (?, ?, ?, ?)", 
                (ad_id, price, url, title)
            )
            conn.commit()

    def cian_record_exists(self, ad_id: str, price: int) -> bool:
        """Проверяет существование объявления ЦИАН в базе"""
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute("SELECT 1 FROM cian_viewed WHERE id=? AND price=?", (ad_id, price))
            return cur.fetchone() is not None

    def list_all_cian_records(self) -> List[Tuple]:
        """Возвращает список всех сохраненных объявлений ЦИАН"""
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute("SELECT id, price, url, title FROM cian_viewed")
            return cur.fetchall()

    def clean_cian_viewed(self) -> None:
        """Очищает таблицу просмотренных объявлений ЦИАН"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM cian_viewed")
            conn.commit()

    def get_cian_scan_ids(self, url: str) -> List[str]:
        """Получить список ID объявлений ЦИАН для заданного URL из последнего сканирования"""
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute("SELECT ad_ids FROM cian_scan_history WHERE url=?", (url,))
            result = cur.fetchone()
            if result and result[0]:
                return json.loads(result[0])
            return []
    
    def save_cian_scan_ids(self, url: str, ids: List[str]) -> None:
        """Сохранить список ID объявлений ЦИАН для URL"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO cian_scan_history(url, ad_ids) VALUES(?, ?)",
                (url, json.dumps(ids))
            )
            conn.commit()
    
    def clean_cian_scan_history(self) -> None:
        """Очистить историю сканирований ЦИАН"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM cian_scan_history")
            conn.commit()

    def add_search(self, user_id: int, platform: str, urls: List[str], settings: Dict[str, Any], name: str = "") -> int:
        settings_copy = settings.copy()
        settings_copy["platform"] = platform
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            all_ids = cursor.execute("SELECT id FROM searches").fetchall()
            all_ids = [row[0] for row in all_ids]
            
            new_id = 1
            while new_id in all_ids:
                new_id += 1
                
            cursor.execute(
                "INSERT INTO searches(id, user_id, platform, urls, settings_json, name, active) VALUES (?, ?, ?, ?, ?, ?, 1)",
                (new_id, user_id, platform, " ".join(urls), json.dumps(settings_copy, ensure_ascii=False), name),
            )
            
            conn.commit()
            return new_id

    def deactivate_search(self, search_id: int) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("UPDATE searches SET active=0 WHERE id=?", (search_id,))
            conn.commit()

    def list_active_searches(self, user_id: int | None = None, platform: str | None = None) -> List[Tuple]:
        sql = "SELECT id, urls, settings_json"
        
        with sqlite3.connect(self.db_path) as conn:
            c = conn.cursor()
            c.execute("PRAGMA table_info(searches)")
            columns = [column[1] for column in c.fetchall()]
            
            if "name" in columns:
                sql += ", name"
            else:
                sql += ", ''"
        
        sql += " FROM searches WHERE active=1"
        
        params: List[Any] = []
        if user_id is not None and user_id != 0:
            sql += " AND user_id=?"
            params.append(user_id)
        if platform:
            sql += " AND platform=?"
            params.append(platform)
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute(sql, tuple(params))
            return cur.fetchall()
    
    def reset_search_counter(self) -> bool:
        """Сбрасывает автоинкрементный счетчик для таблицы searches.
        
        Использует более радикальный подход - пересоздание таблицы searches.
        """
        try:
            # Шаг 1: Создаем новое соединение с базой данных для этой операции
            # и устанавливаем режим внешних ключей OFF для безопасности
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("PRAGMA foreign_keys = OFF")
                
                # Шаг 2: Сохраняем данные из таблицы searches в память
                cursor = conn.cursor()
                cursor.execute("SELECT user_id, platform, urls, settings_json, active, name FROM searches WHERE active=1")
                active_searches = cursor.fetchall()
                
                # Шаг 3: Удаляем таблицу searches
                conn.execute("DROP TABLE IF EXISTS searches")
                
                # Шаг 4: Создаем таблицу searches заново
                conn.execute("""
                    CREATE TABLE searches (
                        id            INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id       INTEGER,
                        platform      TEXT,
                        urls          TEXT,
                        settings_json TEXT,
                        active        INTEGER DEFAULT 1,
                        name          TEXT
                    )
                """)
                
                # Шаг 5: Восстанавливаем активные поиски (если необходимо)
                if active_searches:
                    conn.executemany(
                        "INSERT INTO searches(user_id, platform, urls, settings_json, active, name) VALUES (?, ?, ?, ?, ?, ?)",
                        active_searches
                    )
                
                # Шаг 6: Включаем обратно режим внешних ключей
                conn.execute("PRAGMA foreign_keys = ON")
                conn.commit()
                
                import logging
                logging.info("Таблица searches пересоздана, счетчик поисков сброшен до 1")
                return True
        except Exception as e:
            import logging
            logging.error(f"Ошибка при сбросе счетчика поисков: {e}")
        return False
    
    def clean_active_searches(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("UPDATE searches SET active=0")
            conn.commit()
            
    def clear_viewed_records(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM viewed")
            conn.commit()