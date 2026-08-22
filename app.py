import os
import io
import re
import time
import math
import random
import logging
import asyncio
import html
import hashlib
import json
import urllib.parse
import struct
import zlib
import xml.etree.ElementTree as ET
from html.parser import HTMLParser
from difflib import SequenceMatcher
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional, Tuple

import pytz
import aiohttp
from dotenv import load_dotenv
from cryptography.fernet import Fernet, InvalidToken

from aiogram import Bot, Dispatcher, Router, F, BaseMiddleware
from aiogram.filters import Command, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message, 
    CallbackQuery, 
    TelegramObject,
    ReplyKeyboardMarkup, 
    KeyboardButton, 
    InlineKeyboardMarkup, 
    InlineKeyboardButton,
    BufferedInputFile
)

# بارگذاری متغیرهای محیطی در صورت وجود فایل .env
load_dotenv()

# ============================================================
# بخش تنظیمات و متغیرهای سراسری (Configuration)
# ============================================================
API_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
BOT_USERNAME = os.getenv("BOT_USERNAME", "TechNowAibot")
BUILD_VERSION = "9.0.0-hybrid-scout-gemini-diagnostics-final"
DEFAULT_MAX_WORKERS = 3
DEFAULT_MAX_AI_WORKERS = 3
AI_VERIFY_ENABLED_DEFAULT = os.getenv("AI_VERIFY_ENABLED", "auto").lower()
AI_PROVIDER_RECHECK_MINUTES = int(os.getenv("AI_PROVIDER_RECHECK_MINUTES", "10"))
BOT_USERNAME_RUNTIME = ""

# تنظیمات اتصال به Cloudflare D1 REST API
CF_ACCOUNT_ID = os.getenv("CF_ACCOUNT_ID")
CF_DATABASE_ID = os.getenv("CF_DATABASE_ID")
CF_API_TOKEN = os.getenv("CF_API_TOKEN")

# تنظیمات مربوط به هوش مصنوعی
AI_API_KEY = os.getenv("AI_API_KEY")
AI_API_URL = os.getenv("AI_API_URL", "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions")
AI_MODEL_NAME = os.getenv("AI_MODEL_NAME", "gemini-1.5-flash")

# تنظیمات اتوماسیون محتوای هوشمند
CHANNEL_ID = os.getenv("CHANNEL_ID", "")
AUTOMATION_ENABLED_DEFAULT = os.getenv("AUTOMATION_ENABLED", "true").lower() in {"1", "true", "yes", "on"}
DEFAULT_SOURCE_INTERVAL_MINUTES = int(os.getenv("DEFAULT_SOURCE_INTERVAL_MINUTES", "15"))
DEFAULT_MAX_DAILY_POSTS = int(os.getenv("MAX_DAILY_POSTS", "6"))
DEFAULT_MIN_CONTENT_SCORE = float(os.getenv("MIN_CONTENT_SCORE", "75"))
DEFAULT_MIN_HOURS_BETWEEN_POSTS = float(os.getenv("MIN_HOURS_BETWEEN_POSTS", "2"))
DEFAULT_PUBLISH_START_HOUR = int(os.getenv("PUBLISH_START_HOUR", "8"))
DEFAULT_PUBLISH_END_HOUR = int(os.getenv("PUBLISH_END_HOUR", "23"))
CONTENT_RETENTION_DAYS = int(os.getenv("CONTENT_RETENTION_DAYS", "30"))
LOG_RETENTION_DAYS = int(os.getenv("LOG_RETENTION_DAYS", "14"))
AI_PROVIDER_ENCRYPTION_KEY = os.getenv("AI_PROVIDER_ENCRYPTION_KEY", "")
HTTP_USER_AGENT = os.getenv("HTTP_USER_AGENT", "TechNowAI/2.0 (+content automation)")
HTTP_TIMEOUT_SECONDS = int(os.getenv("HTTP_TIMEOUT_SECONDS", "20"))
MAX_HTTP_BYTES = int(os.getenv("MAX_HTTP_BYTES", "1500000"))
MAX_SOURCE_ITEMS_PER_CYCLE = int(os.getenv("MAX_SOURCE_ITEMS_PER_CYCLE", "5"))

# تنظیم لاگر برای خطایابی بهتر
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)
HTTP_SESSION: Optional[aiohttp.ClientSession] = None
SETTINGS_CACHE: Dict[str, Tuple[str, float]] = {}
SETTINGS_CACHE_TTL = 20.0

async def get_http_session() -> aiohttp.ClientSession:
    global HTTP_SESSION
    if HTTP_SESSION is None or HTTP_SESSION.closed:
        HTTP_SESSION = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SECONDS))
    return HTTP_SESSION

async def close_http_session():
    global HTTP_SESSION
    if HTTP_SESSION and not HTTP_SESSION.closed:
        await HTTP_SESSION.close()
    HTTP_SESSION = None

# ============================================================
# کلاس ارتباط با دیتابیس Cloudflare D1 REST API
# ============================================================
class D1Database:
    def __init__(self, account_id: str, database_id: str, api_token: str):
        self.account_id = account_id
        self.database_id = database_id
        self.api_token = api_token
        self.url = f"https://api.cloudflare.com/client/v4/accounts/{account_id}/d1/database/{database_id}/query"
        self.headers = {
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json"
        }
        self.session: Optional[aiohttp.ClientSession] = None

    async def start(self):
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SECONDS)
            self.session = aiohttp.ClientSession(timeout=timeout)

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
        self.session = None

    async def execute(self, sql: str, params: List[Any] = None) -> List[Dict[str, Any]]:
        payload = {"sql": sql}
        if params:
            payload["params"] = params

        session = self.session
        temporary_session = False
        if session is None or session.closed:
            session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SECONDS))
            temporary_session = True
        try:
                async with session.post(self.url, headers=self.headers, json=payload) as resp:
                    if resp.status != 200:
                        text = await resp.text()
                        logger.error(f"D1 API Error (status {resp.status}): {text}")
                        raise Exception(f"Cloudflare D1 API returned status {resp.status}: {text}")
                    
                    data = await resp.json()
                    if not data.get("success"):
                        errors = data.get("errors", [])
                        logger.error(f"D1 Query failed: {errors}")
                        raise Exception(f"D1 Query failed: {errors}")
                    
                    result = data.get("result", [])
                    if isinstance(result, list) and len(result) > 0:
                        return result[0].get("results", [])
                    elif isinstance(result, dict):
                        return result.get("results", [])
                    return []
        except Exception as e:
            logger.error(f"Error executing SQL: {sql} with params {params}. Error: {e}")
            raise e
        finally:
            if temporary_session:
                await session.close()

    async def execute_batch(self, queries: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
        session = self.session
        temporary_session = False
        if session is None or session.closed:
            session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SECONDS))
            temporary_session = True
        output = []
        try:
            for query in queries:
                    payload = {"sql": query["sql"]}
                    if query.get("params"):
                        payload["params"] = query["params"]

                    async with session.post(self.url, headers=self.headers, json=payload) as resp:
                        if resp.status != 200:
                            text = await resp.text()
                            logger.error(f"D1 Batch API Error (status {resp.status}): {text}")
                            raise Exception(f"Cloudflare D1 Batch API returned status {resp.status}: {text}")
                        
                        data = await resp.json()
                        if not data.get("success"):
                            errors = data.get("errors", [])
                            logger.error(f"D1 Batch Query failed: {errors}")
                            raise Exception(f"D1 Batch Query failed: {errors}")
                        
                        result = data.get("result", [])
                        if isinstance(result, list) and len(result) > 0:
                            output.append(result[0].get("results", []))
                        elif isinstance(result, dict):
                            output.append(result.get("results", []))
                        else:
                            output.append([])
            return output
        except Exception as e:
            logger.error(f"Error executing batch queries. Error: {e}")
            raise e
        finally:
            if temporary_session:
                await session.close()


async def initialize_database(db: D1Database):
    queries = [
        {"sql": "CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY, joined_at TEXT, role TEXT DEFAULT 'user', tokens_used INTEGER DEFAULT 0, last_reset_date TEXT)"},
        {"sql": "CREATE TABLE IF NOT EXISTS posts(id INTEGER PRIMARY KEY AUTOINCREMENT, text TEXT, file_id TEXT, media_type TEXT, likes INTEGER DEFAULT 0, dislikes INTEGER DEFAULT 0, views INTEGER DEFAULT 0, deleted INTEGER DEFAULT 0)"},
        {"sql": "CREATE INDEX IF NOT EXISTS idx_posts_deleted ON posts(deleted)"},
        {"sql": "CREATE INDEX IF NOT EXISTS idx_posts_text ON posts(text)"},
        {"sql": "CREATE TABLE IF NOT EXISTS saves(user INTEGER, post INTEGER, folder TEXT, PRIMARY KEY(user, post))"},
        {"sql": "CREATE TABLE IF NOT EXISTS votes(user_id INTEGER, post_id INTEGER, vote_type TEXT, PRIMARY KEY(user_id, post_id))"},
        {"sql": "CREATE TABLE IF NOT EXISTS user_states(user_id INTEGER PRIMARY KEY, state TEXT, data TEXT)"},
        {"sql": "CREATE TABLE IF NOT EXISTS processed_updates(update_id INTEGER PRIMARY KEY, processed_at TEXT)"}
    ]
    await db.execute_batch(queries)
    try:
        await db.execute("ALTER TABLE posts ADD COLUMN views INTEGER DEFAULT 0")
    except Exception:
        pass
    try:
        await db.execute("ALTER TABLE users ADD COLUMN tokens_used INTEGER DEFAULT 0")
    except Exception:
        pass
    try:
        await db.execute("ALTER TABLE users ADD COLUMN last_reset_date TEXT")
    except Exception:
        pass



# ============================================================
# زیرسیستم اتوماسیون محتوا (Content Automation)
# ============================================================

async def initialize_automation_database(db: D1Database):
    queries = [
        {"sql": "CREATE TABLE IF NOT EXISTS sources(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, url TEXT UNIQUE, feed_url TEXT, category TEXT DEFAULT 'tech', enabled INTEGER DEFAULT 1, interval_minutes INTEGER DEFAULT 15, priority INTEGER DEFAULT 5, last_checked_at TEXT, next_check_at TEXT, last_error TEXT, trust_score REAL DEFAULT 80, created_at TEXT)"},
        {"sql": "CREATE INDEX IF NOT EXISTS idx_sources_due ON sources(enabled, next_check_at)"},
        {"sql": "CREATE TABLE IF NOT EXISTS source_items(id INTEGER PRIMARY KEY AUTOINCREMENT, source_id INTEGER NOT NULL, canonical_url TEXT NOT NULL, title TEXT, description TEXT, content TEXT, image_url TEXT, published_at TEXT, discovered_at TEXT, content_hash TEXT, status TEXT DEFAULT 'new', score REAL DEFAULT 0, category TEXT, article_id INTEGER, last_error TEXT, UNIQUE(source_id, canonical_url))"},
        {"sql": "CREATE INDEX IF NOT EXISTS idx_source_items_status ON source_items(status)"},
        {"sql": "CREATE INDEX IF NOT EXISTS idx_source_items_hash ON source_items(content_hash)"},
        {"sql": "CREATE TABLE IF NOT EXISTS articles(id INTEGER PRIMARY KEY AUTOINCREMENT, source_item_id INTEGER UNIQUE, title TEXT, channel_text TEXT, body TEXT, source_url TEXT, image_url TEXT, category TEXT, score REAL, status TEXT DEFAULT 'ready', deep_token TEXT UNIQUE, created_at TEXT, verified_at TEXT, published_message_id INTEGER)"},
        {"sql": "CREATE TABLE IF NOT EXISTS publication_queue(id INTEGER PRIMARY KEY AUTOINCREMENT, article_id INTEGER UNIQUE, scheduled_at TEXT, status TEXT DEFAULT 'queued', attempts INTEGER DEFAULT 0, last_error TEXT, created_at TEXT, published_at TEXT)"},
        {"sql": "CREATE INDEX IF NOT EXISTS idx_publication_queue_due ON publication_queue(status, scheduled_at)"},
        {"sql": "CREATE TABLE IF NOT EXISTS ai_providers(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE, base_url TEXT, encrypted_api_key TEXT, model_name TEXT, priority INTEGER DEFAULT 10, enabled INTEGER DEFAULT 1, created_at TEXT, updated_at TEXT, status TEXT DEFAULT 'unknown', last_error TEXT, cooldown_until TEXT, last_checked_at TEXT, last_latency_ms INTEGER DEFAULT 0, consecutive_failures INTEGER DEFAULT 0)"},
        {"sql": "CREATE TABLE IF NOT EXISTS automation_settings(key TEXT PRIMARY KEY, value TEXT)"},
        {"sql": "CREATE TABLE IF NOT EXISTS automation_logs(id INTEGER PRIMARY KEY AUTOINCREMENT, level TEXT, event TEXT, details TEXT, created_at TEXT)"},
        {"sql": "CREATE INDEX IF NOT EXISTS idx_automation_logs_created ON automation_logs(created_at)"},
        {"sql": "CREATE TABLE IF NOT EXISTS manual_channel_events(id INTEGER PRIMARY KEY AUTOINCREMENT, message_id INTEGER, created_at TEXT)"}
    ]
    await db.execute_batch(queries)
    # مهاجرت امن برای نصب‌های قبلی
    for sql in [
        "ALTER TABLE ai_providers ADD COLUMN status TEXT DEFAULT 'unknown'",
        "ALTER TABLE ai_providers ADD COLUMN last_error TEXT",
        "ALTER TABLE ai_providers ADD COLUMN cooldown_until TEXT",
        "ALTER TABLE ai_providers ADD COLUMN last_checked_at TEXT",
        "ALTER TABLE ai_providers ADD COLUMN last_latency_ms INTEGER DEFAULT 0",
        "ALTER TABLE ai_providers ADD COLUMN consecutive_failures INTEGER DEFAULT 0",
        "ALTER TABLE articles ADD COLUMN published_at TEXT",
    ]:
        try:
            await db.execute(sql)
        except Exception:
            pass
    defaults = {
        "automation_enabled": "1" if AUTOMATION_ENABLED_DEFAULT else "0",
        "max_daily_posts": str(DEFAULT_MAX_DAILY_POSTS),
        "min_content_score": str(DEFAULT_MIN_CONTENT_SCORE),
        "min_hours_between_posts": str(DEFAULT_MIN_HOURS_BETWEEN_POSTS),
        "publish_start_hour": str(DEFAULT_PUBLISH_START_HOUR),
        "publish_end_hour": str(DEFAULT_PUBLISH_END_HOUR),
        "default_source_interval": str(DEFAULT_SOURCE_INTERVAL_MINUTES),
        "ai_verify_mode": "auto",
        "ai_web_scout_enabled": "1",
        "ai_web_scout_max_items": "5",
        "last_cleanup_at": "",
        "last_manual_channel_post_at": "",
        "channel_id": CHANNEL_ID,
        "channel_username": "",
        "max_workers": str(DEFAULT_MAX_WORKERS),
        "max_ai_workers": str(DEFAULT_MAX_AI_WORKERS),
        "ai_verify_mode": AI_VERIFY_ENABLED_DEFAULT,
        "weight_global": "15",
        "weight_technology": "15",
        "weight_ai": "15",
        "weight_cyber": "15",
        "weight_education": "10",
        "weight_iran": "15",
        "weight_freshness": "10",
        "weight_source": "5",
        "weight_novelty": "10",
    }
    for k, v in defaults.items():
        await db.execute("INSERT OR IGNORE INTO automation_settings(key, value) VALUES(?, ?)", [k, v])
    # Provider قدیمی محیطی را از چرخه failover خارج می‌کنیم؛ مدیر فقط مدل‌هایی را که
    # خودش در پنل تست کرده است وارد اتوماسیون می‌کند.
    try:
        await db.execute("UPDATE ai_providers SET enabled=0, status='invalid', last_error='Environment Default disabled by managed-provider mode' WHERE name='Environment Default'")
    except Exception:
        pass


def encrypt_secret(value: str) -> str:
    if not value:
        return ""
    if not AI_PROVIDER_ENCRYPTION_KEY:
        return value
    try:
        return Fernet(AI_PROVIDER_ENCRYPTION_KEY.encode()).encrypt(value.encode()).decode()
    except Exception:
        logger.exception("Failed to encrypt AI provider secret")
        return value


def decrypt_secret(value: str) -> str:
    if not value:
        return ""
    if not AI_PROVIDER_ENCRYPTION_KEY:
        return value
    try:
        return Fernet(AI_PROVIDER_ENCRYPTION_KEY.encode()).decrypt(value.encode()).decode()
    except (InvalidToken, Exception):
        return value


async def get_setting(db: D1Database, key: str, default: str = "") -> str:
    now = time.monotonic()
    cached = SETTINGS_CACHE.get(key)
    if cached and now - cached[1] < SETTINGS_CACHE_TTL:
        return cached[0]
    rows = await db.execute("SELECT value FROM automation_settings WHERE key = ?", [key])
    value = str(rows[0].get("value")) if rows else default
    SETTINGS_CACHE[key] = (value, now)
    return value


async def set_setting(db: D1Database, key: str, value: str):
    await db.execute("INSERT OR REPLACE INTO automation_settings(key, value) VALUES(?, ?)", [key, value])
    SETTINGS_CACHE[key] = (str(value), time.monotonic())


async def get_channel_id(db: D1Database) -> str:
    return (await get_setting(db, "channel_id", CHANNEL_ID)).strip()


async def log_automation(db: D1Database, level: str, event: str, details: str = ""):
    try:
        if len(details) > 2000:
            details = details[:2000]
        await db.execute("INSERT INTO automation_logs(level, event, details, created_at) VALUES(?, ?, ?, ?)", [
            level, event, details, datetime.now(timezone.utc).isoformat()
        ])
    except Exception:
        logger.exception("automation log failed")


async def cleanup_automation_data(db: D1Database):
    now = datetime.now(timezone.utc)
    cutoff_content = (now - timedelta(days=CONTENT_RETENTION_DAYS)).isoformat()
    cutoff_logs = (now - timedelta(days=LOG_RETENTION_DAYS)).isoformat()
    await db.execute("DELETE FROM automation_logs WHERE created_at < ?", [cutoff_logs])
    await db.execute("DELETE FROM source_items WHERE status IN ('rejected','error') AND discovered_at < ?", [cutoff_content])
    await db.execute("DELETE FROM publication_queue WHERE status IN ('published','failed') AND created_at < ?", [cutoff_content])
    await set_setting(db, "last_cleanup_at", now.isoformat())


def normalize_url(url: str) -> str:
    url = (url or "").strip()
    if not url:
        return ""
    parsed = urllib.parse.urlsplit(url if "://" in url else "https://" + url)
    query = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    query = [(k, v) for k, v in query if not k.lower().startswith(("utm_", "fbclid", "gclid"))]
    return urllib.parse.urlunsplit((parsed.scheme.lower(), parsed.netloc.lower(), parsed.path.rstrip('/') or '/', urllib.parse.urlencode(query), ""))


def same_domain(a: str, b: str) -> bool:
    try:
        return urllib.parse.urlsplit(a).netloc.lower().removeprefix('www.') == urllib.parse.urlsplit(b).netloc.lower().removeprefix('www.')
    except Exception:
        return False


def text_hash(text: str) -> str:
    return hashlib.sha256(re.sub(r"\s+", " ", text or "").strip().lower().encode("utf-8", errors="ignore")).hexdigest()


def strip_html_text(value: str) -> str:
    if not value:
        return ""
    value = re.sub(r"<script.*?</script>|<style.*?</style>|<noscript.*?</noscript>", " ", value, flags=re.I | re.S)
    value = re.sub(r"<[^>]+>", " ", value)
    return re.sub(r"\s+", " ", html.unescape(value)).strip()


class SimpleHTMLParser(HTMLParser):
    """سبک و بدون وابستگی برای استخراج داده‌های مقاله و لینک‌ها."""
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.title = ""
        self.meta = {}
        self.links = []
        self._title_depth = 0
        self._current_link = ""
        self._current_link_text = []
        self._article_depth = 0
        self._paragraph_depth = 0
        self._body_parts = []
        self._skip_depth = 0

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag in {"script", "style", "noscript", "svg"}:
            self._skip_depth += 1
            return
        if self._skip_depth:
            return
        if tag == "title":
            self._title_depth += 1
        if tag == "meta":
            key = attrs.get("property") or attrs.get("name") or attrs.get("itemprop")
            content = attrs.get("content")
            if key and content:
                self.meta[key.lower()] = content.strip()
        if tag == "a":
            href = attrs.get("href") or ""
            self._current_link = href
            self._current_link_text = []
        if tag == "article":
            self._article_depth += 1
        if tag in {"p", "li", "h1", "h2", "h3"}:
            self._paragraph_depth += 1

    def handle_endtag(self, tag):
        if self._skip_depth:
            if tag in {"script", "style", "noscript", "svg"}:
                self._skip_depth -= 1
            return
        if tag == "title" and self._title_depth:
            self._title_depth -= 1
        if tag == "a":
            text = re.sub(r"\s+", " ", " ".join(self._current_link_text)).strip()
            if self._current_link and text:
                self.links.append((self._current_link, text))
            self._current_link = ""
            self._current_link_text = []
        if tag == "article" and self._article_depth:
            self._article_depth -= 1
        if tag in {"p", "li", "h1", "h2", "h3"} and self._paragraph_depth:
            self._paragraph_depth -= 1

    def handle_data(self, data):
        if self._skip_depth:
            return
        data = data.strip()
        if not data:
            return
        if self._title_depth:
            self.title += " " + data
        if self._current_link:
            self._current_link_text.append(data)
        if self._article_depth or self._paragraph_depth:
            self._body_parts.append(data)

    @property
    def body(self):
        return re.sub(r"\s+", " ", " ".join(self._body_parts)).strip()


async def http_get(url: str, session: aiohttp.ClientSession) -> Tuple[str, str]:
    headers={"User-Agent":HTTP_USER_AGENT,"Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8","Accept-Language":"fa-IR,fa;q=0.9,en-US;q=0.8,en;q=0.7","Cache-Control":"no-cache"}
    timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SECONDS)
    last_error=None
    for attempt in range(3):
        try:
            async with session.get(url,headers=headers,timeout=timeout,allow_redirects=True) as resp:
                if resp.status>=400:
                    text=await resp.text(errors="ignore")
                    if resp.status in {429,500,502,503,504} and attempt<2:
                        await asyncio.sleep(0.5*(attempt+1)); continue
                    raise RuntimeError(f"HTTP {resp.status}" + (f": {text[:180]}" if resp.status in {429,500,502,503,504} else ""))
                data=await resp.content.read(MAX_HTTP_BYTES+1)
                if len(data)>MAX_HTTP_BYTES: raise RuntimeError("response too large")
                ctype=resp.headers.get("Content-Type",""); enc=resp.charset or "utf-8"
                return data.decode(enc,errors="ignore"),ctype
        except (aiohttp.ClientError, asyncio.TimeoutError, ConnectionError) as e:
            last_error=e
            if attempt<2:
                await asyncio.sleep(0.5*(attempt+1)); continue
            raise RuntimeError(str(e))
    raise RuntimeError(str(last_error or "request failed"))


def local_name(tag: str) -> str:
    return tag.rsplit('}', 1)[-1].lower()


def xml_child_text(el, names):
    for child in el.iter():
        if local_name(child.tag) in names and child is not el:
            txt = "".join(child.itertext()).strip()
            if txt:
                return html.unescape(txt)
    return ""


def parse_feed(text: str, base_url: str) -> List[Dict[str, Any]]:
    try:
        root = ET.fromstring(text)
    except Exception:
        return []
    items = []
    for el in root.iter():
        if local_name(el.tag) not in {"item", "entry"}:
            continue
        title = xml_child_text(el, {"title"})
        link = ""
        for child in el.iter():
            if local_name(child.tag) == "link":
                href = child.attrib.get("href")
                if href:
                    link = href
                    break
                txt = "".join(child.itertext()).strip()
                if txt:
                    link = txt
                    break
        desc = xml_child_text(el, {"description", "summary", "content"})
        pub = xml_child_text(el, {"pubdate", "published", "updated", "date"})
        image = ""
        for child in el.iter():
            if local_name(child.tag) in {"thumbnail", "content", "image", "enclosure"}:
                u = child.attrib.get("url") or child.attrib.get("href")
                if u:
                    image = u
                    break
        if not link or not title:
            continue
        items.append({"title": strip_html_text(title), "url": urllib.parse.urljoin(base_url, link), "description": strip_html_text(desc), "published_at": pub, "image_url": urllib.parse.urljoin(base_url, image) if image else ""})
    return items


def extract_html_page(html_text: str, url: str) -> Dict[str, Any]:
    p = SimpleHTMLParser()
    p.feed(html_text)
    canonical = p.meta.get("og:url") or p.meta.get("twitter:url") or url
    title = p.meta.get("og:title") or p.meta.get("twitter:title") or p.title
    image = p.meta.get("og:image") or p.meta.get("twitter:image") or ""
    desc = p.meta.get("og:description") or p.meta.get("description") or p.meta.get("twitter:description") or ""
    body = p.body
    links = []
    for href, text in p.links:
        full = normalize_url(urllib.parse.urljoin(url, href))
        if full and same_domain(full, url) and text and len(text) >= 12:
            links.append((full, text[:300]))
    dedup = []
    seen = set()
    for item in links:
        if item[0] not in seen:
            seen.add(item[0]); dedup.append(item)
    return {"canonical_url": normalize_url(canonical), "title": strip_html_text(title), "description": strip_html_text(desc), "body": body, "image_url": urllib.parse.urljoin(url, image) if image else "", "links": dedup}


def article_candidates_from_html(parsed: Dict[str, Any], source_url: str) -> List[Dict[str, Any]]:
    path = urllib.parse.urlsplit(source_url).path.rstrip('/')
    # صفحه ریشه سایت معمولاً صفحه مقاله نیست؛ از آن فقط لینک‌های داخلی را استخراج می‌کنیم.
    if path and len(parsed.get("body", "")) > 700 and parsed.get("title"):
        return [{"title": parsed["title"][:300], "url": parsed["canonical_url"] or source_url, "description": parsed["description"][:1000], "body": parsed["body"][:12000], "image_url": parsed.get("image_url", ""), "published_at": ""}]
    out = []
    for url, title in parsed.get("links", [])[:MAX_SOURCE_ITEMS_PER_CYCLE]:
        if len(title) < 15:
            continue
        out.append({"title": title, "url": url, "description": "", "body": "", "image_url": "", "published_at": ""})
    return out


async def discover_source_items(source: Dict[str, Any], return_diagnostics: bool = False) -> Any:
    base = normalize_url(source.get("url", ""))
    session = await get_http_session()
    diagnostics = []
    seen_urls = set()

    def done(items, method):
        result = {"items": items[:MAX_SOURCE_ITEMS_PER_CYCLE], "method": method, "diagnostics": diagnostics}
        return result if return_diagnostics else result["items"]

    if not base:
        raise RuntimeError("آدرس منبع خالی یا نامعتبر است")

    configured = (source.get("feed_url") or "").strip()
    if configured:
        try:
            feed_text, _ = await http_get(configured, session)
            items = parse_feed(feed_text, base)
            if items:
                diagnostics.append("✅ feed سفارشی")
                return done(items, "configured_feed")
            diagnostics.append("⚠️ feed سفارشی آیتمی نداشت")
        except Exception as e:
            diagnostics.append(f"⚠️ feed سفارشی: {e}")

    # Homepage: use alternate feeds if advertised, otherwise extract links.
    try:
        homepage_html, _ = await http_get(base, session)
        parsed = extract_html_page(homepage_html, base)
        alternate_feeds = []
        for m in re.finditer(r'<link\b[^>]*>', homepage_html, flags=re.I):
            tag=m.group(0)
            href=re.search(r'href=["\']([^"\']+)',tag,flags=re.I)
            typ=re.search(r'type=["\']([^"\']+)',tag,flags=re.I)
            rel=re.search(r'rel=["\']([^"\']+)',tag,flags=re.I)
            if href and ((typ and any(x in typ.group(1).lower() for x in ("rss","atom"))) or (rel and "alternate" in rel.group(1).lower())):
                alternate_feeds.append(urllib.parse.urljoin(base,href.group(1)))
        for feed_url in list(dict.fromkeys(alternate_feeds))[:4]:
            try:
                feed_text,_=await http_get(feed_url,session); items=parse_feed(feed_text,feed_url)
                if items:
                    diagnostics.append(f"✅ alternate feed: {feed_url}")
                    return done(items,"alternate_feed")
            except Exception as e:
                diagnostics.append(f"⚠️ alternate feed {feed_url}: {e}")
        html_candidates=article_candidates_from_html(parsed,base)
        if html_candidates:
            diagnostics.append(f"✅ HTML homepage ({len(html_candidates)} candidate)")
            return done(html_candidates,"homepage_html")
        diagnostics.append("⚠️ صفحه اصلی باز شد ولی لینک مقاله قابل استفاده پیدا نشد")
    except Exception as e:
        diagnostics.append(f"⚠️ صفحه اصلی: {e}")

    for feed_path in ["/feed","/rss","/rss.xml","/feed.xml","/atom.xml","/index.xml"]:
        candidate=urllib.parse.urljoin(base+"/",feed_path.lstrip("/"))
        try:
            text,_=await http_get(candidate,session); items=parse_feed(text,candidate)
            if items:
                diagnostics.append(f"✅ common feed: {feed_path}")
                return done(items,"common_feed")
        except Exception as e:
            diagnostics.append(f"⚠️ {feed_path}: {e}")

    wp_url=urllib.parse.urljoin(base+"/","wp-json/wp/v2/posts?per_page=8&_fields=link,title,excerpt,date,jetpack_featured_media_url")
    try:
        text,_=await http_get(wp_url,session); data=json.loads(text)
        if isinstance(data,list) and data:
            items=[]
            for post in data:
                title=strip_html_text(((post.get("title") or {}).get("rendered") or ""))
                url=post.get("link") or ""
                if title and url:
                    items.append({"title":title,"url":url,"description":strip_html_text(((post.get("excerpt") or {}).get("rendered") or "")),"published_at":post.get("date") or "","image_url":post.get("jetpack_featured_media_url") or ""})
            if items:
                diagnostics.append("✅ WordPress REST API")
                return done(items,"wordpress_api")
    except Exception as e:
        diagnostics.append(f"⚠️ WordPress API: {e}")

    sitemap=urllib.parse.urljoin(base+"/","sitemap.xml")
    try:
        sm_text,_=await http_get(sitemap,session)
        try:
            root=ET.fromstring(sm_text)
            locs=[normalize_url(loc.text.strip()) for loc in root.iter() if local_name(loc.tag)=="loc" and loc.text]
            is_index=(local_name(root.tag)=="sitemapindex" or any(local_name(e.tag)=="sitemap" for e in root))
        except Exception as xml_error:
            locs=extract_xml_locs_resilient(sm_text)
            is_index=bool(re.search(r"<sitemap(?:index)?\b",sm_text,re.I))
            diagnostics.append(f"⚠️ XML ناقص بود؛ استخراج مقاوم loc انجام شد: {xml_error}")
        if is_index:
            child_urls=locs[:6]; expanded=[]
            for child in child_urls:
                try:
                    ctext,_=await http_get(child,session)
                    try:
                        cr=ET.fromstring(ctext)
                        expanded.extend(normalize_url(loc.text.strip()) for loc in cr.iter() if local_name(loc.tag)=="loc" and loc.text)
                    except Exception as child_error:
                        fallback_locs=extract_xml_locs_resilient(ctext)
                        expanded.extend(fallback_locs)
                        diagnostics.append(f"⚠️ child sitemap {child}: XML ناقص؛ fallback فعال شد ({child_error})")
                except Exception as e:
                    diagnostics.append(f"⚠️ child sitemap {child}: {e}")
            locs=expanded or locs
        candidate_urls=[]
        for u in locs:
            if u and same_domain(u,base) and u not in seen_urls:
                seen_urls.add(u); candidate_urls.append(u)
            if len(candidate_urls)>=MAX_SOURCE_ITEMS_PER_CYCLE: break
        async def read_one(u):
            try:
                text,_=await http_get(u,session); parsed=extract_html_page(text,u)
                if parsed.get("title"):
                    return {"title":parsed["title"],"url":parsed["canonical_url"] or u,"description":parsed["description"],"body":parsed["body"][:12000],"image_url":parsed["image_url"],"published_at":""}
            except Exception as e:
                diagnostics.append(f"⚠️ sitemap URL {u}: {e}")
            return None
        results=[x for x in await asyncio.gather(*(read_one(u) for u in candidate_urls)) if x]
        if results:
            diagnostics.append(f"✅ sitemap ({len(results)} article)")
            return done(results,"sitemap")
        diagnostics.append("⚠️ sitemap باز شد ولی مقاله قابل استفاده پیدا نشد")
    except Exception as e:
        diagnostics.append(f"⚠️ sitemap: {e}")

    message="؛ ".join(diagnostics[-10:]) or "هیچ روش دریافت محتوا موفق نشد"
    if return_diagnostics:
        return {"items":[],"method":"none","diagnostics":diagnostics,"error":message}
    raise RuntimeError("source fetch failed: "+message)

async def enrich_candidate_content(item: Dict[str, Any]) -> Dict[str, Any]:
    if item.get("body") and len(item["body"]) >= 700:
        return item
    session = await get_http_session()
    try:
        text, _ = await http_get(item["url"], session)
        parsed = extract_html_page(text, item["url"])
        item["title"] = item.get("title") or parsed["title"]
        item["description"] = item.get("description") or parsed["description"]
        item["body"] = parsed["body"][:14000]
        item["image_url"] = item.get("image_url") or parsed["image_url"]
        item["url"] = parsed["canonical_url"] or item["url"]
    except Exception:
        pass
    return item

TOPIC_WORDS = re.compile(r"\b(ai|artificial intelligence|machine learning|llm|gpt|gemini|openai|anthropic|claude|robot|cyber|cybersecurity|hack|hacking|malware|ransomware|phishing|zero-day|zero day|exploit|vulnerability|security|technology|tech|software|chip|nvidia|microsoft|google|apple|meta|startup|model|browser|cloud|linux|python|developer|api|data breach|privacy)\b", re.I)

def heuristic_topic_match(title: str, description: str, category: str) -> bool:
    if category.lower() in {"ai", "tech", "technology", "cyber", "security", "education", "edu"}:
        return True
    return bool(TOPIC_WORDS.search((title or "") + " " + (description or "")))


def recent_semantic_similarity(title: str, recent_titles: List[str]) -> float:
    best = 0.0
    a = (title or "").lower()
    for t in recent_titles:
        b = (t or "").lower()
        best = max(best, SequenceMatcher(None, a, b).ratio())
    return best


def parse_json_object(text: str) -> Dict[str, Any]:
    text = (text or "").strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text, flags=re.I | re.S).strip()
    try:
        obj = json.loads(text)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        m = re.search(r"\{.*\}", text, flags=re.S)
        if m:
            try:
                obj = json.loads(m.group(0))
                return obj if isinstance(obj, dict) else {}
            except Exception:
                return {}
        return {}


class AIProviderManager:
    """AI provider manager with shared HTTP session, cooldown, failover and basic native protocol support."""
    def __init__(self, db: D1Database, bot: Optional[Bot] = None):
        self.db=db
        self.bot=bot
        self._session: Optional[aiohttp.ClientSession]=None
        self._notify_lock=asyncio.Lock()
        self._last_final_notice=0.0

    async def start(self):
        if self._session is None or self._session.closed:
            self._session=aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=45))

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
        self._session=None

    async def providers(self):
        now=datetime.now(timezone.utc).isoformat()
        return await self.db.execute(
            "SELECT * FROM ai_providers WHERE enabled=1 AND (status IS NULL OR status!='invalid' OR cooldown_until IS NULL OR cooldown_until<=?) ORDER BY priority ASC,id ASC",[now]
        )

    @staticmethod
    def protocol(url:str)->str:
        # Detect wire protocol from URL.
        u=(url or "").lower().rstrip("/")
        if "generativelanguage.googleapis.com" in u:
            if "/openai" in u or "chat/completions" in u:
                return "openai"
            return "gemini"
        if "api.anthropic.com" in u and "/chat/completions" not in u:
            return "anthropic"
        return "openai"

    @staticmethod
    def endpoint(url:str, protocol:str, model:str="")->str:
        u=(url or "").strip().rstrip("/")
        if protocol=="gemini":
            if u.endswith(":generateContent"): return u
            if "/models/" in u: return u+":generateContent"
            return u + f"/models/{urllib.parse.quote(model, safe='')}:generateContent"
        if protocol=="anthropic":
            return u if u.endswith("/messages") else u+"/v1/messages" if not u.endswith("/v1") else u+"/messages"
        if u.endswith("/chat/completions"): return u
        if u.endswith("/v1"): return u+"/chat/completions"
        if u.endswith("/openai"): return u+"/chat/completions"
        return u+"/chat/completions"

    @staticmethod
    def google_openai_endpoint(base_url:str)->str:
        u=(base_url or "").strip().rstrip("/")
        if "generativelanguage.googleapis.com" not in u:
            return ""
        if "/openai" in u:
            return u if u.endswith("/chat/completions") else u+"/chat/completions"
        return "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions"

    @staticmethod
    def _extract_content(protocol:str, data:dict)->str:
        if protocol=="anthropic":
            return "".join((b.get("text","") for b in data.get("content",[]) if isinstance(b,dict) and b.get("type")=="text"))
        if protocol=="gemini":
            parts=[]
            for candidate in data.get("candidates") or []:
                content=candidate.get("content") or {}
                for part in content.get("parts") or []:
                    if isinstance(part,dict) and part.get("text"):
                        parts.append(str(part.get("text")))
            return "".join(parts).strip()
        choice=(data.get("choices") or [{}])[0] or {}
        message=choice.get("message") or {}
        content=message.get("content")
        if isinstance(content,str): return content.strip()
        if isinstance(content,list):
            return "".join(str(x.get("text","")) for x in content if isinstance(x,dict)).strip()
        return str(content or "").strip()

    @staticmethod
    def _empty_response_reason(protocol:str, data:dict)->str:
        if not isinstance(data,dict): return "بدنه پاسخ JSON قابل تشخیص نبود."
        if protocol=="gemini":
            reasons=[]
            pf=data.get("promptFeedback") or {}
            if pf.get("blockReason"): reasons.append(f"promptFeedback.blockReason={pf.get('blockReason')}")
            for c in data.get("candidates") or []:
                if c.get("finishReason"): reasons.append(f"finishReason={c.get('finishReason')}")
                if c.get("finishMessage"): reasons.append(f"finishMessage={c.get('finishMessage')}")
                if c.get("safetyRatings"): reasons.append("safetyRatings=present")
            return "؛ ".join(reasons) or "Gemini پاسخ HTTP موفق داد اما متن قابل استخراجی نداشت."
        if protocol=="anthropic":
            return str(data.get("stop_reason") or data.get("error") or "Anthropic متن قابل استخراجی نداشت.")
        choices=data.get("choices") or []
        if choices:
            c=choices[0] or {}
            return f"finish_reason={c.get('finish_reason') or '-'}؛ message.content خالی است."
        return "پاسخ API فاقد choices بود."

    async def _request(self, provider, messages, temperature, max_tokens, forced_protocol:Optional[str]=None, forced_endpoint:Optional[str]=None):
        await self.start()
        key=decrypt_secret(provider.get("encrypted_api_key") or "")
        model=(provider.get("model_name") or "").strip()
        base=provider.get("base_url") or ""
        protocol=forced_protocol or self.protocol(base)
        endpoint=forced_endpoint or self.endpoint(base,protocol,model)
        headers={"Content-Type":"application/json","User-Agent":HTTP_USER_AGENT}
        started=time.perf_counter()

        if protocol=="anthropic":
            headers["x-api-key"]=key
            headers["anthropic-version"]="2023-06-01"
            system="\n".join(m.get("content","") for m in messages if m.get("role")=="system").strip()
            msgs=[{"role":"assistant" if m.get("role")=="assistant" else "user","content":m.get("content","")} for m in messages if m.get("role")!="system"]
            payload={"model":model,"messages":msgs,"max_tokens":max_tokens,"temperature":temperature}
            if system: payload["system"]=system
        elif protocol=="gemini":
            headers["x-goog-api-key"]=key
            parts=[{"text":m.get("content","")} for m in messages if m.get("role")!="system"]
            payload={"contents":[{"role":"user","parts":parts or [{"text":""}]}],"generationConfig":{"temperature":temperature,"maxOutputTokens":max_tokens}}
            sys="\n".join(m.get("content","") for m in messages if m.get("role")=="system").strip()
            if sys: payload["systemInstruction"]={"parts":[{"text":sys}]}
        else:
            headers["Authorization"]=f"Bearer {key}"
            payload={"model":model,"messages":messages,"temperature":temperature,"max_tokens":max_tokens}

        async with self._session.post(endpoint,headers=headers,json=payload) as resp:
            raw=await resp.text()
            latency=int((time.perf_counter()-started)*1000)
            if resp.status!=200:
                raise RuntimeError(f"HTTP {resp.status} | endpoint={endpoint} | body={raw[:1400]}")
            try: data=json.loads(raw)
            except Exception as e: raise RuntimeError(f"HTTP 200 ولی JSON نامعتبر بود: {e} | body={raw[:900]}")
            content=self._extract_content(protocol,data)
            usage=(data.get("usageMetadata") if protocol=="gemini" else data.get("usage")) or {}
            if not content:
                raise RuntimeError(f"پاسخ مدل خالی بود | protocol={protocol} | model={model} | {self._empty_response_reason(protocol,data)} | response={json.dumps(data,ensure_ascii=False)[:1800]}")
            return content,data,latency,usage,protocol,endpoint

    async def test_provider_values(self, base_url, api_key, model):
        await self.start()
        base=(base_url or "").strip(); key=(api_key or "").strip(); mdl=(model or "").strip()
        if not base: return {"ok":False,"stage":"validation","error":"Base URL خالی است."}
        if not key: return {"ok":False,"stage":"validation","error":"API Key/Token خالی است."}
        if not mdl: return {"ok":False,"stage":"validation","error":"نام دقیق مدل خالی است."}
        protocol=self.protocol(base)
        candidates=[(protocol,self.endpoint(base,protocol,mdl))]
        if "generativelanguage.googleapis.com" in base:
            compat=self.google_openai_endpoint(base)
            if compat and all(ep!=compat for _,ep in candidates):
                candidates.append(("openai",compat))
        diagnostics=[]
        for proto,endpoint in candidates:
            preflight="not_run"
            try:
                if "generativelanguage.googleapis.com" in endpoint:
                    headers={"x-goog-api-key":key,"User-Agent":HTTP_USER_AGENT} if proto=="gemini" else {"Authorization":f"Bearer {key}","User-Agent":HTTP_USER_AGENT}
                    model_path=(f"https://generativelanguage.googleapis.com/v1beta/models/{urllib.parse.quote(mdl,safe='')}" if proto=="gemini" else f"https://generativelanguage.googleapis.com/v1beta/openai/models/{urllib.parse.quote(mdl,safe='')}")
                    async with self._session.get(model_path,headers=headers,timeout=aiohttp.ClientTimeout(total=15)) as r:
                        pre_raw=await r.text()
                        if r.status==200: preflight="ok"
                        else:
                            diagnostics.append(f"{proto} preflight HTTP {r.status}: {pre_raw[:700]}")
                            preflight=f"HTTP {r.status}"
                content,data,latency,usage,actual_proto,actual_endpoint=await self._request(
                    {"base_url":base,"encrypted_api_key":encrypt_secret(key),"model_name":mdl},
                    [{"role":"system","content":"You are a connectivity test. Do not analyze anything."},{"role":"user","content":"Reply with exactly: TEST_OK"}],
                    0,32,forced_protocol=proto,forced_endpoint=endpoint)
                return {"ok":True,"latency_ms":latency,"preview":content.strip()[:120],"usage":usage,"protocol":actual_proto,"endpoint":actual_endpoint,"preflight":preflight,"diagnostics":diagnostics}
            except Exception as e:
                diagnostics.append(f"{proto} {endpoint}: {str(e)[:1600]}")
                continue
        return {"ok":False,"stage":"request","protocol":protocol,"endpoint":candidates[0][1],"error":"\n\n".join(diagnostics)[:5000],"diagnostics":diagnostics}

    async def gemini_web_scout(self, site_url:str, max_items:int=5) -> Dict[str,Any]:
        await self.start()
        rows=await self.db.execute("SELECT * FROM ai_providers WHERE enabled=1 ORDER BY priority ASC,id ASC")
        providers=[p for p in rows if "generativelanguage.googleapis.com" in str(p.get("base_url") or "") and p.get("status") not in {"invalid","cooldown"}]
        if not providers: return {"ok":False,"error":"هیچ Provider بومی Gemini برای Web Scout فعال نیست."}
        domain=urllib.parse.urlsplit(site_url).netloc
        prompt=f'''یک Source Scout کنترل‌شده برای یک کانال فارسی فناوری هستی.
URL منبع: {site_url}
دامنه: {domain}

با ابزارهای وب خودت سایت/دامنه را بررسی کن و حداکثر {max_items} مطلب جدید و واقعاً مرتبط پیدا کن. حوزه‌ها: فناوری، هوش مصنوعی، مدل‌ها، ابزارها، امنیت سایبری، آموزش و اخبار مهم جهان. ایران/فارسی‌زبان بودن امتیاز است ولی شرط اجباری نیست. فقط مقاله/خبر مشخص را برگردان، نه صفحه دسته‌بندی یا صفحه اصلی. اگر تاریخ دقیق را نمی‌دانی خالی بگذار؛ حدس نزن.

خروجی فقط JSON معتبر با این ساختار:
{{"items":[{{"title":"...","url":"https://...","description":"خلاصه factual کوتاه","published_at":"...","image_url":"https://..."}}]}}

محتوا را تحلیل یا قضاوت نکن؛ فقط candidateهای واقعی را پیدا و اطلاعات قابل مشاهده را استخراج کن.'''
        for p in providers[:4]:
            key=decrypt_secret(p.get("encrypted_api_key") or ""); model=p.get("model_name") or ""
            endpoint=self.endpoint(p.get("base_url") or "https://generativelanguage.googleapis.com/v1beta","gemini",model)
            headers={"x-goog-api-key":key,"Content-Type":"application/json","User-Agent":HTTP_USER_AGENT}
            payload={"contents":[{"role":"user","parts":[{"text":prompt}]}],"generationConfig":{"temperature":0.1,"maxOutputTokens":1800},"tools":[{"url_context":{}},{"google_search":{}}]}
            try:
                async with self._session.post(endpoint,headers=headers,json=payload,timeout=aiohttp.ClientTimeout(total=45)) as resp:
                    raw=await resp.text()
                    if resp.status!=200:
                        if resp.status in {400,404}:
                            payload["tools"]=[{"url_context":{}}]
                            async with self._session.post(endpoint,headers=headers,json=payload,timeout=aiohttp.ClientTimeout(total=45)) as r2:
                                raw2=await r2.text()
                                if r2.status!=200: raise RuntimeError(f"HTTP {r2.status}: {raw2[:900]}")
                                data=json.loads(raw2)
                        else: raise RuntimeError(f"HTTP {resp.status}: {raw[:900]}")
                    else: data=json.loads(raw)
                    content=self._extract_content("gemini",data)
                    if not content: raise RuntimeError(self._empty_response_reason("gemini",data))
                    obj=parse_json_object(content); items=obj.get("items") if isinstance(obj,dict) else None
                    if not isinstance(items,list): raise RuntimeError("Scout JSON فاقد items است")
                    clean=[]
                    for it in items[:max_items]:
                        if not isinstance(it,dict): continue
                        u=normalize_url(str(it.get("url") or "")); t=strip_html_text(str(it.get("title") or ""))[:500]
                        if not u or not t or not same_domain(u,site_url): continue
                        clean.append({"title":t,"url":u,"description":strip_html_text(str(it.get("description") or ""))[:2000],"published_at":str(it.get("published_at") or "")[:100],"image_url":normalize_url(str(it.get("image_url") or ""))})
                    if clean: return {"ok":True,"items":clean,"provider":p.get("name"),"model":model}
                    raise RuntimeError("Scout پاسخ داد ولی candidate معتبر پیدا نشد")
            except Exception as e:
                await log_automation(self.db,"WARN","gemini_web_scout_failed",f"source={site_url} provider={p.get('id')} {str(e)[:1000]}")
        return {"ok":False,"error":"Gemini Web Scout برای این منبع candidate قابل استفاده برنگرداند."}

    async def test_provider(self, provider_id:int):
        rows=await self.db.execute("SELECT * FROM ai_providers WHERE id=?",[provider_id])
        if not rows: return {"ok":False,"error":"Provider یافت نشد"}
        p=rows[0]
        result=await self.test_provider_values(p.get("base_url",""),decrypt_secret(p.get("encrypted_api_key","")),p.get("model_name",""))
        now=datetime.now(timezone.utc).isoformat()
        if result["ok"]:
            was_unhealthy=bool(p.get("last_error")) or p.get("status") in {"invalid","cooldown"}
            await self.db.execute("UPDATE ai_providers SET status='healthy',last_error=NULL,cooldown_until=NULL,last_checked_at=?,last_latency_ms=?,consecutive_failures=0,updated_at=? WHERE id=?",[now,result.get("latency_ms",0),now,provider_id])
            if was_unhealthy and self.bot and ADMIN_ID:
                try: await self.bot.send_message(ADMIN_ID,f"✅ <b>مدل دوباره فعال شد</b>\n\nModel: <code>{html.escape(str(p.get('model_name')))}</code>\nLatency: {result.get('latency_ms',0)}ms",parse_mode="HTML")
                except Exception: pass
        else:
            cooldown=(datetime.now(timezone.utc)+timedelta(minutes=AI_PROVIDER_RECHECK_MINUTES)).isoformat()
            await self.db.execute("UPDATE ai_providers SET status='invalid',last_error=?,cooldown_until=?,last_checked_at=?,updated_at=? WHERE id=?",[result.get("error","")[:1200],cooldown,now,now,provider_id])
        return result

    @staticmethod
    def classify_error(msg:str)->str:
        m=msg.lower()
        if any(x in m for x in ("404","model_not_found","401","403","authentication","invalid api")): return "invalid"
        return "temporary"

    async def call(self,messages,temperature=0.2,max_tokens=2500,purpose="generic"):
        providers=await self.providers()
        if not providers:
            return {"content":"","provider":None,"model":None,"tokens":0,"error":"هیچ مدل فعالی در پنل AI وجود ندارد."}
        errors=[]; tried=0; now=datetime.now(timezone.utc)
        for p in providers:
            cooldown=p.get("cooldown_until") or ""
            if cooldown:
                try:
                    if datetime.fromisoformat(cooldown.replace("Z","+00:00"))>now: continue
                except Exception: pass
            tried+=1
            try:
                content,data,latency,usage,_,_=await self._request(p,messages,temperature,max_tokens)
                t=datetime.now(timezone.utc).isoformat()
                was_unhealthy=bool(p.get("last_error")) or p.get("status") in {"invalid","cooldown"}
                await self.db.execute("UPDATE ai_providers SET status='healthy',last_error=NULL,cooldown_until=NULL,last_checked_at=?,last_latency_ms=?,consecutive_failures=0,updated_at=? WHERE id=?",[t,latency,t,p["id"]])
                if was_unhealthy and self.bot and ADMIN_ID:
                    try: await self.bot.send_message(ADMIN_ID,f"✅ <b>مدل دوباره فعال شد</b>\n\nModel: <code>{html.escape(str(p.get('model_name')))}</code>\nLatency: {latency}ms",parse_mode="HTML")
                    except Exception: pass
                return {"content":content,"provider":p.get("name"),"model":p.get("model_name"),"tokens":usage.get("total_tokens",0) if isinstance(usage,dict) else 0,"error":None}
            except Exception as e:
                msg=str(e); errors.append(f"{p.get('name')}: {msg[:220]}")
                kind=self.classify_error(msg)
                status="invalid" if kind=="invalid" else "cooldown"
                cd=(datetime.now(timezone.utc)+timedelta(minutes=AI_PROVIDER_RECHECK_MINUTES if kind=="invalid" else 5)).isoformat()
                await self.db.execute("UPDATE ai_providers SET status=?,last_error=?,cooldown_until=?,consecutive_failures=COALESCE(consecutive_failures,0)+1,last_checked_at=?,updated_at=? WHERE id=?",[status,msg[:1200],cd,datetime.now(timezone.utc).isoformat(),datetime.now(timezone.utc).isoformat(),p["id"]])
        final=("همه مدل‌ها در cooldown یا نامعتبر هستند." if tried==0 else "تمام مدل‌های قابل استفاده خطا دادند.")+" | "+" | ".join(errors)
        if purpose!="user_chat" and time.time()-self._last_final_notice>600 and self.bot:
            self._last_final_notice=time.time()
            try: await self.bot.send_message(ADMIN_ID,"🚨 خطای نهایی AI\n"+html.escape(final[:1500]))
            except Exception: pass
        return {"content":"","provider":None,"model":None,"tokens":0,"error":final}


async def ai_analyze_candidate(ai: AIProviderManager, item: Dict[str, Any], source: Dict[str, Any], recent_titles: List[str]) -> Dict[str, Any]:
    body = (item.get("body") or item.get("description") or "")[:10000]
    sim = recent_semantic_similarity(item.get("title", ""), recent_titles)
    prompt = f"""تو سردبیر ارشد یک کانال فارسی درباره تکنولوژی، هوش مصنوعی، ابزارها، مدل‌های AI، امنیت سایبری و اخبار مهم فناوری هستی.\n\nمنبع: {source.get('name')}\nدسته منبع: {source.get('category')}\nعنوان: {item.get('title')}\nلینک: {item.get('url')}\nتاریخ انتشار احتمالی: {item.get('published_at')}\nخلاصه/متن: {body}\nشباهت متنی اولیه با عناوین اخیر: {sim:.2f}\n\nبررسی کن آیا این محتوا ارزش انتشار برای فارسی‌زبانان، مخصوصاً ایران، دارد. clickbait، تبلیغ کم‌ارزش، شایعه، محتوای تکراری و خبرهای فاقد ارزش را رد کن. اگر اطلاعات برای تصمیم‌گیری کافی نیست، رد کن.\n\nفقط JSON معتبر برگردان با این فیلدها:\n{{\n  "accept": true/false,\n  "score": 0-100,\n  "category": "ai|tech|cyber|edu|general",\n  "importance_reason": "...",\n  "iran_relevance": 0-10,\n  "freshness": 0-10,\n  "reliability": 0-10,\n  "duplicate_risk": 0-10,\n  "event_date": "...",\n  "why": "..."\n}}"""
    result = await ai.call([{"role": "system", "content": "You are a strict editorial gate. Output JSON only."}, {"role": "user", "content": prompt}], temperature=0.1, max_tokens=900, purpose="candidate_scoring")
    obj = parse_json_object(result.get("content", ""))
    if not obj:
        return {"accept": False, "score": 0, "reason": "AI returned invalid JSON", "ai": result}
    return {**obj, "ai": result}


async def ai_generate_content(ai: AIProviderManager, item: Dict[str, Any], analysis: Dict[str, Any], source: Dict[str, Any]) -> Dict[str, Any]:
    source_text = (item.get("body") or item.get("description") or "")[:14000]
    prompt = f"""برای یک کانال فارسی حرفه‌ای در حوزه تکنولوژی، هوش مصنوعی، ابزارها، مدل‌ها و امنیت سایبری محتوا تولید کن.\n\nمنبع: {source.get('name')}\nURL: {item.get('url')}\nعنوان منبع: {item.get('title')}\nتحلیل قبلی: {json.dumps(analysis, ensure_ascii=False)}\nمتن منبع:\n{source_text}\n\nخروجی فقط JSON معتبر باشد:\n{{\n  "title": "عنوان دقیق و جذاب بدون clickbait",\n  "channel_text": "متن غنی و مستقل برای کانال، ترجیحاً 400 تا 600 کاراکتر، که خود خبر را توضیح دهد و در انتها جای لینک بیشتر داشته باشد. لینک را خودت ننویس.",\n  "article_text": "مقاله عمیق‌تر و مستقل برای داخل ربات. فقط تکرار channel_text نباشد. زمینه، اتفاق اصلی، جزئیات، اهمیت، اثرات، کاربرد، وضعیت کاربران و ایران در صورت ارتباط، محدودیت‌ها و جمع‌بندی را پوشش بده. طول متناسب با موضوع باشد.",\n  "category": "ai|tech|cyber|edu|general",\n  "facts": ["..."],\n  "image_note": "brief reason if source image is suitable"\n}}\n\nدر article_text هیچ ادعای مهمی که از منبع یا تحلیل داده‌ها پشتیبانی نمی‌شود نساز. فارسی طبیعی و خوانا بنویس."""
    result = await ai.call([{"role": "system", "content": "You are an expert Persian technology editor. Output JSON only."}, {"role": "user", "content": prompt}], temperature=0.35, max_tokens=4500, purpose="content_generation")
    obj = parse_json_object(result.get("content", ""))
    if not obj:
        return {"error": "invalid generation JSON", "ai": result}
    return {**obj, "ai": result}


async def ai_verify_content(ai: AIProviderManager, item: Dict[str, Any], generated: Dict[str, Any]) -> Dict[str, Any]:
    prompt = f"""محتوای زیر را با منبع مقایسه کن.\n\nSOURCE:\nعنوان: {item.get('title')}\nURL: {item.get('url')}\nمتن: {(item.get('body') or item.get('description') or '')[:12000]}\n\nGENERATED:\n{json.dumps(generated, ensure_ascii=False)}\n\nفقط JSON معتبر بده:\n{{\n "ok": true/false,\n "issues": ["..."],\n "confidence": 0-100\n}}\n\nهر ادعای ساخته‌شده، تاریخ/عدد نادرست، تناقض، hallucination یا تکرار ضعیف را مشکل بدان."""
    result = await ai.call([{"role": "system", "content": "You are a strict fact-checking editor. Output JSON only."}, {"role": "user", "content": prompt}], temperature=0, max_tokens=1200, purpose="content_verification")
    obj = parse_json_object(result.get("content", ""))
    return obj if obj else {"ok": False, "issues": ["invalid verifier response"], "confidence": 0}


def make_deep_token(article_id: int) -> str:
    return hashlib.sha256(f"techhow-{article_id}-{time.time_ns()}".encode()).hexdigest()[:18]



class TelegramHTMLSanitizer(HTMLParser):
    ALLOWED = {"b","strong","i","em","u","s","del","code","pre","blockquote","a","tg-spoiler"}
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.out=[]
    def handle_data(self, data):
        self.out.append(html.escape(data, quote=False))
    def handle_starttag(self, tag, attrs):
        tag=tag.lower()
        if tag not in self.ALLOWED: return
        if tag=="a":
            href=dict(attrs).get("href","")
            if href.startswith(("https://","http://","tg://")):
                self.out.append(f'<a href="{html.escape(href,quote=True)}">')
        else:
            self.out.append(f"<{tag}>")
    def handle_startendtag(self, tag, attrs):
        if tag.lower()=="br": self.out.append("\n")
    def handle_endtag(self, tag):
        tag=tag.lower()
        if tag in self.ALLOWED and tag!="a": self.out.append(f"</{tag}>")
        elif tag=="a": self.out.append("</a>")

def sanitize_telegram_html(value: str) -> str:
    value = (value or "").strip()
    if not value: return ""
    try:
        p=TelegramHTMLSanitizer(); p.feed(value); p.close(); return "".join(p.out).strip()
    except Exception:
        return html.escape(strip_html_text(value), quote=False)

def plain_len(value: str) -> int:
    return len(strip_html_text(value or ""))

def rich_channel_fallback(title: str, text: str) -> str:
    clean=strip_html_text(text or "")
    if len(clean)>700: clean=clean[:700].rsplit(" ",1)[0]+"…"
    return f"<b>🔎 {html.escape(title[:180])}</b>\n\n{html.escape(clean)}"

def ensure_rich_channel_format(title: str, value: str, category: str = "tech") -> str:
    """Guarantee a visually structured Telegram caption even when a model returns plain text."""
    clean=sanitize_telegram_html(value or "")
    plain=strip_html_text(clean)
    if not plain: return rich_channel_fallback(title, "اطلاعات قابل انتشار از منبع دریافت شد.")
    if not any(tag in clean.lower() for tag in ("<b>","<strong>","<i>","<blockquote>")):
        emoji={"ai":"🤖","cyber":"🔐","tech":"💻","edu":"📚","general":"🌐"}.get(category,"💻")
        clean=f"<b>{emoji} {html.escape(title[:180])}</b>\n\n{clean}"
    return clean

def ensure_rich_article_format(title: str, value: str, source_url: str) -> str:
    clean=sanitize_telegram_html(value or "")
    plain=strip_html_text(clean)
    if not plain: return rich_article_fallback(title, "اطلاعات کافی برای تهیه متن کامل از منبع دریافت شد.", source_url)
    if not any(tag in clean.lower() for tag in ("<b>","<strong>","<i>","<blockquote>")):
        clean=f"<b>{html.escape(title[:220])}</b>\n\n{clean}"
    return clean

def rich_article_fallback(title: str, text: str, source_url: str) -> str:
    clean=strip_html_text(text or "")
    return (f"<b>{html.escape(title[:220])}</b>\n\n"
            f"{html.escape(clean)}\n\n"
            f"<blockquote>این مطلب بر پایه اطلاعات منبع تهیه شده؛ جزئیات و زمینه را می‌توانید از متن کامل بخوانید.</blockquote>\n"
            f"<a href=\"{html.escape(source_url,quote=True)}\">🔗 منبع اصلی</a>")

def make_article_png(width=1280,height=720):
    # کارت تصویری fallback؛ بدون وابستگی به PIL. برای زمانی که منبع عکس قابل دریافت نیست.
    raw=bytearray()
    for y in range(height):
        raw.append(0)
        for x in range(width):
            t=x/(width-1); u=y/(height-1)
            r=int(18+38*t); g=int(24+50*u); b=int(55+95*(1-t))
            # الگوی نرم و جذاب برای جلوگیری از پست بدون تصویر
            glow=max(0,1-(((x-width*.78)/(width*.35))**2+((y-height*.28)/(height*.45))**2))
            r=min(255,int(r+45*glow)); g=min(255,int(g+30*glow)); b=min(255,int(b+55*glow))
            raw.extend((r,g,b))
    def chunk(tag,data):
        return struct.pack('>I',len(data))+tag+data+struct.pack('>I',zlib.crc32(tag+data)&0xffffffff)
    return b'\x89PNG\r\n\x1a\n'+chunk(b'IHDR',struct.pack('>IIBBBBB',width,height,8,2,0,0,0))+chunk(b'IDAT',zlib.compress(bytes(raw),6))+chunk(b'IEND',b'')

def extract_xml_locs_resilient(text: str) -> List[str]:
    # بعض سایت‌ها XML ناقص/بزرگ تحویل می‌دهند؛ در این حالت فقط locها را با regex بیرون می‌کشیم.
    found=[]
    for m in re.finditer(r"<loc[^>]*>\\s*(.*?)\\s*</loc>", text or "", flags=re.I|re.S):
        u=html.unescape(re.sub(r"<[^>]+>","",m.group(1)).strip())
        if u: found.append(normalize_url(u))
    return [u for u in dict.fromkeys(found) if u]

async def ai_editorial_process(ai: AIProviderManager,item:Dict[str,Any],source:Dict[str,Any],recent_titles:List[str],weights:Dict[str,float]):
    body=(item.get("body") or item.get("description") or "")[:14000]
    prompt=f"""تو موتور تحریریه و تولید محتوای یک کانال فارسی حرفه‌ای هستی؛ نه قاضی، نه مفسر سیاسی و نه منتقد.
وظیفه تو این است که از منبع داده‌شده، اگر واقعاً ارزش انتشار داشت، محتوای غنی، دقیق، بی‌طرف و قابل‌فهم بسازی.

موضوعات کانال: فناوری، هوش مصنوعی، ابزارها و مدل‌ها، امنیت سایبری، آموزش و اخبار مهم جهان.
مخاطب: فارسی‌زبان‌ها، با توجه بیشتر به ایران؛ اما «ایران‌محور بودن» فقط یک امتیاز است و شرط اجباری نیست.

منبع: {source.get('name')}
عنوان: {item.get('title')}
URL: {item.get('url')}
متن منبع:
{body}

وزن‌های تعیین‌شده توسط مدیر:
{json.dumps(weights,ensure_ascii=False)}

اول فقط برای تصمیم داخلی، امتیاز 0 تا 100 بده. این تصمیم نباید وارد متن نهایی شود و نباید با عبارت‌هایی مثل «این خبر مهم است»، «این خبر ارزشمند است»، «ما توصیه می‌کنیم» یا قضاوت شخصی نوشته شود.
اگر اطلاعات کافی برای تولید دقیق وجود ندارد، accept=false بده.

اگر accept=true، همزمان محتوای نهایی را تولید کن:
1) channel_html: حدود 450 تا 650 کاراکتر «خودِ خبر»؛ نه teaser و نه صرفاً معرفی لینک. ساختار بصری داشته باشد: تیتر/شروع با <b>، حداکثر 1 بخش کوتاه با <i> یا <blockquote> فقط وقتی طبیعی است، پاراگراف‌های کوتاه و 1 تا 3 ایموجی دقیق و مرتبط.
2) article_html: حدود 1600 تا 2600 کاراکتر و در صورت نیاز بیشتر؛ مستقل و غنی‌تر از متن کانال. از تیترهای کوتاه با <b>، پاراگراف‌های کوتاه و در صورت مناسب یک <blockquote> استفاده کن. متن باید برای خواندن در موبایل خوش‌خوان باشد و صرفاً کپی/تکرار channel_html نباشد.
3) title: کوتاه، جذاب و غیرکلیک‌بیتی.
4) category و facts.

قواعد نگارش:
- فارسی روان، دوستانه، عامیانه و خوش‌خوان؛ رسمی و خشک نباش.
- اگر اصطلاح فنی لازم است، معادل فارسی + اصطلاح انگلیسی را بیاور.
- از ایموجی کم اما دقیق و مرتبط استفاده کن.
- هیچ نتیجه‌گیری شخصی یا قضاوتی به کاربر تحمیل نکن.
- «طبق منبع»، «گزارش شده» و «این شرکت گفته» را فقط وقتی لازم است برای نسبت‌دادن ادعا استفاده کن.
- چیزی را که در منبع نیست به عنوان واقعیت نساز.
- channel_html و article_html را با HTML سازگار با Telegram بده؛ Markdown استفاده نکن.
- لینک منبع را خودت داخل channel_html یا article_html قرار نده؛ برنامه آن را اضافه می‌کند.

فقط JSON معتبر:
{{"accept":true/false,"score":0-100,"global_relevance":0-10,"technology_relevance":0-10,"ai_relevance":0-10,"cyber_relevance":0-10,"education_relevance":0-10,"iran_relevance":0-10,"freshness":0-10,"reliability":0-10,"duplicate_risk":0-10,"category":"ai|tech|cyber|edu|general","why":"دلیل داخلی کوتاه","title":"...","channel_html":"...","article_html":"...","facts":["..."]}}"""
    result=await ai.call([{"role":"system","content":"You are a Persian technology content producer. Be neutral and factual. Return JSON only."},{"role":"user","content":prompt}],0.35,5000,"editorial")
    obj=parse_json_object(result.get("content",""))
    if not obj:
        repair_prompt=("پاسخ زیر را فقط به JSON معتبر تبدیل کن؛ محتوای آن را تغییر نده. فیلدها: accept,score,category,iran_relevance,freshness,reliability,duplicate_risk,why,title,channel_html,article_html,facts.\n\n"+str(result.get("content",""))[:14000])
        retry=await ai.call([{"role":"system","content":"Return valid JSON only."},{"role":"user","content":repair_prompt}],0,4200,"editorial_json_repair")
        obj=parse_json_object(retry.get("content","")); result=retry
    if not obj: return {"error":"پاسخ AI JSON معتبر نبود","ai":result}
    # اگر مدل فیلدهای HTML را نساخته بود، از متن معمولی fallback می‌سازیم.
    title=strip_html_text(obj.get("title") or item.get("title") or "")[:240]
    category=str(obj.get("category") or source.get("category") or "tech")
    ch=ensure_rich_channel_format(title, obj.get("channel_html") or obj.get("channel_text") or "", category)
    ar=ensure_rich_article_format(title, obj.get("article_html") or obj.get("article_text") or "", item.get("url") or "")
    obj["title"]=title; obj["channel_html"]=ch; obj["article_html"]=ar
    # امتیاز نهایی را خود ربات از وزن‌های مدیر محاسبه می‌کند؛ بنابراین تغییر وزن واقعاً اثر دارد.
    dims={
        "global":float(obj.get("global_relevance",5) or 0),
        "technology":float(obj.get("technology_relevance",5) or 0),
        "ai":float(obj.get("ai_relevance",5) or 0),
        "cyber":float(obj.get("cyber_relevance",5) or 0),
        "education":float(obj.get("education_relevance",5) or 0),
        "iran":float(obj.get("iran_relevance",0) or 0),
        "freshness":float(obj.get("freshness",5) or 0),
        "source":max(0,min(10,float(source.get("trust_score") or 80)/10)),
        "novelty":10-max(0,min(10,float(obj.get("duplicate_risk",0) or 0)))
    }
    total_weight=sum(max(0,float(weights.get(k,0))) for k in dims)
    weighted=sum(max(0,min(10,v))*max(0,float(weights.get(k,0))) for k,v in dims)
    obj["score"]=round((weighted/(total_weight*10))*100,1) if total_weight else round(float(obj.get("score",0) or 0),1)
    return {**obj,"ai":result}

async def add_source(db: D1Database, url: str, category: str = "tech", interval_minutes: Optional[int] = None, priority: int = 5) -> int:
    clean = normalize_url(url)
    if not clean:
        raise ValueError("invalid URL")
    parsed = urllib.parse.urlsplit(clean)
    name = parsed.netloc or clean
    interval = interval_minutes or int(await get_setting(db, "default_source_interval", str(DEFAULT_SOURCE_INTERVAL_MINUTES)))
    now = datetime.now(timezone.utc)
    next_check = now.isoformat()
    res = await db.execute("INSERT INTO sources(name, url, category, enabled, interval_minutes, priority, next_check_at, created_at) VALUES(?, ?, ?, 1, ?, ?, ?, ?) RETURNING id", [name, clean, category, interval, priority, next_check, now.isoformat()])
    source_id = res[0].get("id") if res else 0
    if not source_id:
        source_id = (await db.execute("SELECT id FROM sources WHERE url = ?", [clean]))[0].get("id")
    return int(source_id)


async def fetch_source_cycle(db: D1Database, source: Dict[str,Any], ai: AIProviderManager):
    source_id=source["id"]; now=datetime.now(timezone.utc)
    try:
        discovery=await discover_source_items(source, return_diagnostics=True)
        items=discovery.get("items") or []
        if not items and await get_setting(db,"ai_web_scout_enabled","1") == "1":
            try:
                scout=await ai.gemini_web_scout(source.get("url") or "", int(await get_setting(db,"ai_web_scout_max_items","5")))
                if scout.get("ok"):
                    items=scout.get("items") or []
                    await log_automation(db,"INFO","gemini_web_scout_used",f"source={source.get('id')} provider={scout.get('provider')} items={len(items)}")
            except Exception as scout_error:
                await log_automation(db,"WARN","gemini_web_scout_error",f"source={source.get('id')} {scout_error}")
        next_check=(now+timedelta(minutes=int(source.get("interval_minutes") or DEFAULT_SOURCE_INTERVAL_MINUTES))).isoformat()
        await db.execute("UPDATE sources SET last_checked_at=?,next_check_at=?,last_error=NULL WHERE id=?",[now.isoformat(),next_check,source_id])
        recent_rows=await db.execute("SELECT title FROM articles WHERE status='published' ORDER BY id DESC LIMIT 30")
        recent_titles=[r.get("title","") for r in recent_rows]
        weight_keys=["global","technology","ai","cyber","education","iran","freshness","source","novelty"]
        weights={k:float(await get_setting(db,f"weight_{k}","10")) for k in weight_keys}
        sem=asyncio.Semaphore(max(1,min(4,int(await get_setting(db,"max_ai_workers","3")))))
        async def process_one(raw):
            raw_title=strip_html_text(raw.get("title",""))[:500]
            raw_desc=strip_html_text(raw.get("description",""))[:2000]
            raw_url=normalize_url(raw.get("url"))
            if not raw_url or not raw_title or not heuristic_topic_match(raw_title,raw_desc,source.get("category","tech")):
                return 0
            item=await enrich_candidate_content(dict(raw))
            title=strip_html_text(item.get("title") or raw_title)[:500]
            url=normalize_url(item.get("url") or raw_url)
            if not url or not title:
                return 0
            async with sem:
                if await db.execute("SELECT id FROM source_items WHERE source_id=? AND canonical_url=?",[source_id,url]): return 0
                body=item.get("body") or item.get("description") or ""
                content_hash=text_hash(title+" "+body)
                if await db.execute("SELECT id FROM source_items WHERE content_hash=? LIMIT 1",[content_hash]): return 0
                ins=await db.execute("INSERT INTO source_items(source_id,canonical_url,title,description,content,image_url,published_at,discovered_at,content_hash,status,category) VALUES(?,?,?,?,?,?,?,?,?,'analyzing',?) RETURNING id",
                    [source_id,url,title,item.get("description","")[:2000],body[:14000],item.get("image_url","")[:1000],item.get("published_at","")[:100],now.isoformat(),content_hash,source.get("category","tech")])
                item_id=ins[0].get("id") if ins else 0
                out=await ai_editorial_process(ai,item,source,recent_titles,weights)
                if out.get("error"):
                    await db.execute("UPDATE source_items SET status='error',last_error=? WHERE id=?",[out["error"][:1000],item_id]); return 1
                score=float(out.get("score",0) or 0)
                min_score=float(await get_setting(db,"min_content_score",str(DEFAULT_MIN_CONTENT_SCORE)))
                accept=bool(out.get("accept")) and score>=min_score and int(out.get("duplicate_risk",0) or 0)<7
                if not accept:
                    await db.execute("UPDATE source_items SET status='rejected',score=?,category=?,last_error=? WHERE id=?",[score,out.get("category",source.get("category","tech")),str(out.get("why") or "score below threshold")[:1000],item_id]); return 1
                verify_mode=await get_setting(db,"ai_verify_mode","auto")
                need_verify=(verify_mode=="always") or (verify_mode=="auto" and (score<90 or float(source.get("trust_score") or 80)<85))
                if need_verify:
                    verify=await ai_verify_content(ai,item,out)
                    if not verify.get("ok") or float(verify.get("confidence",0) or 0)<80:
                        await db.execute("UPDATE source_items SET status='rejected',score=?,last_error=? WHERE id=?",[score,json.dumps(verify,ensure_ascii=False)[:1000],item_id]); return 1
                title_out=strip_html_text(out.get("title") or title)[:500]
                article_text=ensure_rich_article_format(title_out, out.get("article_html") or out.get("article_text") or "", url)
                channel_text=ensure_rich_channel_format(title_out, out.get("channel_html") or out.get("channel_text") or "", str(out.get("category") or source.get("category") or "tech"))
                if plain_len(channel_text)>780: channel_text=rich_channel_fallback(title_out, channel_text)
                if plain_len(article_text)<1200: article_text=rich_article_fallback(title_out, article_text, url)
                if plain_len(article_text)>4200: article_text=ensure_rich_article_format(title_out, strip_html_text(article_text)[:4200], url)
                if plain_len(article_text)<900:
                    await db.execute("UPDATE source_items SET status='rejected',last_error='article too short' WHERE id=?",[item_id]); return 1
                art=await db.execute("INSERT INTO articles(source_item_id,title,channel_text,body,source_url,image_url,category,score,status,created_at) VALUES(?,?,?,?,?,?,?,?, 'ready',?) RETURNING id",
                    [item_id,title_out,channel_text,article_text[:18000],url,item.get("image_url",""),out.get("category") or source.get("category","tech"),score,now.isoformat()])
                aid=art[0].get("id") if art else 0
                token=make_deep_token(int(aid))
                await db.execute("UPDATE articles SET deep_token=? WHERE id=?",[token,aid])
                await db.execute("UPDATE source_items SET status='ready',article_id=?,score=? WHERE id=?",[aid,score,item_id])
                await db.execute("INSERT OR IGNORE INTO publication_queue(article_id,scheduled_at,status,attempts,created_at) VALUES(?,?, 'queued',0,?)",[aid,now.isoformat(),now.isoformat()])
                return 1
        results=await asyncio.gather(*(process_one(x) for x in items[:MAX_SOURCE_ITEMS_PER_CYCLE]),return_exceptions=True)
        return sum(int(x) for x in results if isinstance(x,int))
    except Exception as e:
        await db.execute("UPDATE sources SET last_checked_at=?,next_check_at=?,last_error=? WHERE id=?",[now.isoformat(),(now+timedelta(minutes=int(source.get("interval_minutes") or DEFAULT_SOURCE_INTERVAL_MINUTES))).isoformat(),str(e)[:1200],source_id])
        await log_automation(db,"ERROR","source_cycle_failed",f"source={source_id} {e}")
        return 0


async def can_publish_now(db: D1Database) -> bool:
    if not await get_channel_id(db):
        return False
    enabled = await get_setting(db, "automation_enabled", "0")
    if enabled != "1":
        return False
    tehran = datetime.now(pytz.timezone("Asia/Tehran"))
    start_h = int(await get_setting(db, "publish_start_hour", str(DEFAULT_PUBLISH_START_HOUR)))
    end_h = int(await get_setting(db, "publish_end_hour", str(DEFAULT_PUBLISH_END_HOUR)))
    if not (start_h <= tehran.hour <= end_h):
        return False
    count_rows = await db.execute("SELECT COUNT(*) as c FROM articles WHERE status='published' AND COALESCE(published_at,created_at) >= ?", [tehran.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc).isoformat()])
    max_daily = int(await get_setting(db, "max_daily_posts", str(DEFAULT_MAX_DAILY_POSTS)))
    if (count_rows[0].get("c", 0) if count_rows else 0) >= max_daily:
        return False
    last_manual = await get_setting(db, "last_manual_channel_post_at", "")
    last_pub = await db.execute("SELECT published_at FROM publication_queue WHERE status='published' ORDER BY id DESC LIMIT 1")
    latest_times = [x for x in [last_manual, last_pub[0].get("published_at") if last_pub else ""] if x]
    if latest_times:
        latest = max(latest_times)
        try:
            delta = datetime.now(timezone.utc) - datetime.fromisoformat(latest.replace("Z", "+00:00"))
            min_hours = float(await get_setting(db, "min_hours_between_posts", str(DEFAULT_MIN_HOURS_BETWEEN_POSTS)))
            if delta.total_seconds() < min_hours * 3600:
                return False
        except Exception:
            pass
    return True


async def get_runtime_bot_username(bot: Bot) -> str:
    global BOT_USERNAME_RUNTIME
    if BOT_USERNAME_RUNTIME:
        return BOT_USERNAME_RUNTIME
    try:
        me=await bot.get_me()
        BOT_USERNAME_RUNTIME=me.username or BOT_USERNAME.lstrip("@")
    except Exception:
        BOT_USERNAME_RUNTIME=BOT_USERNAME.lstrip("@")
    return BOT_USERNAME_RUNTIME

async def publish_next_article(db: D1Database, bot: Bot, force: bool=False) -> bool:
    if force:
        channel_id=await get_channel_id(db)
        if not channel_id: return False
        max_daily=int(await get_setting(db,"max_daily_posts",str(DEFAULT_MAX_DAILY_POSTS)))
        tehran=datetime.now(pytz.timezone("Asia/Tehran"))
        count_rows=await db.execute("SELECT COUNT(*) c FROM articles WHERE status='published' AND COALESCE(published_at,created_at) >= ?",[tehran.replace(hour=0,minute=0,second=0,microsecond=0).astimezone(timezone.utc).isoformat()])
        if (count_rows[0].get("c",0) if count_rows else 0)>=max_daily: return False
    elif not await can_publish_now(db):
        return False
    now_iso=datetime.now(timezone.utc).isoformat()
    schedule_filter="" if force else " AND (q.scheduled_at IS NULL OR q.scheduled_at <= ?)"
    params=[now_iso] if not force else []
    rows=await db.execute("SELECT q.id as queue_id,q.article_id,a.* FROM publication_queue q JOIN articles a ON a.id=q.article_id WHERE q.status='queued' AND a.status='ready'"+schedule_filter+" ORDER BY a.score DESC,q.created_at ASC LIMIT 1",params)
    if not rows:
        return False
    row=rows[0]; queue_id=row["queue_id"]; article_id=row["article_id"]
    await db.execute("UPDATE publication_queue SET status='publishing',attempts=attempts+1 WHERE id=?",[queue_id])
    try:
        token=row.get("deep_token")
        bot_username=await get_runtime_bot_username(bot)
        if not token or not bot_username: raise RuntimeError("deep link token یا نام کاربری ربات تنظیم نشده است")
        deep_link=f"https://t.me/{bot_username}?start=auto_{token}"
        channel_id=await get_channel_id(db)
        channel_text=sanitize_telegram_html(row.get("channel_text") or row.get("title") or "")
        channel_text += f"\n\n<a href=\"{html.escape(deep_link,quote=True)}\">📖 بیشتر بخوانید</a>"
        image_url=row.get("image_url") or ""
        sent=None
        if image_url:
            try:
                sent=await bot.send_photo(chat_id=channel_id,photo=image_url,caption=channel_text[:1024],parse_mode="HTML")
            except Exception as img_error:
                await log_automation(db,"WARN","source_image_failed",f"article={article_id} {img_error}")
        if sent is None:
            try:
                sent=await bot.send_photo(chat_id=channel_id,photo=BufferedInputFile(make_article_png(),filename="tech-content-card.png"),caption=channel_text[:1024],parse_mode="HTML")
            except Exception:
                sent=await bot.send_message(chat_id=channel_id,text=channel_text[:4096],parse_mode="HTML",disable_web_page_preview=False)
        published_at=datetime.now(timezone.utc).isoformat()
        await db.execute("UPDATE articles SET status='published',published_message_id=?,published_at=? WHERE id=?",[getattr(sent,"message_id",0),published_at,article_id])
        await db.execute("UPDATE publication_queue SET status='published',published_at=? WHERE id=?",[published_at,queue_id])
        await log_automation(db,"INFO","published",f"article={article_id} message={getattr(sent,'message_id',0)} force={force}")
        return True
    except Exception as e:
        await db.execute("UPDATE publication_queue SET status='failed',last_error=? WHERE id=?",[str(e)[:1500],queue_id])
        await db.execute("UPDATE articles SET status='ready' WHERE id=?",[article_id])
        await log_automation(db,"ERROR","publication_failed",f"article={article_id} {e}")
        try: await bot.send_message(ADMIN_ID,f"❌ خطا در انتشار خودکار\nArticle: {article_id}\nError: {html.escape(str(e)[:800])}")
        except Exception: pass
        return False

async def recheck_failed_providers(db:D1Database,bot:Bot,manager:AIProviderManager):
    now=datetime.now(timezone.utc).isoformat()
    rows=await db.execute("SELECT id,name,model_name,status,cooldown_until FROM ai_providers WHERE enabled=1 AND status IN ('invalid','cooldown') AND (cooldown_until IS NULL OR cooldown_until <= ?) ORDER BY priority ASC LIMIT 8",[now])
    for p in rows:
        try:
            result=await manager.test_provider(int(p["id"]))
            if result.get("ok"):
                try:
                    await bot.send_message(ADMIN_ID,f"✅ <b>مدل دوباره در دسترس است</b>\n\nProvider: {html.escape(str(p.get('name')))}\nModel: <code>{html.escape(str(p.get('model_name')))}</code>\nLatency: {result.get('latency_ms',0)}ms",parse_mode="HTML")
                except Exception: pass
        except Exception as e:
            await log_automation(db,"ERROR","provider_recheck_failed",f"provider={p.get('id')} {e}")

async def automation_loop(db: D1Database, bot: Bot):
    ai = AIProviderManager(db, bot)
    last_cleanup = 0.0
    last_provider_recheck = 0.0
    try:
        while True:
            try:
                enabled = await get_setting(db, "automation_enabled", "0")
                if enabled == "1":
                    max_workers = max(1, min(4, int(await get_setting(db, "max_workers", "2"))))
                    due_sources = await db.execute("SELECT * FROM sources WHERE enabled=1 AND (next_check_at IS NULL OR next_check_at <= ?) ORDER BY priority DESC, next_check_at ASC LIMIT 8", [datetime.now(timezone.utc).isoformat()])
                    sem = asyncio.Semaphore(max_workers)
                    async def run_source(source):
                        async with sem:
                            return await fetch_source_cycle(db, source, ai)
                    await publish_next_article(db, bot)
                    if due_sources:
                        await asyncio.gather(*(run_source(src) for src in due_sources), return_exceptions=True)
                    await publish_next_article(db, bot)
                if time.time() - last_provider_recheck > AI_PROVIDER_RECHECK_MINUTES*60:
                    await recheck_failed_providers(db,bot,ai)
                    last_provider_recheck = time.time()
                if time.time() - last_cleanup > 3600:
                    await cleanup_automation_data(db)
                    last_cleanup = time.time()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.exception("automation loop error")
                await log_automation(db, "ERROR", "automation_loop_failed", str(e))
            await asyncio.sleep(15)
    finally:
        await ai.close()


async def automation_report(db: D1Database) -> str:
    settings = {
        "enabled": await get_setting(db, "automation_enabled", "0"),
        "max_daily": await get_setting(db, "max_daily_posts", str(DEFAULT_MAX_DAILY_POSTS)),
        "min_score": await get_setting(db, "min_content_score", str(DEFAULT_MIN_CONTENT_SCORE)),
    }
    sources = await db.execute("SELECT COUNT(*) as c FROM sources WHERE enabled=1")
    new_items = await db.execute("SELECT COUNT(*) as c FROM source_items WHERE discovered_at >= ?", [(datetime.now(timezone.utc) - timedelta(days=1)).isoformat()])
    queued = await db.execute("SELECT COUNT(*) as c FROM publication_queue WHERE status='queued'")
    published = await db.execute("SELECT COUNT(*) as c FROM articles WHERE status='published' AND COALESCE(published_at,created_at) >= ?", [datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()])
    failed = await db.execute("SELECT COUNT(*) as c FROM publication_queue WHERE status='failed' AND created_at >= ?", [(datetime.now(timezone.utc) - timedelta(days=1)).isoformat()])
    channel = await get_channel_id(db)
    channel_label = await get_setting(db, 'channel_username', '') or ('کانال خصوصی تنظیم شده' if channel else '')
    return (f"📊 گزارش اتوماسیون\n\n"
            f"🟢 وضعیت: {'فعال' if settings['enabled']=='1' else 'خاموش'}\n"
            f"📢 کانال: {channel_label or 'تنظیم نشده'}\n"
            f"🌐 منابع فعال: {sources[0].get('c',0) if sources else 0}\n"
            f"📰 آیتم جدید ۲۴ساعت: {new_items[0].get('c',0) if new_items else 0}\n"
            f"⏳ در صف: {queued[0].get('c',0) if queued else 0}\n"
            f"📢 منتشرشده امروز: {published[0].get('c',0) if published else 0}/{settings['max_daily']}\n"
            f"⭐ حداقل امتیاز: {settings['min_score']}\n"
            f"❌ انتشار ناموفق ۲۴ساعت: {failed[0].get('c',0) if failed else 0}")


def automation_menu_kb(enabled: bool) -> InlineKeyboardMarkup:
    state_text = "⏸ خاموش کردن اتوماسیون" if enabled else "▶️ روشن کردن اتوماسیون"
    state_cb = "auto_off" if enabled else "auto_on"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=state_text, callback_data=state_cb)],
        [InlineKeyboardButton(text="🌐 منابع خبری", callback_data="auto_sources"), InlineKeyboardButton(text="🤖 مدل‌های AI", callback_data="auto_providers")],
        [InlineKeyboardButton(text="📢 کانال و انتشار", callback_data="auto_channel"), InlineKeyboardButton(text="🧠 کیفیت محتوا", callback_data="auto_quality")],
        [InlineKeyboardButton(text="⏱ برنامه انتشار", callback_data="auto_schedule"), InlineKeyboardButton(text="📥 صف انتشار", callback_data="auto_queue")],
        [InlineKeyboardButton(text="🧪 تست و سلامت", callback_data="auto_health"), InlineKeyboardButton(text="📊 گزارش", callback_data="auto_report")],
        [InlineKeyboardButton(text="🔙 پنل اصلی", callback_data="admin_home")]
    ])


def source_list_kb(sources: List[Dict[str, Any]]) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text="➕ افزودن منبع", callback_data="auto_add_source")]]
    for s in sources[:20]:
        mark = "🟢" if s.get("enabled") else "🔴"
        rows.append([InlineKeyboardButton(text=f"{mark} {s.get('name','source')[:35]}", callback_data=f"source_view_{s['id']}")])
    rows.append([InlineKeyboardButton(text="🔙 بازگشت", callback_data="auto_back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def provider_list_kb(providers: List[Dict[str, Any]]) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text="➕ افزودن مدل جدید", callback_data="auto_add_provider")],
            [InlineKeyboardButton(text="ℹ️ راهنمای مدیریت مدل‌ها", callback_data="provider_help")]]
    for p in providers[:20]:
        status = p.get('status') or 'unknown'
        mark = {'healthy':'🟢', 'invalid':'🔴', 'cooldown':'🟡'}.get(status, '⚪')
        enabled = 'فعال' if p.get('enabled') else 'خاموش'
        label = f"{mark} #{p['id']} {str(p.get('model_name','model'))[:30]} · {enabled}"
        rows.append([InlineKeyboardButton(text=label, callback_data=f"provider_view_{p['id']}")])
    rows.append([InlineKeyboardButton(text="🔙 اتوماسیون محتوا", callback_data="auto_back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def quality_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⭐ حداقل امتیاز انتشار", callback_data="set_min_score")],
        [InlineKeyboardButton(text="🎯 وزن معیارهای محتوا", callback_data="quality_weights")],
        [InlineKeyboardButton(text="🧾 معیارها", callback_data="quality_about")],
        [InlineKeyboardButton(text="🔙 اتوماسیون محتوا", callback_data="auto_back")]
    ])

def schedule_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔢 سقف تقریبی پست روزانه", callback_data="set_max_daily")],
        [InlineKeyboardButton(text="⏱ حداقل فاصله پست‌ها", callback_data="set_min_gap")],
        [InlineKeyboardButton(text="🌐 فاصله بررسی منابع", callback_data="set_default_interval")],
        [InlineKeyboardButton(text="⚡ Workerهای بررسی منابع", callback_data="set_workers")],
        [InlineKeyboardButton(text="🧠 Workerهای AI", callback_data="set_ai_workers")],
        [InlineKeyboardButton(text="🛡 راستی‌آزمایی AI", callback_data="set_ai_verify")],
        [InlineKeyboardButton(text="🔙 اتوماسیون محتوا", callback_data="auto_back")]
    ])

async def reset_database(db: D1Database):
    queries = [
        {"sql": "DROP TABLE IF EXISTS users"},
        {"sql": "DROP TABLE IF EXISTS posts"},
        {"sql": "DROP TABLE IF EXISTS saves"},
        {"sql": "DROP TABLE IF EXISTS user_states"},
        {"sql": "DROP TABLE IF EXISTS votes"},
        {"sql": "DROP TABLE IF EXISTS processed_updates"},
        {"sql": "DROP TABLE IF EXISTS sources"},
        {"sql": "DROP TABLE IF EXISTS source_items"},
        {"sql": "DROP TABLE IF EXISTS articles"},
        {"sql": "DROP TABLE IF EXISTS publication_queue"},
        {"sql": "DROP TABLE IF EXISTS ai_providers"},
        {"sql": "DROP TABLE IF EXISTS automation_settings"},
        {"sql": "DROP TABLE IF EXISTS automation_logs"},
        {"sql": "DROP TABLE IF EXISTS manual_channel_events"}
    ]
    await db.execute_batch(queries)
    await initialize_database(db)
    await initialize_automation_database(db)

# ============================================================
# ماشین وضعیت کاربران (FSM States)
# ============================================================
class BotStates(StatesGroup):
    idle = State()
    ai_chat = State()
    user_chat_admin = State()
    waiting_post_content = State()
    waiting_post_confirm = State()
    waiting_broadcast_content = State()
    waiting_broadcast_confirm = State()
    admin_search_word = State()
    admin_view_all = State()
    admin_post_edit = State()
    admin_source_priority = State()
    admin_content_weight = State()
    user_search_folder = State()
    admin_add_source = State()
    admin_add_provider = State()
    admin_provider_token = State()
    admin_provider_model = State()
    admin_channel_input = State()
    admin_automation_setting = State()

# ============================================================
# بخش کیبوردهای ربات (Keyboards)
# ============================================================
FOLDER_NAMES = {
    "cyber": "🔒 امنیت سایبری",
    "tech": "💻 تکنولوژی و فناوری",
    "ai": "🧠 هوش مصنوعی",
    "edu": "📚 آموزش"
}

def get_main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🤖 هوش مصنوعی", callback_data="user_ai"), InlineKeyboardButton(text="💾 ذخیره‌های من", callback_data="user_saves")],
        [InlineKeyboardButton(text="👤 پروفایل", callback_data="user_profile"), InlineKeyboardButton(text="❓ راهنما", callback_data="user_help")],
        [InlineKeyboardButton(text="📞 ارتباط با مدیریت", callback_data="user_contact")]
    ])


def get_admin_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📰 اتوماسیون محتوا", callback_data="admin_automation")],
        [InlineKeyboardButton(text="📁 مدیریت محتوای هسته", callback_data="admin_content")],
        [InlineKeyboardButton(text="📢 ارسال همگانی", callback_data="admin_broadcast"),
         InlineKeyboardButton(text="➕ افزودن پست", callback_data="admin_add_post")],
        [InlineKeyboardButton(text="📊 آمار کلی", callback_data="admin_stats")],
        [InlineKeyboardButton(text="👤 حالت کاربری", callback_data="admin_user_mode")]
    ])


def get_admin_back_kb(target="admin_home") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 بازگشت", callback_data=target)]])

def get_exit_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ لغو و بازگشت", callback_data="cancel_state")]])

def get_folder_selection_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text=FOLDER_NAMES["cyber"], callback_data="f_view_cyber"),
                InlineKeyboardButton(text=FOLDER_NAMES["tech"], callback_data="f_view_tech")
            ],
            [
                InlineKeyboardButton(text=FOLDER_NAMES["ai"], callback_data="f_view_ai"),
                InlineKeyboardButton(text=FOLDER_NAMES["edu"], callback_data="f_view_edu")
            ]
        ]
    )

def get_save_to_folder_kb(post_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text=FOLDER_NAMES["cyber"], callback_data=f"fsave_{post_id}_cyber"),
                InlineKeyboardButton(text=FOLDER_NAMES["tech"], callback_data=f"fsave_{post_id}_tech")
            ],
            [
                InlineKeyboardButton(text=FOLDER_NAMES["ai"], callback_data=f"fsave_{post_id}_ai"),
                InlineKeyboardButton(text=FOLDER_NAMES["edu"], callback_data=f"fsave_{post_id}_edu")
            ]
        ]
    )

def get_post_inline_kb(post_id: int, likes: int, dislikes: int, is_saved: bool) -> InlineKeyboardMarkup:
    save_text = "❌ حذف از ذخیره‌ها" if is_saved else "💾 ذخیره"
    save_cb = f"unsave_{post_id}" if is_saved else f"save_{post_id}"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text=f"👍 {likes}", callback_data=f"like_{post_id}"),
                InlineKeyboardButton(text=f"👎 {dislikes}", callback_data=f"dis_{post_id}")
            ],
            [
                InlineKeyboardButton(text=save_text, callback_data=save_cb)
            ]
        ]
    )

def get_saved_folder_pagination_kb(post_id: int, folder: str, index: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🗑️ حذف", callback_data=f"ask_del_{post_id}_{folder}")],
            [
                InlineKeyboardButton(text="⏮ قبلی", callback_data=f"fpg_prev_{folder}_{index}"),
                InlineKeyboardButton(text="⏭ بعدی", callback_data=f"fpg_next_{folder}_{index}")
            ],
            [InlineKeyboardButton(text="🔍 جستجو", callback_data=f"f_srch_{folder}")]
        ]
    )

def get_saved_folder_search_pagination_kb(post_id: int, folder: str, index: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🗑️ حذف", callback_data=f"ask_del_{post_id}_{folder}")],
            [
                InlineKeyboardButton(text="⏮ قبلی", callback_data=f"fspg_prev_{folder}_{index}"),
                InlineKeyboardButton(text="⏭ بعدی", callback_data=f"fspg_next_{folder}_{index}")
            ],
            [InlineKeyboardButton(text="🔍 جستجوی مجدد", callback_data=f"f_srch_{folder}")]
        ]
    )

def get_confirm_delete_kb(post_id: int, folder: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🗑️ بله، حذفش کن", callback_data=f"f_del_save_{post_id}_{folder}")],
            [InlineKeyboardButton(text="🔙 نه، پشیمون شدم", callback_data=f"cancel_delete_{folder}")]
        ]
    )

def get_confirm_add_post_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ بله، ثبتش کن!", callback_data="conf_add_yes"),
                InlineKeyboardButton(text="❌ خیر، بیخیال شو", callback_data="conf_add_no")
            ]
        ]
    )

def get_confirm_broadcast_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🚀 بله، ارسال همگانی شود!", callback_data="conf_broad_yes"),
                InlineKeyboardButton(text="❌ لغو", callback_data="conf_broad_no")
            ]
        ]
    )

def get_content_management_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 جستجو", callback_data="adm_search_text"),
         InlineKeyboardButton(text="📋 همه محتواها", callback_data="adm_view_all")],
        [InlineKeyboardButton(text="🔙 بازگشت", callback_data="admin_home")]
    ])

def get_admin_search_pagination_kb(post_id: int, index: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏮ قبلی", callback_data=f"asearch_prev_{index}"),
         InlineKeyboardButton(text="⏭ بعدی", callback_data=f"asearch_next_{index}")],
        [InlineKeyboardButton(text="✏️ ویرایش", callback_data=f"aedit_{post_id}"),
         InlineKeyboardButton(text="📊 آمار", callback_data=f"astats_{post_id}")],
        [InlineKeyboardButton(text="🗑️ حذف", callback_data=f"adelete_{post_id}"),
         InlineKeyboardButton(text="🔙 مدیریت محتوا", callback_data="admin_content")]
    ])

def get_admin_all_posts_kb(posts: list, page: int, total_pages: int) -> InlineKeyboardMarkup:
    rows=[]
    for p in posts:
        rows.append([
            InlineKeyboardButton(text=f"✏️ #{p['id']}",callback_data=f"aedit_{p['id']}"),
            InlineKeyboardButton(text=f"🗑 #{p['id']}",callback_data=f"adelete_{p['id']}")
        ])
    rows.append([
        InlineKeyboardButton(text="⏮ قبلی",callback_data=f"adm_all_page_prev_{page}"),
        InlineKeyboardButton(text=f"{page+1}/{total_pages}",callback_data="noop"),
        InlineKeyboardButton(text="⏭ بعدی",callback_data=f"adm_all_page_next_{page}")
    ])
    rows.append([InlineKeyboardButton(text="🔙 مدیریت محتوا",callback_data="admin_content")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def get_admin_view_all_confirm_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ نمایش",callback_data="adm_view_all_confirm")],
        [InlineKeyboardButton(text="❌ لغو",callback_data="adm_view_all_cancel")]
    ])

def get_help_more_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💡 بیشتر بهم توضیح بده", callback_data="help_more")]
        ]
    )

def get_help_got_it_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🤓 متوجه شدم!", callback_data="help_got_it")]
        ]
    )

# ============================================================
# بخش‌های کمکی هوش مصنوعی و منطقه زمانی (AI & Zone Utilities)
# ============================================================
def get_tehran_date() -> str:
    tehran_tz = pytz.timezone("Asia/Tehran")
    now_tehran = datetime.now(tehran_tz)
    return now_tehran.strftime("%Y-%m-%d")

async def download_telegram_file_text(bot: Bot, file_id: str) -> str:
    file_info = await bot.get_file(file_id)
    dest = io.BytesIO()
    await bot.download_file(file_info.file_path, destination=dest)
    dest.seek(0)
    text = dest.read().decode('utf-8', errors='ignore')
    if len(text) > 15000:
        text = text[:15000] + "\n\n[حجم فایل زیاد بود، بخشی از آن بررسی شد]"
    return text

async def call_ai_with_history(url: str, api_key: str, model: str, messages: List[Dict[str, str]]) -> Dict[str, Any]:
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": model,
        "messages": messages
    }
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(url, headers=headers, json=payload) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    return {
                        "content": f"❌ خطا در پاسخ هوش مصنوعی (کد {resp.status}):\n`{text}`",
                        "tokens": 0
                    }
                
                data = await resp.json()
                if "error" in data:
                    err_msg = data["error"].get("message", str(data["error"]))
                    return {
                        "content": f"❌ خطا:\n`{err_msg}`",
                        "tokens": 0
                    }
                
                if "choices" in data and len(data["choices"]) > 0:
                    content = data["choices"][0]["message"]["content"]
                    total_tokens = data.get("usage", {}).get("total_tokens", math.ceil(len(content) / 4))
                    return {
                        "content": content,
                        "tokens": total_tokens
                    }
                return {
                    "content": f"⚠️ پاسخی دریافت نشد:\n`{data}`",
                    "tokens": 0
                }
        except Exception as e:
            return {
                "content": f"❌ خطای ارتباط با سرور هوش مصنوعی: {str(e)}",
                "tokens": 0
            }

# ============================================================
# میدل‌ور ممانعت از اسپم (Rate Limiter Middleware)
# ============================================================
FUNNY_MESSAGES = [
    "آروم‌تر قهرمان! 🏎️",
    "دکمه‌ها گناه دارن، یواش‌تر! 🥺",
    "اسپم نکن مشتی، یکم استراحت کن ☕",
    "سرعتت زیاده! یواش‌تر بران 🛑",
    "آروم‌تر بکوب رو دکمه‌ها دوست من! 🛠️"
]

class RateLimitMiddleware(BaseMiddleware):
    def __init__(self, admin_id: int):
        super().__init__()
        self.admin_id = admin_id
        self.rate_limit_map = {}

    async def __call__(self, handler, event: TelegramObject, data: dict):
        user_id = None
        if isinstance(event, Message) and event.from_user:
            user_id = event.from_user.id
        elif isinstance(event, CallbackQuery) and event.from_user:
            user_id = event.from_user.id
            
        if user_id and user_id != self.admin_id:
            now = time.time()
            last_active = self.rate_limit_map.get(user_id, 0.0)
            if now - last_active < 1.0:
                msg = random.choice(FUNNY_MESSAGES)
                if isinstance(event, Message):
                    await event.answer(msg)
                elif isinstance(event, CallbackQuery):
                    await event.answer(msg, show_alert=True)
                return
            self.rate_limit_map[user_id] = now
            
        return await handler(event, data)

# ============================================================
# ثبت هندلرهای ربات (Telegram Event Handlers)
# ============================================================
router = Router()

async def register_user_if_not_exists(db: D1Database, user_id: int):
    sql = "INSERT OR IGNORE INTO users(id, joined_at) VALUES(?, ?)"
    await db.execute(sql, [user_id, datetime.now(timezone.utc).isoformat()])

async def send_post_content(bot: Bot, chat_id: int, post: dict, reply_markup=None):
    text = post.get("text") or ""
    file_id = post.get("file_id")
    media_type = post.get("media_type")
    
    caption = text if len(text) <= 1024 else text[:1020] + "..."
    
    try:
        if media_type == "photo" and file_id:
            return await bot.send_photo(chat_id=chat_id, photo=file_id, caption=caption, reply_markup=reply_markup)
        elif media_type == "document" and file_id:
            return await bot.send_document(chat_id=chat_id, document=file_id, caption=caption, reply_markup=reply_markup)
        elif media_type == "video" and file_id:
            return await bot.send_video(chat_id=chat_id, video=file_id, caption=caption, reply_markup=reply_markup)
        elif media_type == "audio" and file_id:
            return await bot.send_audio(chat_id=chat_id, audio=file_id, caption=caption, reply_markup=reply_markup)
        else:
            safe_text = text if len(text) <= 4096 else text[:4090] + "..."
            return await bot.send_message(chat_id=chat_id, text=safe_text or "محتوای ارسالی", reply_markup=reply_markup)
    except Exception as e:
        logger.error(f"Error sending post content: {e}")
        return None

# هندلرهای دستورات اصلی که باید بالاتر از بقیه هندلرهای متنی باشند
@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext, db: D1Database, bot: Bot):
    user_id = message.from_user.id
    await register_user_if_not_exists(db, user_id)
    await state.set_state(BotStates.idle)
    state_data = await state.get_data()
    
    args = message.text.split()
    if len(args) > 1:
        deep_arg = args[1]
        if deep_arg.startswith("auto_"):
            token = deep_arg[5:]
            article_rows = await db.execute("SELECT * FROM articles WHERE deep_token=? AND status IN ('ready','published')", [token])
            if article_rows:
                article = article_rows[0]
                body_html=sanitize_telegram_html(article.get('body') or '')
                title_html=f"<b>{html.escape(article.get('title') or '')}</b>"
                source_html=f"<a href=\"{html.escape(article.get('source_url') or '',quote=True)}\">🔗 منبع اصلی</a>"
                full_text=f"{title_html}\n\n{body_html}\n\n{source_html}"
                await db.execute("UPDATE posts SET views = views + 1 WHERE id = (SELECT id FROM posts WHERE text LIKE ? LIMIT 1)", [f"%{article.get('title') or ''}%"])
                image=article.get('image_url') or ''
                sent_photo=False
                if image:
                    try:
                        first=full_text[:1000]
                        await bot.send_photo(message.chat.id,photo=image,caption=first,parse_mode="HTML")
                        sent_photo=True
                        rest=full_text[1000:]
                        if rest: 
                            for i in range(0,len(rest),3800): await message.answer(rest[i:i+3800],parse_mode="HTML",disable_web_page_preview=True)
                    except Exception: sent_photo=False
                if not sent_photo:
                    try:
                        await bot.send_photo(message.chat.id,photo=BufferedInputFile(make_article_png(),filename="tech-content-card.png"),caption=full_text[:1000],parse_mode="HTML")
                        rest=full_text[1000:]
                        if rest:
                            for i in range(0,len(rest),3800): await message.answer(rest[i:i+3800],parse_mode="HTML",disable_web_page_preview=True)
                    except Exception:
                        for i in range(0,len(full_text),3800): await message.answer(full_text[i:i+3800],parse_mode="HTML",disable_web_page_preview=True)
                admin_mode = state_data.get("admin_mode", "user")
                await message.answer("👇 منوی اصلی:", reply_markup=get_admin_menu() if (user_id == ADMIN_ID and admin_mode != "user") else get_main_menu())
                return
            await message.answer("❌ این مقاله دیگر در دسترس نیست یا لینک منقضی شده است.")
            return
        post_id_str = deep_arg
        if post_id_str.isdigit():
            post_id = int(post_id_str)
            post_rows = await db.execute(
                "SELECT text, file_id, media_type, likes, dislikes FROM posts WHERE id = ? AND deleted = 0",
                [post_id]
            )
            if post_rows:
                post = post_rows[0]
                await db.execute("UPDATE posts SET views = views + 1 WHERE id = ?", [post_id])
                save_rows = await db.execute("SELECT folder FROM saves WHERE user = ? AND post = ?", [user_id, post_id])
                is_saved = len(save_rows) > 0
                
                kb = get_post_inline_kb(post_id, post.get("likes", 0), post.get("dislikes", 0), is_saved)
                await send_post_content(bot, message.chat.id, post, kb)
                
                admin_mode = state_data.get("admin_mode", "user")
                menu = get_admin_menu() if (user_id == ADMIN_ID and admin_mode != "user") else get_main_menu()
                await message.answer("👇 منوی اصلی:", reply_markup=menu)
                return
            else:
                await message.answer("❌ این پست یافت نشد یا حذف شده است.")
                return

    first_name = message.from_user.first_name or "دوست عزیز"
    welcomes = [
        f"سلام {first_name} عزیز! 👋 خیلی خوش اومدی. وقت کاوش تو دنیای تکنولوژیه! 🚀",
        f"درود {first_name}! 🌟 خوشحالیم که اینجایی. آماده‌ای برای مطالب جذاب؟ 📚",
        f"سلام {first_name} جان! 🤖 به پایگاه دانش ما خوش اومدی. بزن بریم که کلی مطلب خفن داریم! 🔥"
    ]
    welcome_text = random.choice(welcomes) + "\n\nاز دکمه های پایین استفاده کنید👇🏻"
    
    admin_mode = state_data.get("admin_mode", "user")
    menu = get_admin_menu() if (user_id == ADMIN_ID and admin_mode != "user") else get_main_menu()
    await message.answer(welcome_text, reply_markup=menu)

@router.message(Command("setup_db"))
async def cmd_setup_db(message: Message, db: D1Database):
    if message.from_user.id == ADMIN_ID:
        try:
            await initialize_database(db)
            await message.answer("✅ Database setup completed successfully.")
        except Exception as e:
            await message.answer(f"❌ Error: {str(e)}")

@router.message(Command("reset_db"))
async def cmd_reset_db(message: Message, db: D1Database):
    if message.from_user.id == ADMIN_ID:
        try:
            await reset_database(db)
            await message.answer("✅ Database reset successfully!")
        except Exception as e:
            await message.answer(f"❌ Error: {str(e)}")

@router.message(F.text == "❌ خروج از نشست")
async def cmd_exit_session(message: Message, state: FSMContext):
    data = await state.get_data()
    admin_mode = data.get("admin_mode", "user")
    
    clean_data = {
        "admin_mode": admin_mode,
        "search_count": data.get("search_count", 0),
        "search_window_start": data.get("search_window_start", 0)
    }
    await state.set_state(BotStates.idle)
    await state.set_data(clean_data)
    
    menu = get_admin_menu() if (message.from_user.id == ADMIN_ID and admin_mode != "user") else get_main_menu()
    await message.answer("🚪 خروج از نشست با موفقیت انجام شد!\n", reply_markup=menu)

# ============================================================
# هندلرهای مبتنی بر وضعیت فعال (FSM Messages) - اولویت بالا
# ============================================================
@router.message(StateFilter(BotStates.ai_chat))
async def process_ai_chat(message: Message, state: FSMContext, db: D1Database, bot: Bot):
    user_id = message.from_user.id
    
    providers = await db.execute("SELECT id FROM ai_providers WHERE enabled=1 AND (status IS NULL OR status != 'invalid') LIMIT 1")
    if not providers:
        await message.answer("⚠️ هنوز هیچ مدل فعالی در پنل هوش مصنوعی تنظیم نشده است. از مدیریت، مدل را اضافه و تست کن.")
        return

    today_tehran = get_tehran_date()
    user_rows = await db.execute("SELECT tokens_used, last_reset_date FROM users WHERE id = ?", [user_id])
    
    tokens_used = 0
    last_reset = ""
    if user_rows:
        tokens_used = user_rows[0].get("tokens_used") or 0
        last_reset = user_rows[0].get("last_reset_date") or ""
        
    if last_reset != today_tehran:
        tokens_used = 0
        last_reset = today_tehran
        await db.execute("UPDATE users SET tokens_used = 0, last_reset_date = ? WHERE id = ?", [today_tehran, user_id])
        
    if tokens_used >= 10000:
        await message.answer(
            "⛔ سهمیه ۱۰۰۰۰ توکن شما برای امروز به پایان رسیده است.\n\n⏱️ سهمیه شما ساعت ۰۰:۰۰ بامداد فردا مجدداً فعال خواهد شد. فردا در خدمت شما هستیم! 🔄"
        )
        return

    user_prompt = ""
    if message.text:
        user_prompt = message.text
    elif message.document:
        await message.answer("⏳ در حال خواندن فایل متنی شما...")
        try:
            file_content = await download_telegram_file_text(bot, message.document.file_id)
            caption = f"\nتوضیحات: {message.caption}" if message.caption else ""
            user_prompt = f"لطفاً این فایل را بررسی کن:\n\n```\n{file_content}\n```{caption}"
        except Exception as e:
            await message.answer(f"⚠️ خطا در خواندن فایل:\n{str(e)}")
            return
    else:
        await message.answer("⚠️ لطفاً یک متن یا فایل متنی معتبر ارسال کنید.")
        return

    await bot.send_chat_action(chat_id=message.chat.id, action="typing")
    
    state_data = await state.get_data()
    history = state_data.get("ai_history", [
        {"role": "system", "content": "You are a helpful assistant. Reply clearly in Persian."}
    ])
    history.append({"role": "user", "content": user_prompt})
    
    if len(history) > 11:
        history = [history[0]] + history[-10:]
        
    ai_manager = AIProviderManager(db, bot)
    try:
        ai_result = await ai_manager.call(history, temperature=0.25, max_tokens=3500, purpose="user_chat")
    finally:
        await ai_manager.close()
    if ai_result.get("error") and not ai_result.get("content"):
        await message.answer("⚠️ هیچ مدل فعالی پاسخ نداد.\n\n" + html.escape(ai_result.get("error", "خطای نامشخص"))[:1800])
        return
    
    history.append({"role": "assistant", "content": ai_result["content"]})
    await state.update_data(ai_history=history)
    
    response_text = ai_result["content"]
    max_length = 3900
    for i in range(0, len(response_text), max_length):
        chunk = response_text[i:i+max_length]
        try:
            await message.answer(chunk, parse_mode="Markdown")
        except Exception:
            await message.answer(chunk)
            
    tokens_used += ai_result["tokens"]
    await db.execute("UPDATE users SET tokens_used = ?, last_reset_date = ? WHERE id = ?", [tokens_used, today_tehran, user_id])

@router.message(StateFilter(BotStates.user_chat_admin))
async def process_user_chat_admin(message: Message, state: FSMContext, bot: Bot):
    user_id = message.from_user.id
    if user_id == ADMIN_ID:
        return
        
    hashtag = f"#User_{user_id}"
    caption = message.caption or ""
    
    if message.photo:
        await bot.send_photo(chat_id=ADMIN_ID, photo=message.photo[-1].file_id, caption=f"پیام جدید:\n{hashtag}\n\n{caption}")
    elif message.document:
        await bot.send_document(chat_id=ADMIN_ID, document=message.document.file_id, caption=f"فایل جدید:\n{hashtag}\n\n{caption}")
    elif message.video:
        await bot.send_video(chat_id=ADMIN_ID, video=message.video.file_id, caption=f"ویدیو جدید:\n{hashtag}\n\n{caption}")
    elif message.audio:
        await bot.send_audio(chat_id=ADMIN_ID, audio=message.audio.file_id, caption=f"صوت جدید:\n{hashtag}\n\n{caption}")
    elif message.text:
        await bot.send_message(chat_id=ADMIN_ID, text=f"پیام جدید:\n{hashtag}\n\n{message.text}")

@router.message(StateFilter(BotStates.waiting_post_content))
async def process_add_post_content(message: Message, state: FSMContext, bot: Bot):
    if message.from_user.id != ADMIN_ID:
        return
        
    file_id, media_type = None, None
    caption = message.text or message.caption or ""
    
    if message.photo:
        file_id = message.photo[-1].file_id
        media_type = "photo"
    elif message.document:
        file_id = message.document.file_id
        media_type = "document"
    elif message.video:
        file_id = message.video.file_id
        media_type = "video"
    elif message.audio:
        file_id = message.audio.file_id
        media_type = "audio"
        
    if not file_id and not caption.strip():
        await message.answer("❌ لطفاً متن یا فایل معتبر ارسال کنید.")
        return
        
    await state.update_data(temp_text=caption, temp_file_id=file_id, temp_media_type=media_type)
    await state.set_state(BotStates.waiting_post_confirm)
    
    post_mock = {"text": caption, "file_id": file_id, "media_type": media_type}
    await send_post_content(bot, message.chat.id, post_mock)
    await message.answer("آیا مایلید این محتوا ذخیره گردد؟", reply_markup=get_confirm_add_post_kb())

@router.message(StateFilter(BotStates.waiting_broadcast_content))
async def process_broadcast_content(message: Message, state: FSMContext, bot: Bot):
    if message.from_user.id != ADMIN_ID:
        return
        
    file_id, media_type = None, None
    caption = message.text or message.caption or ""
    
    if message.photo:
        file_id = message.photo[-1].file_id
        media_type = "photo"
    elif message.document:
        file_id = message.document.file_id
        media_type = "document"
    elif message.video:
        file_id = message.video.file_id
        media_type = "video"
    elif message.audio:
        file_id = message.audio.file_id
        media_type = "audio"
        
    if not file_id and not caption.strip():
        await message.answer("❌ لطفاً متن یا فایل معتبر ارسال کنید.")
        return
        
    broadcast_caption = caption + "\n\n#Broadcast"
    await state.update_data(temp_text=broadcast_caption, temp_file_id=file_id, temp_media_type=media_type)
    await state.set_state(BotStates.waiting_broadcast_confirm)
    
    post_mock = {"text": broadcast_caption, "file_id": file_id, "media_type": media_type}
    await send_post_content(bot, message.chat.id, post_mock)
    await message.answer("از ارسال نهایی این پیام به تمامی اعضا مطمئن هستید؟", reply_markup=get_confirm_broadcast_kb())

@router.message(StateFilter(BotStates.user_search_folder))
async def process_user_search_folder(message: Message, state: FSMContext, db: D1Database, bot: Bot):
    query_text = (message.text or "").strip()
    if not query_text:
        return
        
    state_data = await state.get_data()
    folder = state_data.get("folder")
    if not folder:
        await state.set_state(BotStates.idle)
        await message.answer("❌ خطا در پوشه جستجو، لطفاً دوباره از پوشه‌ها وارد شوید.")
        return
        
    now = time.time() * 1000
    WINDOW_MS = 8 * 60 * 60 * 1000
    search_count = state_data.get("search_count", 0)
    window_start = state_data.get("search_window_start", 0)
    
    if now - window_start > WINDOW_MS:
        search_count = 0
        window_start = 0
        
    if search_count >= 5:
        unlock_time_ms = window_start + WINDOW_MS
        tehran_tz = pytz.timezone("Asia/Tehran")
        unlock_dt = datetime.fromtimestamp(unlock_time_ms / 1000, tehran_tz)
        time_str = unlock_dt.strftime("%H:%M")
        day_str = "امروز" if unlock_dt.date() == datetime.now(tehran_tz).date() else "فردا"
        
        await message.answer(f"⏱️ موتور جستجوی اختصاصی شما {day_str} ساعت {time_str} فعال میشه\n\n تا اون موقع می‌تونی دستی پوشه‌هات رو ورق بزنی ! 🕵️‍♂️")
        await state.set_state(BotStates.idle)
        return
        
    if search_count == 0:
        window_start = now
    search_count += 1
    
    await state.update_data(search_count=search_count, search_window_start=window_start)
    
    rows = await db.execute(
        """SELECT posts.id FROM saves JOIN posts ON saves.post = posts.id
           WHERE saves.user = ? AND saves.folder = ? AND posts.text LIKE ? AND posts.deleted = 0
           ORDER BY posts.id DESC LIMIT 30""",
        [message.from_user.id, folder, f"%{query_text}%"]
    )
    
    if not rows:
        await message.answer("❌ محتوایی با این کلمه پیدا نکردم 🫠\nیه کلمه دیگه بفرست تا دوباره بگردم:")
        return
        
    search_ids = [r["id"] for r in rows]
    await state.update_data(search_ids=search_ids, search_index=0)
    await message.answer(f"🎉 {len(search_ids)} تا مطلب با این کلمه پیدا کردم!\n(هر وقت خواستی جستجو رو عوض کنی، کافیه یه کلمه جدید بفرستی 🔄)")
    
    first_post_id = search_ids[0]
    post_rows = await db.execute("SELECT text, file_id, media_type FROM posts WHERE id = ?", [first_post_id])
    if post_rows:
        kb = get_saved_folder_search_pagination_kb(first_post_id, folder, 0)
        await send_post_content(bot, message.chat.id, post_rows[0], kb)

# ============================================================
# هندلرهای کمکی، عمومی و منوهای اصلی (FSM Idle/None)
# ============================================================

# ریپلای ادمین فقط در وضعیت idle یا None قابل استفاده است و با وضعیت‌های فعال تداخلی ندارد
@router.message(F.chat.id == ADMIN_ID, F.reply_to_message, StateFilter(None, BotStates.idle))
async def process_admin_replies(message: Message, state: FSMContext, bot: Bot):
    reply_text = message.reply_to_message.text or message.reply_to_message.caption or ""
    match = re.search(r"#User_(\d+)", reply_text)
    if match:
        target_user = int(match.group(1))
        prefix = "پاسخ مدیریت:\n\n"
        caption = message.caption or ""
        
        try:
            if message.photo:
                await bot.send_photo(chat_id=target_user, photo=message.photo[-1].file_id, caption=f"{prefix}{caption}")
            elif message.document:
                await bot.send_document(chat_id=target_user, document=message.document.file_id, caption=f"{prefix}{caption}")
            elif message.video:
                await bot.send_video(chat_id=target_user, video=message.video.file_id, caption=f"{prefix}{caption}")
            elif message.audio:
                await bot.send_audio(chat_id=target_user, audio=message.audio.file_id, caption=f"{prefix}{caption}")
            elif message.text:
                await bot.send_message(chat_id=target_user, text=f"{prefix}{message.text}")
            await message.answer("✅ پاسخ شما با موفقیت ارسال شد.")
        except Exception as e:
            await message.answer(f"❌ خطا در ارسال پیام به کاربر: {e}")

COMMANDS_LIST = [
    "کاربر", "مدیریت", "🤖 هوش مصنوعی", "💾 ذخیره‌های من", "📞 ارتباط با مدیریت",
    "❓ راهنما", "👤 پروفایل", "➕ افزودن پست", "📁 مدیریت محتوا",
    "📊 آمار", "📢 ارسال همگانی", "⚙️ اتوماسیون محتوا"
]

@router.message(F.text.in_(COMMANDS_LIST), StateFilter(None, BotStates.idle))
async def intercept_global_commands(message: Message, state: FSMContext, db: D1Database):
    text = message.text
    user_id = message.from_user.id
    state_data = await state.get_data()
    
    if text == "🤖 هوش مصنوعی":
        history = [{"role": "system", "content": "You are a helpful assistant. Reply clearly in Persian."}]
        await state.set_state(BotStates.ai_chat)
        await state.update_data(ai_history=history)
        await message.answer(
            "سلام من هوش مصنوعی TechNowAi هستم 🦾\nچطور میتونم کمکت کنم ❔",
            reply_markup=get_exit_menu()
        )
        
    elif text == "کاربر":
        await state.update_data(admin_mode="user")
        await message.answer("✅ فاز کاربری فعال شد.", reply_markup=get_main_menu())
        
    elif text == "مدیریت":
        if user_id == ADMIN_ID:
            await state.update_data(admin_mode="admin")
            await message.answer("✅ پنل مدیریت فعال شد.", reply_markup=get_admin_menu())
        else:
            await message.answer("⛔ شما دسترسی مدیریت ندارید.")
            
    elif text == "❓ راهنما":
        first_name = message.from_user.first_name or "دوست"
        help_text = f""" خب {first_name} جان ببین 👀

اینجا فقط یه ابزار ساده نیست، یه دستیار شخصیه که بهت کمک می‌کنه پست‌های طولانی و جذاب کانال @TechNowAi رو خیلی راحت و بدون دردسر بخونی. 🤓

وقتی تو کانال یه مطلب توجهت رو جلب می‌کنه، با زدن روی لینک مستقیم میای اینجا تا هم متن کاملش رو با تمرکز مطالعه کنی و هم اگه دوست داشتی تو آرشیو شخصیت نگهش داری! 📚✨

برای اینکه دقیق‌تر بدونی چطور می‌تونی از همه امکانات استفاده کنی، دکمه پایین رو لمس کن 👇"""
        await message.answer(help_text, reply_markup=get_help_more_kb())
        
    elif text == "👤 پروفایل":
        rows = await db.execute("SELECT joined_at, role FROM users WHERE id = ?", [user_id])
        joined_str = rows[0].get("joined_at") if rows else None
        user_role_db = rows[0].get("role") if rows else "user"
        
        time_string = "🌱 وضعیت عضویت: تازه وارد"
        join_date_line = ""
        
        if joined_str:
            try:
                joined_dt = datetime.fromisoformat(joined_str).replace(tzinfo=timezone.utc)
                delta = datetime.now(timezone.utc) - joined_dt
                days = delta.days
                hours = int(delta.seconds / 3600)
                time_string = f"⏱️ مدت همراهی: {days} روز و {hours} ساعت ⏳"
                
                tehran_tz = pytz.timezone("Asia/Tehran")
                joined_tehran = joined_dt.astimezone(tehran_tz)
                date_str = joined_tehran.strftime("%Y/%m/%d ساعت %H:%M")
                join_date_line = f"📅 تاریخ عضویت: {date_str}\n"
            except Exception:
                pass
                
        saves_count = (await db.execute("SELECT COUNT(*) as c FROM saves WHERE user = ?", [user_id]))[0].get("c", 0)
        likes_count = (await db.execute("SELECT COUNT(*) as c FROM votes WHERE user_id = ? AND vote_type = 'like'", [user_id]))[0].get("c", 0)
        dislikes_count = (await db.execute("SELECT COUNT(*) as c FROM votes WHERE user_id = ? AND vote_type = 'dislike'", [user_id]))[0].get("c", 0)
        
        role_display = "مدیر 🌟" if user_role_db == "admin" else "کاربر عادی 🟢"
        first_name_clean = message.from_user.first_name or "عزیز"
        profile_text = f"""👤 پروفایل {first_name_clean} عزیز

{join_date_line}{time_string}
💾 مطالب ذخیره‌شده: {saves_count}
👍 لایک‌های شما: {likes_count}
👎 دیس‌لایک‌های شما: {dislikes_count}

🔰 سطح حساب: {role_display}
🔰 سطح ویژه به زودی ..."""
        await message.answer(profile_text)
        
    elif text == "💾 ذخیره‌های من":
        await message.answer("📂 کدوم پوشه رو میخوای باز کنی؟ 👇", reply_markup=get_folder_selection_kb())
        
    elif text == "📞 ارتباط با مدیریت":
        await state.set_state(BotStates.user_chat_admin)
        await message.answer(
            "🛡️ ارتباط امن و ناشناس با مدیریت برقرار شد!\n\nهر پیامی داری همین الان بفرست ...",
            reply_markup=get_exit_menu()
        )
        
    elif text == "➕ افزودن پست":
        if user_id == ADMIN_ID:
            await state.set_state(BotStates.waiting_post_content)
            await message.answer("📝 لطفاً متن، تصویر، ویدیو یا سند جدید خود را ارسال کنید:", reply_markup=get_exit_menu())
        else:
            await message.answer("⛔ شما دسترسی مدیریت ندارید.")
            
    elif text == "📁 مدیریت محتوا":
        if user_id == ADMIN_ID:
            await message.answer("📂 انتخاب کنید:", reply_markup=get_content_management_kb())
        else:
            await message.answer("⛔ شما دسترسی مدیریت ندارید.")
            
    elif text == "📊 آمار":
        if user_id == ADMIN_ID:
            total_users = (await db.execute("SELECT COUNT(*) as c FROM users"))[0].get("c", 0)
            total_likes = (await db.execute("SELECT SUM(likes) as s FROM posts"))[0].get("s") or 0
            total_views = (await db.execute("SELECT SUM(views) as s FROM posts"))[0].get("s") or 0
            active_posts = (await db.execute("SELECT COUNT(*) as c FROM posts WHERE deleted = 0"))[0].get("c", 0)
            total_posts = (await db.execute("SELECT COUNT(*) as c FROM posts"))[0].get("c", 0)
            
            stat_text = f"""📊 آمار کلی ربات:

👥 کل کاربران: {total_users} نفر
📝 کل پست‌ها: {total_posts}
📄 پست‌های فعال: {active_posts}
👁️ مجموع بازدید: {total_views}
👍 مجموع لایک‌ها: {total_likes}"""
            await message.answer(stat_text)
        else:
            await message.answer("⛔ شما دسترسی مدیریت ندارید.")
            
    elif text == "📢 ارسال همگانی":
        if user_id == ADMIN_ID:
            await state.set_state(BotStates.waiting_broadcast_content)
            await message.answer("📢 پیام همگانی خود را بفرستید (متن، عکس، ویدیو یا سند):", reply_markup=get_exit_menu())
        else:
            await message.answer("⛔ شما دسترسی مدیریت ندارید.")
    elif text == "⚙️ اتوماسیون محتوا":
        if user_id == ADMIN_ID:
            enabled = (await get_setting(db, "automation_enabled", "0")) == "1"
            report = await automation_report(db)
            await message.answer(report, reply_markup=automation_menu_kb(enabled))
        else:
            await message.answer("⛔ شما دسترسی مدیریت ندارید.")

@router.message(StateFilter(None, BotStates.idle))
async def process_unknown_commands(message: Message, state: FSMContext):
    data=await state.get_data()
    if message.from_user.id==ADMIN_ID and data.get("admin_mode")=="admin":
        await message.answer("❌ دستور نامعتبر است. از منوی همین بخش استفاده کن.",reply_markup=get_admin_menu())
    else:
        await message.answer("❌ دستور نامعتبر است. از منوی همین بخش استفاده کن.",reply_markup=get_main_menu())


# ============================================================
# پنل مدیریت اتوماسیون محتوا
# ============================================================

@router.message(F.chat.id == ADMIN_ID, StateFilter(BotStates.admin_add_source))
async def admin_add_source_input(message: Message, state: FSMContext, db: D1Database, bot: Bot):
    url = (message.text or "").strip()
    if not url or not re.match(r"^https?://", url, re.I):
        await message.answer("❌ URL معتبر نیست. نمونه:\nhttps://example.com",reply_markup=get_exit_menu())
        return
    try:
        source_id = await add_source(db, url)
        data = await state.get_data()
        panel_id = data.get('panel_message_id')
        await state.set_state(BotStates.idle)
        rows = await db.execute("SELECT * FROM sources ORDER BY priority DESC, id DESC")
        if panel_id:
            try:
                await bot.edit_message_text(chat_id=message.chat.id, message_id=panel_id, text=f"✅ منبع اضافه شد.\n\nشناسه: {source_id}", reply_markup=source_list_kb(rows))
                return
            except Exception:
                pass
        await message.answer(f"✅ منبع با موفقیت اضافه شد.\nشناسه: {source_id}", reply_markup=source_list_kb(rows))
    except Exception as e:
        await message.answer(f"❌ افزودن منبع ناموفق بود:\n{html.escape(str(e))}",reply_markup=get_exit_menu())



@router.message(F.chat.id == ADMIN_ID, StateFilter(BotStates.admin_automation_setting))
async def admin_automation_setting_input(message:Message,state:FSMContext,db:D1Database,bot:Bot):
    data=await state.get_data(); key=data.get("automation_setting_key"); value=(message.text or "").strip()
    parent=data.get("parent_callback","admin_home")
    try:
        if key=="__source_interval__":
            sid=int(data["source_interval_id"]); value=str(max(1,int(value)))
            await db.execute("UPDATE sources SET interval_minutes=?,next_check_at=? WHERE id=?",[int(value),datetime.now(timezone.utc).isoformat(),sid])
            parent=f"source_view_{sid}"
        elif key=="__source_priority__":
            sid=int(data["source_priority_id"]); value=str(max(1,int(value)))
            await db.execute("UPDATE sources SET priority=? WHERE id=?",[int(value),sid]); parent=f"source_view_{sid}"
        elif key=="__provider_priority__":
            pid=int(data["provider_priority_id"]); value=str(max(1,int(value)))
            await db.execute("UPDATE ai_providers SET priority=?,updated_at=? WHERE id=?",[int(value),datetime.now(timezone.utc).isoformat(),pid]); parent=f"provider_view_{pid}"
        elif key.startswith("weight_"):
            value=str(max(0,min(100,float(value)))); await set_setting(db,key,value); parent="quality_weights"
        elif key in {"max_daily_posts","default_source_interval"}:
            value=str(max(1,int(value))); await set_setting(db,key,value); parent="auto_schedule"
        elif key in {"max_workers","max_ai_workers"}:
            value=str(max(1,min(4,int(value)))); await set_setting(db,key,value); parent="auto_schedule"
        elif key=="min_content_score":
            value=str(max(0,min(100,float(value)))); await set_setting(db,key,value); parent="auto_quality"
        elif key=="min_hours_between_posts":
            value=str(max(0,float(value))); await set_setting(db,key,value); parent="auto_schedule"
        elif key=="ai_verify_mode":
            if value not in {"auto","always","off"}: raise ValueError("auto / always / off")
            await set_setting(db,key,value); parent="auto_schedule"
        elif key=="__publish_delay__":
            delay=max(0,min(10080,int(value)))
            row=await db.execute("SELECT article_id FROM publication_queue WHERE status='queued' ORDER BY score DESC,created_at ASC LIMIT 1")
            if not row: raise ValueError("صف انتشار خالی است")
            # برنامه‌ریزی دستی فقط همان اولین آیتم آماده را جابه‌جا می‌کند.
            when=(datetime.now(timezone.utc)+timedelta(minutes=delay)).isoformat()
            await db.execute("UPDATE publication_queue SET scheduled_at=? WHERE article_id=? AND status='queued'",[when,row[0]['article_id']])
            parent="auto_channel"
        else:
            raise ValueError("setting not supported")
        await state.set_state(BotStates.idle)

        # Render the parent menu back into the original panel message.
        panel_id=data.get("panel_message_id")
        if panel_id:
            if parent=="auto_schedule":
                await bot.edit_message_text(chat_id=message.chat.id,message_id=panel_id,
                    text=(await automation_report(db))+"\n\n⏱ <b>برنامه انتشار</b>",parse_mode="HTML",reply_markup=schedule_menu_kb())
            elif parent=="auto_quality":
                score=await get_setting(db,"min_content_score",str(DEFAULT_MIN_CONTENT_SCORE))
                await bot.edit_message_text(chat_id=message.chat.id,message_id=panel_id,
                    text=f"🧠 <b>کیفیت محتوا</b>\n\nحداقل امتیاز: <b>{html.escape(score)}</b>",
                    parse_mode="HTML",reply_markup=quality_menu_kb())
            elif parent=="quality_weights":
                labels=[("global","🌍 جهانی"),("technology","💻 فناوری"),("ai","🤖 AI"),("cyber","🔐 سایبری"),("education","📚 آموزش"),("iran","🇮🇷 ایران/فارسی"),("freshness","🆕 تازگی"),("source","✅ منبع"),("novelty","♻️ عدم تکرار")]
                text="🎯 <b>وزن معیارها</b>\n\n"+ "\n".join([f"{lab}: <b>{await get_setting(db,'weight_'+k,'10')}</b>" for k,lab in labels])
                rows=[[InlineKeyboardButton(text=lab,callback_data="weight_"+k)] for k,lab in labels]
                rows.append([InlineKeyboardButton(text="🔙 کیفیت محتوا",callback_data="auto_quality")])
                await bot.edit_message_text(chat_id=message.chat.id,message_id=panel_id,text=text,parse_mode="HTML",reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
            elif parent.startswith("source_view_"):
                sid=int(parent.rsplit("_",1)[1]); await bot.edit_message_text(chat_id=message.chat.id,message_id=panel_id,text="✅ تنظیم منبع ذخیره شد.",reply_markup=get_admin_back_kb(parent))
            elif parent.startswith("provider_view_"):
                pid=int(parent.rsplit("_",1)[1]); await bot.edit_message_text(chat_id=message.chat.id,message_id=panel_id,text="✅ تنظیم مدل ذخیره شد.",reply_markup=get_admin_back_kb(parent))
            elif parent=="auto_channel":
                fake=type("FakeCall",(),{"message":type("FakeMessage",(),{"edit_text":lambda self,*a,**kw: None})()})()
                # پنل اصلی را در همان پیام با داده فعلی بازسازی می‌کنیم.
                channel_id=await get_channel_id(db); channel_username=await get_setting(db,'channel_username',''); shown=html.escape(channel_username) if channel_username else ('✅ کانال خصوصی تنظیم شده' if channel_id else '⛔ تنظیم نشده')
                enabled=(await get_setting(db,'automation_enabled','0'))=='1'
                text='📢 <b>کانال و انتشار</b>\n\n'+f'کانال فعلی: {shown}\nاتوماسیون: {"🟢 فعال" if enabled else "🔴 خاموش"}'
                kb=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text='📢 تنظیم/تغییر کانال',callback_data='auto_channel_set')],[InlineKeyboardButton(text='🚀 همین حالا منتشر کن',callback_data='publish_now')],[InlineKeyboardButton(text='⏱ زمان‌بندی انتشار',callback_data='publish_schedule')],[InlineKeyboardButton(text='🧪 تست کانال',callback_data='channel_test')],[InlineKeyboardButton(text='🔙 اتوماسیون محتوا',callback_data='auto_back')]])
                await bot.edit_message_text(chat_id=message.chat.id,message_id=panel_id,text=text,parse_mode='HTML',reply_markup=kb)
            else:
                await bot.edit_message_text(chat_id=message.chat.id,message_id=panel_id,text="✅ ذخیره شد.",reply_markup=get_admin_menu())
        else:
            await message.answer("✅ ذخیره شد.",reply_markup=get_admin_menu())
    except Exception as e:
        await message.answer(f"❌ مقدار نامعتبر است: {html.escape(str(e))}",reply_markup=get_exit_menu())


async def render_admin_home(call: CallbackQuery, db: D1Database):
    text=("🛠 <b>پنل مدیریت</b>\n<code>Build: "+BUILD_VERSION+"</code>\n\n"
          "اینجا بخش موردنظر را انتخاب کن.\nبرای سلامت، گزارش و انتشار وارد «اتوماسیون محتوا» شو.")
    await call.message.edit_text(text,parse_mode='HTML',reply_markup=get_admin_menu()); await call.answer()


@router.callback_query(F.data == "admin_home")
async def admin_home(call: CallbackQuery, db: D1Database):
    if call.from_user.id != ADMIN_ID:
        await call.answer("دسترسی ندارید", show_alert=True); return
    await render_admin_home(call, db)


@router.callback_query(F.data == "admin_automation")
async def admin_automation(call: CallbackQuery, db: D1Database):
    if call.from_user.id != ADMIN_ID: return
    enabled = (await get_setting(db, 'automation_enabled', '0')) == '1'
    await call.message.edit_text(await automation_report(db), reply_markup=automation_menu_kb(enabled))
    await call.answer()


@router.callback_query(F.data == "admin_ai")
async def admin_ai(call: CallbackQuery, db: D1Database):
    if call.from_user.id != ADMIN_ID: return
    await auto_providers(call, db)


@router.callback_query(F.data == "admin_sources")
async def admin_sources(call: CallbackQuery, db: D1Database):
    if call.from_user.id != ADMIN_ID: return
    await auto_sources(call, db)


@router.callback_query(F.data == "admin_publish")
async def admin_publish(call: CallbackQuery, db: D1Database):
    if call.from_user.id != ADMIN_ID: return
    await render_channel_panel(call, db)

@router.callback_query(F.data == "admin_quality")
async def admin_quality(call: CallbackQuery, db: D1Database):
    if call.from_user.id != ADMIN_ID: return
    await auto_quality(call, db)

@router.callback_query(F.data == "admin_monitor")
async def admin_monitor(call: CallbackQuery, db: D1Database):
    if call.from_user.id != ADMIN_ID: return
    users = (await db.execute("SELECT COUNT(*) c FROM users"))[0].get('c',0)
    posts = (await db.execute("SELECT COUNT(*) c FROM posts WHERE deleted=0"))[0].get('c',0)
    views = (await db.execute("SELECT COALESCE(SUM(views),0) s FROM posts"))[0].get('s',0)
    automation = await automation_report(db)
    text = f"📊 <b>مرکز آمار و سلامت</b>\n\n👥 کاربران: {users}\n📝 محتوای هسته: {posts}\n👁 بازدید: {views}\n\n{automation}"
    await call.message.edit_text(text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text='🔄 بروزرسانی', callback_data='admin_monitor')],[InlineKeyboardButton(text='🔙 پنل اصلی', callback_data='admin_home')]]))
    await call.answer()


@router.callback_query(F.data == "admin_stats")
async def admin_stats(call: CallbackQuery, db: D1Database):
    if call.from_user.id != ADMIN_ID: return
    users=(await db.execute("SELECT COUNT(*) c FROM users"))[0].get("c",0)
    posts=(await db.execute("SELECT COUNT(*) c FROM posts WHERE deleted=0"))[0].get("c",0)
    views=(await db.execute("SELECT COALESCE(SUM(views),0) s FROM posts"))[0].get("s",0)
    await call.message.edit_text(f"📊 <b>آمار کلی</b>\n\n👥 کاربران: {users}\n📝 محتوای فعال: {posts}\n👁 بازدید: {views}",parse_mode="HTML",reply_markup=get_admin_back_kb("admin_home"))
    await call.answer()

@router.callback_query(F.data == "admin_content")
async def admin_content(call: CallbackQuery, db: D1Database):
    if call.from_user.id != ADMIN_ID: return
    await call.message.edit_text("📁 <b>مدیریت محتوای هسته</b>\n\nجستجو، مشاهده و حذف محتوا از آرشیو اصلی.", parse_mode='HTML', reply_markup=get_content_management_kb())
    await call.answer()


@router.callback_query(F.data == "admin_add_post")
async def admin_add_post(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    await state.set_state(BotStates.waiting_post_content)
    await state.update_data(panel_message_id=call.message.message_id)
    await call.message.edit_text("📝 متن، تصویر، ویدیو یا سند پست را ارسال کن:", reply_markup=get_exit_menu())
    await call.answer()


@router.callback_query(F.data == "admin_broadcast")
async def admin_broadcast(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID: return
    await state.set_state(BotStates.waiting_broadcast_content)
    await state.update_data(panel_message_id=call.message.message_id)
    await call.message.edit_text("📢 پیام همگانی را ارسال کن؛ قبل از ارسال نهایی یک مرحله تأیید می‌گیریم.", reply_markup=get_exit_menu())
    await call.answer()


@router.callback_query(F.data == "admin_user_mode")
async def admin_user_mode(call: CallbackQuery, state: FSMContext):
    await state.update_data(admin_mode='user')
    await call.message.edit_text("👤 حالت کاربری فعال شد.", reply_markup=get_main_menu())
    await call.answer()


async def render_channel_panel(call: CallbackQuery, db: D1Database):
    channel_id = await get_channel_id(db)
    channel_username = await get_setting(db, 'channel_username', '')
    if channel_username:
        shown = html.escape(channel_username)
    elif channel_id:
        shown = '✅ کانال خصوصی تنظیم شده (شناسه عددی مخفی)'
    else:
        shown = '⛔ هنوز تنظیم نشده'
    enabled = (await get_setting(db, 'automation_enabled', '0')) == '1'
    text = (
        '📢 <b>کانال و انتشار</b>\n\n'
        f'کانال فعلی: {shown}\n'
        f'اتوماسیون: {"🟢 فعال" if enabled else "🔴 خاموش"}\n\n'
        'برای تنظیم، فقط @username کانال یا لینک t.me آن را بفرست. برای کانال خصوصی، شناسه -100… هم قابل قبول است.\n'
        'ربات باید مدیر کانال باشد و اجازه انتشار پیام داشته باشد.'
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='📢 تنظیم/تغییر کانال', callback_data='auto_channel_set')],
        [InlineKeyboardButton(text='🚀 همین حالا منتشر کن', callback_data='publish_now')],
        [InlineKeyboardButton(text='⏱ زمان‌بندی انتشار', callback_data='publish_schedule')],
        [InlineKeyboardButton(text='🧪 تست کانال', callback_data='channel_test')],
        [InlineKeyboardButton(text='🔙 اتوماسیون محتوا', callback_data='auto_back')]
    ])
    await call.message.edit_text(text, parse_mode='HTML', reply_markup=kb)

@router.callback_query(F.data == "auto_channel")
async def auto_channel(call: CallbackQuery, db: D1Database):
    if call.from_user.id != ADMIN_ID: return
    await render_channel_panel(call, db)
    await call.answer()

@router.callback_query(F.data == "auto_channel_set")
async def auto_channel_set(call: CallbackQuery, state: FSMContext, db: D1Database):
    if call.from_user.id != ADMIN_ID: return
    await state.set_state(BotStates.admin_channel_input)
    await state.update_data(panel_message_id=call.message.message_id)
    await call.message.edit_text(
        '📢 <b>تنظیم کانال انتشار</b>\n\n'
        'آیدی کانال یا @username را ارسال کن.\n'
        'مثال: <code>@my_channel</code> یا <code>-1001234567890</code>\n\n'
        'ربات باید در کانال ادمین باشد و اجازه انتشار پیام داشته باشد.',
        parse_mode='HTML', reply_markup=get_exit_menu())
    await call.answer()

@router.callback_query(F.data == "publish_now")
async def publish_now(call: CallbackQuery, db:D1Database, bot:Bot):
    if call.from_user.id!=ADMIN_ID: return
    await call.answer("🚀 در حال انتشار…")
    try:
        ok=await publish_next_article(db,bot,force=True)
        msg="✅ اولین محتوای آماده همین حالا منتشر شد." if ok else "⏸ محتوای آماده‌ای برای انتشار نیست یا سقف روزانه پر شده است."
    except Exception as e:
        msg="❌ انتشار دستی شکست خورد:\n"+html.escape(str(e)[:1200])
    await call.message.edit_text(msg,parse_mode="HTML",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 کانال و انتشار",callback_data="auto_channel")]]))

@router.callback_query(F.data == "publish_schedule")
async def publish_schedule(call:CallbackQuery,state:FSMContext):
    if call.from_user.id!=ADMIN_ID:return
    await state.set_state(BotStates.admin_automation_setting)
    await state.update_data(automation_setting_key="__publish_delay__",panel_message_id=call.message.message_id,parent_callback="auto_channel")
    await call.message.edit_text("⏱ <b>زمان‌بندی انتشار</b>\n\nچند دقیقه دیگر منتشر شود؟\n\n0 = همین حالا\n2 = دو دقیقه دیگر\n10 = ده دقیقه دیگر\nمثلاً 30",parse_mode="HTML",reply_markup=get_exit_menu()); await call.answer()

@router.callback_query(F.data == "channel_test")
async def channel_test(call: CallbackQuery, db: D1Database, bot: Bot):
    if call.from_user.id != ADMIN_ID: return
    channel_id = await get_channel_id(db)
    if not channel_id:
        await call.answer('کانال هنوز تنظیم نشده است.', show_alert=True); return
    try:
        chat = await bot.get_chat(channel_id)
        me = await bot.get_me()
        member = await bot.get_chat_member(chat.id, me.id)
        status = str(getattr(member, 'status', ''))
        if status not in {'administrator', 'creator'}:
            raise RuntimeError('ربات در کانال ادمین نیست.')
        perms = getattr(member, 'can_post_messages', None)
        if perms is False:
            raise RuntimeError('اجازه انتشار پیام برای ربات فعال نیست.')
        label = '@' + chat.username if getattr(chat, 'username', None) else 'کانال خصوصی'
        await call.message.edit_text(f'✅ <b>کانال سالم است</b>\n\n📢 {html.escape(label)}\n👤 وضعیت ربات: {html.escape(status)}\n📝 اجازه انتشار: ✅', parse_mode='HTML', reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text='🔙 کانال و انتشار', callback_data='auto_channel')]]))
    except Exception as e:
        await call.message.edit_text(f'❌ <b>تست کانال ناموفق بود</b>\n\n{html.escape(str(e)[:1000])}', parse_mode='HTML', reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text='🔙 کانال و انتشار', callback_data='auto_channel')]]))
    await call.answer()

@router.message(F.chat.id == ADMIN_ID, StateFilter(BotStates.admin_channel_input))
async def admin_channel_input(message: Message, state: FSMContext, db: D1Database, bot: Bot):
    raw = (message.text or '').strip()
    if not raw:
        return
    if raw.startswith('https://t.me/') or raw.startswith('http://t.me/'):
        raw = '@' + raw.rstrip('/').split('/')[-1]
    try:
        chat = await bot.get_chat(raw)
        me = await bot.get_me()
        member = await bot.get_chat_member(chat.id, me.id)
        status = str(getattr(member, 'status', ''))
        if status not in {'administrator', 'creator'}:
            await message.answer('❌ ربات در این کانال ادمین نیست یا دسترسی کافی ندارد. ورودی قبلی باقی است؛ دوباره آیدی/username را بفرست.',reply_markup=get_exit_menu())
            return
        await set_setting(db, 'channel_id', str(chat.id))
        await set_setting(db, 'channel_username', '@' + chat.username if getattr(chat, 'username', None) else '')
        await state.set_state(BotStates.idle)
        label = '@' + chat.username if getattr(chat, 'username', None) else 'کانال خصوصی تنظیم شد'
        panel_id=(await state.get_data()).get('panel_message_id')
        text=f"✅ <b>کانال با موفقیت تنظیم شد.</b>\n\n📢 {html.escape(label)}\n🆔 <code>{chat.id}</code>"
        if panel_id:
            try:
                await bot.edit_message_text(chat_id=message.chat.id,message_id=panel_id,text=text,parse_mode='HTML',reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text='🧪 تست کانال',callback_data='channel_test')],[InlineKeyboardButton(text='🔙 کانال و انتشار',callback_data='auto_channel')]]))
            except Exception:
                await message.answer(text,parse_mode='HTML',reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text='🔙 کانال و انتشار',callback_data='auto_channel')]]))
        else:
            await message.answer(text,parse_mode='HTML',reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text='🔙 کانال و انتشار',callback_data='auto_channel')]]))
    except Exception as e:
        await message.answer(f"❌ نتوانستم کانال را تأیید کنم:\n{html.escape(str(e)[:1000])}\n\nآیدی/@username را دوباره بفرست.",reply_markup=get_exit_menu())


@router.callback_query(F.data == "cancel_state")
async def cancel_state(call: CallbackQuery, state: FSMContext, db: D1Database):
    data=await state.get_data(); parent=data.get("parent_callback") or "admin_home"
    await state.set_state(BotStates.idle)
    await state.update_data(panel_message_id=None,provider_base_url=None,provider_token=None,provider_edit_id=None)
    try:
        if parent=="auto_schedule": await auto_schedule(call,db); return
        if parent=="auto_quality": await auto_quality(call,db); return
        if parent=="quality_weights": await quality_weights(call,db); return
        if parent=="auto_channel": await render_channel_panel(call,db); return
        if parent=="auto_providers": await auto_providers(call,db); return
        if parent.startswith("provider_view_"): await provider_view(call,db); return
        if parent.startswith("source_view_"): await source_view(call,db); return
        if parent=="auto_sources": await auto_sources(call,db); return
    except Exception:
        pass
    if call.from_user.id==ADMIN_ID:
        await render_admin_home(call,db)
    else:
        await call.message.edit_text("لغو شد.",reply_markup=get_main_menu()); await call.answer("لغو شد")

@router.callback_query(F.data == "user_home")
async def user_home(call: CallbackQuery):
    await call.message.edit_text("🏠 منوی اصلی کاربر\n\nچه کاری می‌خواهی انجام بدهی؟", reply_markup=get_main_menu())
    await call.answer()


@router.callback_query(F.data == "user_ai")
async def user_ai(call: CallbackQuery, state: FSMContext):
    await state.set_state(BotStates.ai_chat)
    await state.update_data(ai_history=[{"role":"system","content":"You are a helpful assistant. Reply clearly in Persian."}])
    await call.message.edit_text("🤖 هوش مصنوعی آماده است.\n\nسؤالت را بفرست یا فایل متنی ارسال کن.", reply_markup=get_exit_menu())
    await call.answer()


@router.callback_query(F.data == "user_saves")
async def user_saves(call: CallbackQuery):
    await call.message.edit_text("💾 ذخیره‌های من\n\nیک پوشه را انتخاب کن:", reply_markup=get_folder_selection_kb())
    await call.answer()


@router.callback_query(F.data == "user_profile")
async def user_profile(call: CallbackQuery, db: D1Database):
    uid = call.from_user.id
    rows = await db.execute("SELECT joined_at, role FROM users WHERE id=?", [uid])
    joined = rows[0].get('joined_at') if rows else '-'
    saves = (await db.execute("SELECT COUNT(*) c FROM saves WHERE user=?", [uid]))[0].get('c',0)
    likes = (await db.execute("SELECT COUNT(*) c FROM votes WHERE user_id=? AND vote_type='like'", [uid]))[0].get('c',0)
    role = rows[0].get('role') if rows else 'user'
    text = f"👤 پروفایل\n\n📅 عضویت: {html.escape(str(joined))}\n💾 ذخیره‌ها: {saves}\n👍 لایک: {likes}\n🔰 سطح: {'مدیر' if role=='admin' else 'کاربر'}"
    await call.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text='🔙 منوی اصلی', callback_data='user_home')]]))
    await call.answer()


@router.callback_query(F.data == "user_help")
async def user_help(call: CallbackQuery):
    await call.message.edit_text("❓ راهنما\n\nاز دکمه‌های شیشه‌ای برای هوش مصنوعی، ذخیره‌ها و ارتباط با مدیریت استفاده کن.\n\nبرای مقالات کانال، دکمه «📖 بیشتر بخوانید» متن کامل و عمیق‌تر را داخل ربات باز می‌کند.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text='💡 توضیحات بیشتر', callback_data='help_more')],[InlineKeyboardButton(text='🔙 منوی اصلی', callback_data='user_home')]]))
    await call.answer()


@router.callback_query(F.data == "user_contact")
async def user_contact(call: CallbackQuery, state: FSMContext):
    await state.set_state(BotStates.user_chat_admin)
    await call.message.edit_text("📞 پیام خود را برای مدیریت ارسال کن.\n\nپیام تو مستقیم برای مدیر فرستاده می‌شود.", reply_markup=get_exit_menu())
    await call.answer()


@router.callback_query(F.data == "auto_on")
async def auto_on(call: CallbackQuery, db: D1Database):
    await set_setting(db, "automation_enabled", "1")
    await call.message.edit_text((await automation_report(db)), reply_markup=automation_menu_kb(True))
    await call.answer("اتوماسیون فعال شد")


@router.callback_query(F.data == "auto_off")
async def auto_off(call: CallbackQuery, db: D1Database):
    await set_setting(db, "automation_enabled", "0")
    await call.message.edit_text((await automation_report(db)), reply_markup=automation_menu_kb(False))
    await call.answer("اتوماسیون خاموش شد")


@router.callback_query(F.data == "auto_back")
async def auto_back(call: CallbackQuery, db: D1Database):
    enabled = (await get_setting(db, "automation_enabled", "0")) == "1"
    await call.message.edit_text(await automation_report(db), reply_markup=automation_menu_kb(enabled))
    await call.answer()


@router.callback_query(F.data == "auto_report")
async def auto_report(call:CallbackQuery,db:D1Database):
    if call.from_user.id!=ADMIN_ID:return
    await call.message.edit_text(await automation_report(db),reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 بروزرسانی",callback_data="auto_report")],
        [InlineKeyboardButton(text="🔙 اتوماسیون محتوا",callback_data="auto_back")]]))
    await call.answer()


@router.callback_query(F.data == "auto_sources")
async def auto_sources(call: CallbackQuery, db: D1Database):
    rows = await db.execute("SELECT * FROM sources ORDER BY priority DESC, id DESC")
    text = "🌐 منابع محتوا\n\n"
    if not rows:
        text += "هنوز منبع اضافه نشده است."
    else:
        for s in rows[:20]:
            text += f"{'🟢' if s.get('enabled') else '🔴'} #{s.get('id')} {s.get('name')} | {s.get('interval_minutes')}m | {s.get('category')}\n"
    await call.message.edit_text(text, reply_markup=source_list_kb(rows))
    await call.answer()


@router.callback_query(F.data == "auto_add_source")
async def auto_add_source(call: CallbackQuery, state: FSMContext):
    await state.set_state(BotStates.admin_add_source)
    await state.update_data(panel_message_id=call.message.message_id)
    await call.message.edit_text("🌐 URL سایت را بفرست:\n\nمثال: https://example.com", reply_markup=get_exit_menu())
    await call.answer()


@router.callback_query(F.data.startswith("source_view_"))
async def source_view(call: CallbackQuery, db: D1Database):
    source_id = int(call.data.split("_")[-1])
    rows = await db.execute("SELECT * FROM sources WHERE id=?", [source_id])
    if not rows:
        await call.answer("منبع یافت نشد", show_alert=True)
        return
    s = rows[0]
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 تست و بررسی اکنون", callback_data=f"source_test_{source_id}")],
        [InlineKeyboardButton(text="🔢 تغییر اولویت", callback_data=f"source_priority_{source_id}")],
        [InlineKeyboardButton(text="⏱ تنظیم فاصله", callback_data=f"source_interval_{source_id}")],
        [InlineKeyboardButton(text="⏸ غیرفعال" if s.get("enabled") else "▶️ فعال", callback_data=f"source_toggle_{source_id}")],
        [InlineKeyboardButton(text="🗑 حذف", callback_data=f"source_delete_{source_id}")],
        [InlineKeyboardButton(text="🔙 منابع", callback_data="auto_sources")]
    ])
    text = f"🌐 #{s['id']} {s.get('name')}\n\nURL: {s.get('url')}\nدسته: {s.get('category')}\nفاصله: {s.get('interval_minutes')} دقیقه\nاولویت: {s.get('priority')}\nآخرین بررسی: {s.get('last_checked_at') or '-'}\nخطا: {s.get('last_error') or '-'}"
    await call.message.edit_text(text, reply_markup=kb)
    await call.answer()


@router.callback_query(F.data.startswith("source_toggle_"))
async def source_toggle(call: CallbackQuery, db: D1Database):
    source_id = int(call.data.split("_")[-1])
    rows = await db.execute("SELECT enabled FROM sources WHERE id=?", [source_id])
    if rows:
        await db.execute("UPDATE sources SET enabled=? WHERE id=?", [0 if rows[0].get("enabled") else 1, source_id])
    await source_view(call, db)


@router.callback_query(F.data.startswith("source_delete_"))
async def source_delete(call: CallbackQuery, db: D1Database):
    source_id = int(call.data.split("_")[-1])
    await db.execute("DELETE FROM sources WHERE id=?", [source_id])
    await db.execute("DELETE FROM source_items WHERE source_id=?", [source_id])
    rows = await db.execute("SELECT * FROM sources ORDER BY priority DESC, id DESC")
    await call.message.edit_text("✅ منبع حذف شد.", reply_markup=source_list_kb(rows))
    await call.answer("حذف شد")


@router.callback_query(F.data.startswith("source_priority_"))
async def source_priority(call: CallbackQuery, state: FSMContext):
    sid=int(call.data.split("_")[-1])
    await state.set_state(BotStates.admin_automation_setting)
    await state.update_data(automation_setting_key="__source_priority__",source_priority_id=sid,parent_callback=f"source_view_{sid}",panel_message_id=call.message.message_id)
    await call.message.edit_text("🔢 اولویت منبع را عددی بفرست.\nعدد کمتر = اولویت بالاتر.",reply_markup=get_exit_menu())
    await call.answer()

@router.callback_query(F.data.startswith("source_interval_"))
async def source_interval(call: CallbackQuery, state: FSMContext):
    source_id = int(call.data.split("_")[-1])
    await state.update_data(source_interval_id=source_id,panel_message_id=call.message.message_id,parent_callback=f"source_view_{source_id}")
    await state.set_state(BotStates.admin_automation_setting)
    await state.update_data(automation_setting_key="__source_interval__")
    await call.message.edit_text("فاصله بررسی را به دقیقه بفرست. مثلاً 15", reply_markup=get_exit_menu())
    await call.answer()


@router.callback_query(F.data.startswith("source_test_"))
async def source_test(call: CallbackQuery, db: D1Database):
    source_id = int(call.data.split("_")[-1])
    rows = await db.execute("SELECT * FROM sources WHERE id=?", [source_id])
    if not rows:
        await call.answer("منبع یافت نشد", show_alert=True); return
    await call.answer("در حال بررسی...", show_alert=True)
    # برای تست دستی، از یک provider واقعی استفاده می‌کنیم ولی در DB فقط state منبع ثبت می‌شود.
    try:
        diag=await discover_source_items(rows[0],return_diagnostics=True)
        if diag.get("items"):
            text=f"✅ <b>تست منبع موفق بود.</b>\n\nروش: <code>{html.escape(str(diag.get('method')))}</code>\nآیتم قابل بررسی: {len(diag.get('items',[]))}"
        else:
            text="❌ <b>منبع فعلاً قابل دریافت نیست.</b>\n\n"+html.escape(str(diag.get('error','بدون نتیجه'))[:1800])
        await call.message.edit_text(text,parse_mode='HTML',reply_markup=get_admin_back_kb(f"source_view_{source_id}"))
    except Exception as e:
        await db.execute("UPDATE sources SET last_error=? WHERE id=?", [str(e)[:1000], source_id])
        await call.message.edit_text(f"❌ تست منبع ناموفق:\n{html.escape(str(e))}",reply_markup=get_admin_back_kb(f"source_view_{source_id}"))


@router.callback_query(F.data == "auto_providers")
async def auto_providers(call: CallbackQuery, db: D1Database):
    rows = await db.execute("SELECT id,name,base_url,model_name,priority,enabled,status,last_error,last_latency_ms FROM ai_providers ORDER BY priority ASC, id ASC")
    text = "🤖 <b>مدل‌های هوش مصنوعی</b>\n\nهر مدل را باز کن تا ویرایش، تست، فعال/غیرفعال، اولویت‌بندی یا حذفش کنی.\n\n"
    if not rows:
        text += "هیچ Provider فعالی وجود ندارد."
    else:
        for p in rows:
            text += f"{'🟢' if p.get('enabled') else '🔴'} #{p['id']} {p.get('name')} | {p.get('model_name')} | priority={p.get('priority')}\n"
    await call.message.edit_text(text, parse_mode='HTML', reply_markup=provider_list_kb(rows))
    await call.answer()


@router.callback_query(F.data == "auto_add_provider")
async def auto_add_provider(call: CallbackQuery, state: FSMContext):
    await state.set_state(BotStates.admin_add_provider)
    await state.update_data(provider_draft={},parent_callback="auto_providers",panel_message_id=call.message.message_id)
    await call.message.edit_text(
        "🤖 افزودن مدل جدید\n\n"
        "مرحله ۱ از ۳\n"
        "🔗 Base URL خود را ارسال کنید.\n\n"
        "می‌تواند endpoint کامل /chat/completions باشد یا Base URL استاندارد مثل /v1.",
        reply_markup=get_exit_menu()
    )
    await call.answer()


@router.message(F.chat.id == ADMIN_ID, StateFilter(BotStates.admin_add_provider))
async def provider_base_input(message: Message, state: FSMContext, bot: Bot):
    base_url = (message.text or '').strip()
    if not re.match(r'^https?://', base_url, re.I):
        await message.answer("❌ Base URL معتبر نیست. باید با http:// یا https:// شروع شود.",reply_markup=get_exit_menu())
        return
    data = await state.get_data()
    panel_id = data.get('panel_message_id')
    await state.update_data(provider_base_url=base_url)
    await state.set_state(BotStates.admin_provider_token)
    text = "🤖 افزودن مدل جدید\n\nمرحله ۲ از ۳\n🔐 توکن/API Key این مدل را ارسال کنید:" 
    if panel_id:
        try:
            await bot.edit_message_text(chat_id=message.chat.id, message_id=panel_id, text=text, reply_markup=get_exit_menu())
            return
        except Exception:
            pass
    await message.answer(text, reply_markup=get_exit_menu())


@router.message(F.chat.id == ADMIN_ID, StateFilter(BotStates.admin_provider_token))
async def provider_token_input(message: Message, state: FSMContext, bot: Bot):
    token = (message.text or '').strip()
    if len(token) < 4:
        await message.answer("❌ توکن خیلی کوتاه است. دوباره ارسال کنید.",reply_markup=get_exit_menu())
        return
    data = await state.get_data()
    panel_id = data.get('panel_message_id')
    await state.update_data(provider_token=token)
    await state.set_state(BotStates.admin_provider_model)
    text = "🤖 افزودن مدل جدید\n\nمرحله ۳ از ۳\n🧩 نام دقیق Model را دقیقاً همان‌طور که Provider می‌شناسد ارسال کنید:" 
    if panel_id:
        try:
            await bot.edit_message_text(chat_id=message.chat.id, message_id=panel_id, text=text, reply_markup=get_exit_menu())
            return
        except Exception:
            pass
    await message.answer(text, reply_markup=get_exit_menu())


@router.message(F.chat.id == ADMIN_ID, StateFilter(BotStates.admin_provider_model))
async def provider_model_input(message: Message, state: FSMContext, db: D1Database, bot: Bot):
    model = (message.text or '').strip()
    data = await state.get_data()
    base_url = data.get('provider_base_url','')
    token = data.get('provider_token','')
    if not model:
        await message.answer("❌ نام مدل خالی است.",reply_markup=get_exit_menu())
        return
    panel_id = data.get('panel_message_id')
    if panel_id:
        try:
            await bot.edit_message_text(chat_id=message.chat.id, message_id=panel_id, text="🔎 مرحله ۱: بررسی endpoint و نام مدل...\n🧪 مرحله ۲: ارسال یک درخواست واقعی TEST_OK به خود مدل...\n⏳ نتیجه بعد از دریافت پاسخ نمایش داده می‌شود.")
        except Exception:
            await message.answer("🔎 در حال بررسی endpoint و نام مدل...\n🧪 سپس یک درخواست واقعی TEST_OK به مدل ارسال می‌شود...")
    else:
        await message.answer("🧪 در حال تست اتصال و نام دقیق مدل...")
    tester = AIProviderManager(db)
    try:
        result = await tester.test_provider_values(base_url, token, model)
    finally:
        await tester.close()
    if not result.get('ok'):
        parent=data.get("parent_callback","auto_providers")
        panel_id=data.get("panel_message_id")
        await state.set_state(BotStates.idle)
        await state.update_data(provider_base_url=None, provider_token=None, provider_edit_id=None)
        error_text=("❌ این مدل در تست اولیه قبول نشد.\n\n"
            f"HTTP/API: {html.escape(str(result.get('error','unknown'))[:1000])}\n\n"
            "هیچ چیزی ذخیره نشد. این بار واقعاً یک POST آزمایشی به مدل ارسال شد.\n\n"
            "اگر Gemini است، Base URL رسمی OpenAI-compatible: https://generativelanguage.googleapis.com/v1beta/openai/\n"
            "یا Base URL بومی: https://generativelanguage.googleapis.com/v1beta\n"
            "نام مدل باید دقیقاً مطابق مدل در Google AI Studio باشد.")
        kb = provider_list_kb(await db.execute("SELECT id,name,base_url,model_name,priority,enabled,status,last_error,last_latency_ms FROM ai_providers ORDER BY priority ASC,id ASC")) if parent=="auto_providers" else get_admin_back_kb(parent)
        if panel_id:
            try:
                await bot.edit_message_text(chat_id=message.chat.id,message_id=panel_id,text=error_text,parse_mode='HTML',reply_markup=kb)
            except Exception:
                await message.answer(error_text,parse_mode='HTML',reply_markup=kb)
        else:
            await message.answer(error_text,parse_mode='HTML',reply_markup=kb)
        return
    now = datetime.now(timezone.utc).isoformat()
    host = urllib.parse.urlsplit(base_url).netloc or 'provider'
    name = f"{model[:80]} | {host[:30]}"[:120]
    edit_id = data.get('provider_edit_id')
    if edit_id:
        # در ویرایش، اولویت و وضعیت فعال قبلی حفظ می‌شود؛ فقط مشخصات اتصال عوض می‌شوند.
        old = await db.execute("SELECT priority,enabled,created_at FROM ai_providers WHERE id=?", [int(edit_id)])
        priority = int(old[0].get('priority') or 10) if old else 10
        await db.execute(
            "UPDATE ai_providers SET name=?, base_url=?, encrypted_api_key=?, model_name=?, updated_at=?, status='healthy', last_error=NULL, cooldown_until=NULL, last_checked_at=?, last_latency_ms=?, consecutive_failures=0 WHERE id=?",
            [name, base_url, encrypt_secret(token), model, now, now, result.get('latency_ms',0), int(edit_id)])
        action_text = f"✏️ مدل #{edit_id} با موفقیت ویرایش و تست شد."
    else:
        count = await db.execute("SELECT COALESCE(MAX(priority),0) AS p FROM ai_providers")
        priority = int(count[0].get('p') or 0) + 10 if count else 10
        await db.execute("INSERT INTO ai_providers(name, base_url, encrypted_api_key, model_name, priority, enabled, created_at, updated_at, status, last_checked_at, last_latency_ms, consecutive_failures) VALUES(?, ?, ?, ?, ?, 1, ?, ?, 'healthy', ?, ?, 0)", [name, base_url, encrypt_secret(token), model, priority, now, now, now, result.get('latency_ms',0)])
        action_text = "➕ مدل جدید با موفقیت اضافه و تست شد."
    await state.set_state(BotStates.idle)
    await state.update_data(provider_base_url=None, provider_token=None, provider_edit_id=None, panel_message_id=None)
    rows = await db.execute("SELECT id,name,base_url,model_name,priority,enabled,status,last_error,last_latency_ms FROM ai_providers ORDER BY priority ASC, id ASC")
    parent=data.get("parent_callback","auto_providers")
    panel_id=data.get("panel_message_id")
    text=f"✅ {action_text}\n\n🤖 Model: <code>{html.escape(model)}</code>\n🔌 Protocol: <code>{html.escape(str(result.get('protocol','auto')))}</code>\n⚡ زمان پاسخ تست واقعی: {result.get('latency_ms',0)}ms\n🧪 پاسخ مدل: <code>{html.escape(str(result.get('preview','TEST_OK'))[:120])}</code>\n🔢 اولویت: {priority}"
    if panel_id:
        try:
            await bot.edit_message_text(chat_id=message.chat.id,message_id=panel_id,text=text,parse_mode='HTML',reply_markup=provider_list_kb(rows) if parent=="auto_providers" else get_admin_back_kb(parent))
        except Exception:
            await message.answer(text,parse_mode='HTML',reply_markup=provider_list_kb(rows))
    else:
        await message.answer(text,parse_mode='HTML',reply_markup=provider_list_kb(rows))


@router.callback_query(F.data.startswith("provider_view_"))
async def provider_view(call: CallbackQuery, db: D1Database):
    pid = int(call.data.split("_")[-1])
    rows = await db.execute("SELECT id,name,base_url,model_name,priority,enabled,status,last_error,last_latency_ms,cooldown_until FROM ai_providers WHERE id=?", [pid])
    if not rows:
        await call.answer("Provider یافت نشد", show_alert=True); return
    p = rows[0]
    status = p.get('status') or 'unknown'
    status_text = {'healthy':'🟢 سالم','invalid':'🔴 تنظیمات/مدل نامعتبر','cooldown':'🟡 موقتاً در انتظار','unknown':'⚪ تست نشده'}.get(status,status)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ ویرایش مدل", callback_data=f"provider_edit_{pid}"), InlineKeyboardButton(text="🧪 تست اتصال", callback_data=f"provider_test_{pid}")],
        [InlineKeyboardButton(text="🔢 تغییر اولویت", callback_data=f"provider_priority_{pid}"), InlineKeyboardButton(text="⏸ خاموش" if p.get('enabled') else "▶️ فعال", callback_data=f"provider_toggle_{pid}")],
        [InlineKeyboardButton(text="🗑 حذف مدل", callback_data=f"provider_delete_{pid}" )],
        [InlineKeyboardButton(text="🔙 فهرست مدل‌ها", callback_data="auto_providers")]
    ])
    text = (f"🤖 مدل #{p['id']}\n\nModel: <code>{html.escape(str(p.get('model_name')))}</code>\n"
            f"Base URL: <code>{html.escape(str(p.get('base_url')))}</code>\n"
            f"اولویت: {p.get('priority')}\nوضعیت: {status_text}\n"
            f"Latency: {p.get('last_latency_ms') or 0}ms\n"
            f"آخرین خطا: {html.escape(str(p.get('last_error') or '-'))[:500]}")
    await call.message.edit_text(text, parse_mode='HTML', reply_markup=kb)
    await call.answer()


@router.callback_query(F.data == "provider_help")
async def provider_help(call: CallbackQuery):
    text = ("🤖 <b>راهنمای مدل‌های هوش مصنوعی</b>\n\n"
            "برای افزودن هر مدل فقط سه چیز لازم است:\n"
            "1️⃣ Base URL\n2️⃣ Token / API Key\n3️⃣ نام دقیق Model\n\n"
            "ربات قبل از ذخیره یک درخواست واقعی آزمایشی می‌فرستد. فقط مدل‌هایی که تستشان موفق باشد ذخیره می‌شوند.\n\n"
            "🔢 عدد اولویت کمتر = اولویت بالاتر\n"
            "🟡 خطاهای موقت مثل 429/503 باعث cooldown می‌شوند.\n"
            "🔴 خطاهایی مثل 404/401/403 به‌عنوان مشکل تنظیمات علامت‌گذاری می‌شوند.\n\n"
            "از صفحه هر مدل می‌توانی آن را ویرایش، تست، فعال/غیرفعال، اولویت‌بندی یا حذف کنی.\n\n"
            "🟦 Gemini / Google AI Studio:\n"
            "OpenAI-compatible Base URL: https://generativelanguage.googleapis.com/v1beta/openai/\n"
            "یا Native Base URL: https://generativelanguage.googleapis.com/v1beta\n"
            "Token همان Gemini API Key است و Model باید دقیقاً نام مدل باشد؛ مثلاً gemini-3.6-flash.\n\n"
            "ربات در افزودن مدل، اول endpoint را بررسی می‌کند و بعد واقعاً TEST_OK را به مدل می‌فرستد؛ اگر Google باشد هر دو مسیر Native و OpenAI-compatible را در صورت نیاز امتحان می‌کند.")
    await call.message.edit_text(text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 فهرست مدل‌ها", callback_data="auto_providers")]]))
    await call.answer()


@router.callback_query(F.data.startswith("provider_edit_"))
async def provider_edit(call: CallbackQuery, state: FSMContext, db: D1Database):
    pid = int(call.data.split("_")[-1])
    rows = await db.execute("SELECT id,base_url,model_name FROM ai_providers WHERE id=?", [pid])
    if not rows:
        await call.answer("مدل پیدا نشد", show_alert=True); return
    p = rows[0]
    await state.set_state(BotStates.admin_add_provider)
    await state.update_data(provider_edit_id=pid, provider_base_url=None, provider_token=None, panel_message_id=call.message.message_id,parent_callback="provider_view_"+str(pid))
    await call.message.edit_text(
        f"✏️ <b>ویرایش مدل #{pid}</b>\n\n"
        f"مدل فعلی: <code>{html.escape(str(p.get('model_name')))}</code>\n\n"
        "مرحله ۱ از ۳\n🔗 Base URL جدید را ارسال کن.",
        parse_mode='HTML', reply_markup=get_exit_menu())
    await call.answer()


@router.callback_query(F.data.startswith("provider_test_"))
async def provider_test(call: CallbackQuery, db: D1Database):
    pid = int(call.data.split("_")[-1])
    await call.answer("🧪 در حال تست...", show_alert=False)
    manager = AIProviderManager(db)
    try:
        result = await manager.test_provider(pid)
    finally:
        await manager.close()
    if result.get('ok'):
        await call.message.edit_text(f"✅ تست موفق بود.\n\n⚡ زمان پاسخ: {result.get('latency_ms',0)}ms\n🤖 پاسخ: {html.escape(result.get('preview','OK'))}", reply_markup=get_admin_back_kb(f"provider_view_{pid}"))
    else:
        await call.message.edit_text(f"❌ تست ناموفق بود.\n\n{html.escape(result.get('error','unknown')[:1200])}", reply_markup=get_admin_back_kb(f"provider_view_{pid}"))


@router.callback_query(F.data.startswith("provider_priority_"))
async def provider_priority(call: CallbackQuery, state: FSMContext):
    pid = int(call.data.split("_")[-1])
    await state.set_state(BotStates.admin_automation_setting)
    await state.update_data(automation_setting_key="__provider_priority__", provider_priority_id=pid,parent_callback=f"provider_view_{pid}",panel_message_id=call.message.message_id)
    await call.message.edit_text("🔢 اولویت این مدل را به عدد بفرست.\nعدد کمتر = اولویت بالاتر.", reply_markup=get_exit_menu())
    await call.answer()


@router.callback_query(F.data.startswith("provider_toggle_"))
async def provider_toggle(call: CallbackQuery, db: D1Database):
    pid = int(call.data.split("_")[-1])
    rows = await db.execute("SELECT enabled FROM ai_providers WHERE id=?", [pid])
    if rows:
        await db.execute("UPDATE ai_providers SET enabled=?, updated_at=? WHERE id=?", [0 if rows[0].get("enabled") else 1, datetime.now(timezone.utc).isoformat(), pid])
    await provider_view(call, db)


@router.callback_query(F.data.regexp(r"^provider_delete_(\d+)$"))
async def provider_delete(call: CallbackQuery, db: D1Database):
    pid = int(call.data.split("_")[-1])
    rows = await db.execute("SELECT id, model_name, name FROM ai_providers WHERE id=?", [pid])
    if not rows:
        await call.answer("مدل پیدا نشد", show_alert=True); return
    p = rows[0]
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗑️ بله، حذف شود", callback_data=f"provider_delete_confirm_{pid}")],
        [InlineKeyboardButton(text="↩️ لغو", callback_data=f"provider_view_{pid}")]
    ])
    await call.message.edit_text(
        f"⚠️ <b>حذف مدل</b>\n\nمدل: <code>{html.escape(str(p.get('model_name')))}</code>\n\nاین Provider از چرخه Failover حذف خواهد شد. ادامه می‌دهی؟",
        parse_mode='HTML', reply_markup=kb)
    await call.answer()


@router.callback_query(F.data.regexp(r"^provider_delete_confirm_(\d+)$"))
async def provider_delete_confirm(call: CallbackQuery, db: D1Database):
    pid = int(call.data.split("_")[-1])
    await db.execute("DELETE FROM ai_providers WHERE id=?", [pid])
    rows = await db.execute("SELECT id,name,base_url,model_name,priority,enabled,status,last_error,last_latency_ms FROM ai_providers ORDER BY priority ASC, id ASC")
    await call.message.edit_text("🗑️ <b>مدل حذف شد.</b>\n\nفهرست مدل‌ها:", parse_mode='HTML', reply_markup=provider_list_kb(rows))
    await call.answer("حذف شد")


@router.callback_query(F.data == "auto_quality")
async def auto_quality(call: CallbackQuery, db: D1Database):
    if call.from_user.id != ADMIN_ID: return
    score = await get_setting(db, 'min_content_score', str(DEFAULT_MIN_CONTENT_SCORE))
    text = (f'🧠 <b>کیفیت محتوا</b>\n\nحداقل امتیاز فعلی: <b>{html.escape(score)}</b> از 100\n\n'
            'این بخش فقط درباره انتخاب و کیفیت خبر است؛ تنظیم زمان و تعداد پست‌ها در بخش «برنامه انتشار» قرار دارد.')
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='⭐ تغییر حداقل امتیاز', callback_data='set_min_score')],
        [InlineKeyboardButton(text='🎯 وزن معیارهای محتوا', callback_data='quality_weights')],
        [InlineKeyboardButton(text='📖 معیارها', callback_data='quality_about')],
        [InlineKeyboardButton(text='🔙 اتوماسیون محتوا', callback_data='auto_back')]
    ])
    await call.message.edit_text(text, parse_mode='HTML', reply_markup=kb)
    await call.answer()

@router.callback_query(F.data == "quality_about")
async def quality_about(call: CallbackQuery):
    text = ('🧠 <b>معیارهای کیفیت محتوا</b>\n\n'
            '⭐ اهمیت جهانی و فناوری\n'
            '🤖 ارتباط با AI و مدل‌ها\n'
            '🔐 ارزش امنیت سایبری\n'
            '🇮🇷 ارتباط با ایران و فارسی‌زبانان\n'
            '🆕 تازگی و ارزش خبری\n'
            '♻️ تکراری نبودن\n'
            '✅ اعتبار و کیفیت منبع\n\n'
            'محتوای ضعیف یا مبهم منتشر نمی‌شود؛ کیفیت بر تعداد اولویت دارد.')
    await call.message.edit_text(text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text='🔙 کیفیت محتوا', callback_data='auto_quality')]]))
    await call.answer()

@router.callback_query(F.data == "weight_global")
async def set_weight_global(call:CallbackQuery,state:FSMContext):
    await prompt_for_setting(call,state,"weight_global","🌍 اهمیت جهانی را به عدد 0 تا 100 بفرست.","quality_weights")

@router.callback_query(F.data == "weight_technology")
async def set_weight_technology(call:CallbackQuery,state:FSMContext):
    await prompt_for_setting(call,state,"weight_technology","💻 فناوری را به عدد 0 تا 100 بفرست.","quality_weights")

@router.callback_query(F.data == "weight_ai")
async def set_weight_ai(call:CallbackQuery,state:FSMContext):
    await prompt_for_setting(call,state,"weight_ai","🤖 هوش مصنوعی را به عدد 0 تا 100 بفرست.","quality_weights")

@router.callback_query(F.data == "weight_cyber")
async def set_weight_cyber(call:CallbackQuery,state:FSMContext):
    await prompt_for_setting(call,state,"weight_cyber","🔐 امنیت سایبری را به عدد 0 تا 100 بفرست.","quality_weights")

@router.callback_query(F.data == "weight_education")
async def set_weight_education(call:CallbackQuery,state:FSMContext):
    await prompt_for_setting(call,state,"weight_education","📚 آموزش را به عدد 0 تا 100 بفرست.","quality_weights")

@router.callback_query(F.data == "weight_iran")
async def set_weight_iran(call:CallbackQuery,state:FSMContext):
    await prompt_for_setting(call,state,"weight_iran","🇮🇷 ارتباط ایران/فارسی را به عدد 0 تا 100 بفرست.","quality_weights")

@router.callback_query(F.data == "weight_freshness")
async def set_weight_freshness(call:CallbackQuery,state:FSMContext):
    await prompt_for_setting(call,state,"weight_freshness","🆕 تازگی را به عدد 0 تا 100 بفرست.","quality_weights")

@router.callback_query(F.data == "weight_source")
async def set_weight_source(call:CallbackQuery,state:FSMContext):
    await prompt_for_setting(call,state,"weight_source","✅ اعتبار منبع را به عدد 0 تا 100 بفرست.","quality_weights")

@router.callback_query(F.data == "weight_novelty")
async def set_weight_novelty(call:CallbackQuery,state:FSMContext):
    await prompt_for_setting(call,state,"weight_novelty","♻️ عدم تکرار را به عدد 0 تا 100 بفرست.","quality_weights")

@router.callback_query(F.data == "quality_weights")
async def quality_weights(call:CallbackQuery,db:D1Database):
    items=[("global","🌍 اهمیت جهانی"),("technology","💻 فناوری"),("ai","🤖 هوش مصنوعی"),("cyber","🔐 امنیت سایبری"),("education","📚 آموزش"),("iran","🇮🇷 ایران/فارسی"),("freshness","🆕 تازگی"),("source","✅ اعتبار منبع"),("novelty","♻️ عدم تکرار")]
    text="🎯 <b>وزن معیارها</b>\nعدد بالاتر = اهمیت بیشتر.\n\n"; rows=[]
    for k,label in items:
        text+=f"{label}: <b>{await get_setting(db,'weight_'+k,'10')}</b>\n"; rows.append([InlineKeyboardButton(text=label,callback_data='weight_'+k)])
    rows.append([InlineKeyboardButton(text="🔙 کیفیت محتوا",callback_data="auto_quality")])
    await call.message.edit_text(text,parse_mode='HTML',reply_markup=InlineKeyboardMarkup(inline_keyboard=rows)); await call.answer()

@router.callback_query(F.data == "auto_schedule")
async def auto_schedule(call: CallbackQuery, db: D1Database):
    if call.from_user.id != ADMIN_ID: return
    text = (await automation_report(db)) + '\n\n⏱ <b>برنامه انتشار</b>\nتعداد پست، فاصله انتشار، فاصله بررسی منابع و Workerها را از اینجا تنظیم کن.'
    await call.message.edit_text(text, parse_mode='HTML', reply_markup=schedule_menu_kb())
    await call.answer()

@router.callback_query(F.data == "auto_health")
async def auto_health(call:CallbackQuery,db:D1Database,bot:Bot):
    if call.from_user.id!=ADMIN_ID:return
    channel=await get_channel_id(db)
    providers=await db.execute("SELECT status,enabled FROM ai_providers")
    healthy=sum(1 for p in providers if p.get("enabled") and p.get("status")=="healthy")
    sources=await db.execute("SELECT COUNT(*) c FROM sources WHERE enabled=1")
    text=(f"🧪 <b>تست و سلامت</b>\n\nD1: {'✅ آماده' if db.session and not db.session.closed else '❌'}\n"
          f"کانال: {'✅ تنظیم شده' if channel else '❌ تنظیم نشده'}\nمدل سالم: {healthy}/{len(providers)}\n"
          f"منبع فعال: {sources[0].get('c',0) if sources else 0}\n\nاول تست AI، بعد منبع، بعد تولید و در پایان انتشار را انجام بده.")
    kb=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🤖 تست AI",callback_data="health_test_ai")],
        [InlineKeyboardButton(text="🌐 تست همه منابع",callback_data="health_test_source")],
        [InlineKeyboardButton(text="🧪 تولید بدون انتشار",callback_data="health_dry_run")],
        [InlineKeyboardButton(text="▶️ اجرای یک چرخه واقعی",callback_data="health_run_cycle")],
        [InlineKeyboardButton(text="📢 تست انتشار واقعی",callback_data="health_test_publish")],
        [InlineKeyboardButton(text="🔄 بروزرسانی",callback_data="auto_health"),InlineKeyboardButton(text="🔙 اتوماسیون",callback_data="auto_back")]
    ])
    await call.message.edit_text(text,parse_mode="HTML",reply_markup=kb); await call.answer()

@router.callback_query(F.data == "health_test_ai")
async def health_test_ai(call:CallbackQuery,db:D1Database):
    rows=await db.execute("SELECT id,name,model_name,priority FROM ai_providers WHERE enabled=1 ORDER BY priority ASC,id ASC LIMIT 8")
    if not rows:
        await call.message.edit_text("❌ هیچ مدل فعالی نیست.",reply_markup=get_admin_back_kb("auto_health")); return
    m=AIProviderManager(db); results=[]
    try:
        for p in rows:
            result=await m.test_provider(int(p["id"]))
            if result.get("ok"):
                results.append(f"✅ {html.escape(str(p.get('model_name')))} · {result.get('latency_ms',0)}ms")
            else:
                results.append(f"❌ {html.escape(str(p.get('model_name')))} · {html.escape(str(result.get('error','unknown'))[:180])}")
    finally:
        await m.close()
    await call.message.edit_text("🧪 <b>نتیجه تست مدل‌های AI</b>\n\n"+"\n".join(results),parse_mode='HTML',reply_markup=get_admin_back_kb("auto_health")); await call.answer()

@router.callback_query(F.data == "health_test_source")
async def health_test_source(call:CallbackQuery,db:D1Database):
    rows=await db.execute("SELECT * FROM sources WHERE enabled=1 ORDER BY priority ASC,id ASC LIMIT 8")
    if not rows:
        await call.message.edit_text("❌ هیچ منبع فعالی نیست.",reply_markup=get_admin_back_kb("auto_health")); return
    results=[]
    for src in rows:
        try:
            diag=await discover_source_items(src,return_diagnostics=True)
            if diag.get("items"):
                results.append(f"✅ {src.get('name')}: {len(diag['items'])} آیتم | {diag.get('method')}")
            else:
                results.append(f"⚠️ {src.get('name')}: {diag.get('error','بدون آیتم')[:260]}")
        except Exception as e:
            results.append(f"❌ {src.get('name')}: {str(e)[:260]}")
    text="🧪 <b>تست منابع</b>\n\n"+"\n".join(results)
    await call.message.edit_text(text,parse_mode='HTML',reply_markup=get_admin_back_kb("auto_health")); await call.answer()

@router.callback_query(F.data == "health_dry_run")
async def health_dry_run(call:CallbackQuery,db:D1Database):
    rows=await db.execute("SELECT * FROM sources WHERE enabled=1 ORDER BY priority ASC,id ASC LIMIT 8")
    if not rows:
        await call.message.edit_text("❌ اول یک منبع فعال اضافه کن.",reply_markup=get_admin_back_kb("auto_health")); return
    m=AIProviderManager(db)
    try:
        chosen=None; chosen_src=None; diagnostics=[]
        for src in rows:
            try:
                diag=await discover_source_items(src,return_diagnostics=True)
                if diag.get("items"):
                    chosen=diag["items"][0]; chosen_src=src; break
                scout=await m.gemini_web_scout(src.get("url") or "", int(await get_setting(db,"ai_web_scout_max_items","5")))
                if scout.get("ok") and scout.get("items"):
                    chosen=scout["items"][0]; chosen_src=src; break
                diagnostics.append(f"{src.get('name')}: {diag.get('error','بدون آیتم')[:180]}")
            except Exception as e:
                diagnostics.append(f"{src.get('name')}: {str(e)[:180]}")
        if not chosen:
            msg="❌ هیچ منبعی برای تست محتوای قابل استفاده نداد.\n\n"+"\n".join(diagnostics)
        else:
            weights={k:float(await get_setting(db,'weight_'+k,'10')) for k in ['global','technology','ai','cyber','education','iran','freshness','source','novelty']}
            out=await ai_editorial_process(m,chosen,chosen_src,[],weights)
            if out.get("error"):
                msg="❌ ارتباط/پاسخ AI در تست تولید مشکل داشت:\n"+html.escape(str(out["error"])[:1200])
            elif not out.get("accept"):
                msg=("⚠️ AI سالم است اما این مطلب را برای انتشار رد کرد.\n\n"
                     f"امتیاز: {out.get('score','-')}\n"
                     f"دلیل: {html.escape(str(out.get('why','-'))[:500])}")
            else:
                ai_info=out.get("ai") or {}
                preview_ch=sanitize_telegram_html(out.get("channel_html") or out.get("channel_text") or "")
                preview_ar=sanitize_telegram_html(out.get("article_html") or out.get("article_text") or "")
                msg=("✅ <b>تولید آزمایشی موفق است.</b>\n\n"
                     f"منبع: {html.escape(str(chosen_src.get('name')))}\n"
                     f"امتیاز نهایی: {out.get('score','-')}\n"
                     f"عنوان: {html.escape(str(out.get('title','-')))}\n"
                     f"مدل: {html.escape(str(ai_info.get('model','-')))}\n\n"
                     "<b>پیش‌نمایش کانال:</b>\n"+preview_ch[:900]+"\n\n"
                     "<b>پیش‌نمایش مقاله ربات:</b>\n"+(preview_ar if len(preview_ar)<=3400 else html.escape(strip_html_text(preview_ar)[:3400])+"…"))
    except Exception as e:
        msg="❌ تست تولید شکست خورد:\n"+html.escape(str(e)[:1500])
    finally: await m.close()
    await call.message.edit_text(msg,parse_mode='HTML',reply_markup=get_admin_back_kb("auto_health")); await call.answer()

def make_health_png(width=1280,height=720):
    row=bytearray()
    for x in range(width):
        t=x/(width-1)
        row.extend((int(16+24*t),int(22+36*t),int(48+78*t)))
    raw=bytearray()
    for _ in range(height):
        raw.append(0); raw.extend(row)
    def chunk(tag,data):
        return struct.pack('>I',len(data))+tag+data+struct.pack('>I',zlib.crc32(tag+data)&0xffffffff)
    return b'\x89PNG\r\n\x1a\n'+chunk(b'IHDR',struct.pack('>IIBBBBB',width,height,8,2,0,0,0))+chunk(b'IDAT',zlib.compress(bytes(raw),9))+chunk(b'IEND',b'')

@router.callback_query(F.data == "health_run_cycle")
async def health_run_cycle(call:CallbackQuery,db:D1Database,bot:Bot):
    if call.from_user.id!=ADMIN_ID: return
    try:
        sources=await db.execute("SELECT * FROM sources WHERE enabled=1 ORDER BY priority DESC,next_check_at ASC LIMIT 8")
        ai=AIProviderManager(db,bot)
        counts=[]
        try:
            for src in sources:
                result=await fetch_source_cycle(db,src,ai)
                counts.append(f"{src.get('name')}: {result} مورد پردازش")
            published=await publish_next_article(db,bot)
        finally:
            await ai.close()
        msg=("✅ <b>یک چرخه واقعی اجرا شد.</b>\n\n"+"\n".join(counts[:8])+f"\n\nانتشار در این چرخه: {'✅ بله' if published else '⏸ خیر (صف/زمان/امتیاز)'}")
    except Exception as e:
        msg="❌ اجرای چرخه شکست خورد:\n"+html.escape(str(e)[:1500])
    await call.message.edit_text(msg,parse_mode='HTML',reply_markup=get_admin_back_kb("auto_health")); await call.answer()

@router.callback_query(F.data == "health_test_publish")
async def health_test_publish(call:CallbackQuery,db:D1Database,bot:Bot):
    channel=await get_channel_id(db)
    if not channel:
        await call.message.edit_text("❌ کانال تنظیم نشده.",reply_markup=get_admin_back_kb("auto_health")); return
    try:
        caption=("<b>🧪 تست سلامت انتشار TechNowAI</b>\n\n"
                 "✅ دسترسی Bot به کانال برقرار است.\n"
                 "✅ ارسال تصویر و HTML فعال است.\n"
                 "✅ مسیر انتشار آماده است.\n\n"
                 "این پیام صرفاً برای تست است و مقاله واقعی نیست.")
        photo=BufferedInputFile(make_health_png(),filename="technowai-health.png")
        sent=await bot.send_photo(channel,photo=photo,caption=caption,parse_mode='HTML')
        msg=f"✅ <b>تست واقعی انتشار موفق بود.</b>\nMessage ID: <code>{sent.message_id}</code>"
    except Exception as e:
        msg="❌ <b>انتشار تستی شکست خورد.</b>\n\n"+html.escape(str(e)[:1200])
    await call.message.edit_text(msg,parse_mode='HTML',reply_markup=get_admin_back_kb("auto_health")); await call.answer()

@router.callback_query(F.data == "auto_settings")
async def auto_settings_legacy(call: CallbackQuery, db: D1Database):
    # سازگاری با callbackهای قدیمی؛ به برنامه انتشار هدایت می‌شود.
    await auto_schedule(call, db)

async def prompt_for_setting(call: CallbackQuery, state: FSMContext, key: str, label: str, parent: str = "auto_schedule"):
    await state.set_state(BotStates.admin_automation_setting)
    await state.update_data(automation_setting_key=key, panel_message_id=call.message.message_id, parent_callback=parent)
    await call.message.edit_text(label, reply_markup=get_exit_menu())
    await call.answer()


@router.callback_query(F.data == "set_max_daily")
async def set_max_daily(call: CallbackQuery, state: FSMContext):
    await prompt_for_setting(call, state, "max_daily_posts", "🔢 تعداد تقریبی/حداکثر پست روزانه را به عدد بفرست. مثلاً 6")

@router.callback_query(F.data == "set_min_score")
async def set_min_score(call: CallbackQuery, state: FSMContext):
    await prompt_for_setting(call, state, "min_content_score", "⭐ حداقل امتیاز انتشار را بین 0 تا 100 بفرست. پیشنهاد: 75", "auto_quality")

@router.callback_query(F.data == "set_min_gap")
async def set_min_gap(call: CallbackQuery, state: FSMContext):
    await prompt_for_setting(call, state, "min_hours_between_posts", "⏱ حداقل فاصله بین دو پست را بر حسب ساعت بفرست. مثلاً 2")

@router.callback_query(F.data == "set_default_interval")
async def set_default_interval(call: CallbackQuery, state: FSMContext):
    await prompt_for_setting(call, state, "default_source_interval", "🌐 فاصله بررسی پیش‌فرض منابع را بر حسب دقیقه بفرست. مثلاً 15")

@router.callback_query(F.data == "set_workers")
async def set_workers(call: CallbackQuery, state: FSMContext):
    await prompt_for_setting(call, state, "max_workers", "⚡ تعداد Workerهای همزمان را بین 1 تا 4 بفرست. پیشنهاد برای Railway کوچک: 2")

@router.callback_query(F.data == "set_ai_workers")
async def set_ai_workers(call: CallbackQuery, state: FSMContext):
    await prompt_for_setting(call, state, "max_ai_workers", "🧠 تعداد درخواست‌های همزمان AI را بین 1 تا 4 بفرست. پیشنهاد Railway کوچک: 2 یا 3")


@router.callback_query(F.data == "set_ai_verify")
async def set_ai_verify(call:CallbackQuery,state:FSMContext):
    await prompt_for_setting(call,state,"ai_verify_mode","🛡 حالت راستی‌آزمایی را بفرست:\nauto = فقط موارد حساس\nalways = همیشه\noff = خاموش","auto_schedule")

async def next_publication_estimate(db:D1Database)->Dict[str,Any]:
    now=datetime.now(timezone.utc)
    last_manual=await get_setting(db,"last_manual_channel_post_at","")
    last_pub=await db.execute("SELECT published_at FROM publication_queue WHERE status='published' AND published_at IS NOT NULL ORDER BY id DESC LIMIT 1")
    candidates=[x for x in [last_manual, last_pub[0].get("published_at") if last_pub else ""] if x]
    latest=None
    for raw in candidates:
        try:
            dt=datetime.fromisoformat(str(raw).replace("Z","+00:00"))
            if latest is None or dt>latest: latest=dt
        except Exception: pass
    interval_hours=float(await get_setting(db,"min_hours_between_posts",str(DEFAULT_MIN_HOURS_BETWEEN_POSTS)))
    target=max(now, latest+timedelta(hours=interval_hours) if latest else now)
    queued=await db.execute("SELECT COUNT(*) c FROM publication_queue WHERE status='queued'")
    return {"target":target,"minutes":max(0,int((target-now).total_seconds()/60)) if target>now else 0,"latest":latest,"interval_minutes":int(interval_hours*60),"queued":int(queued[0].get('c',0)) if queued else 0}

@router.callback_query(F.data == "auto_publish_now")
async def auto_publish_now(call:CallbackQuery,db:D1Database,bot:Bot):
    if call.from_user.id!=ADMIN_ID: return
    try:
        ok=await publish_next_article(db,bot,force=True)
        msg="✅ اولین محتوای صف همین حالا منتشر شد." if ok else "⚠️ محتوای آماده‌ای برای انتشار فوری پیدا نشد یا سقف روزانه تکمیل شده است."
    except Exception as e:
        msg="❌ انتشار فوری شکست خورد:\n"+html.escape(str(e)[:1200])
    await call.message.edit_text(msg,parse_mode="HTML",reply_markup=get_admin_back_kb("auto_queue")); await call.answer()

@router.callback_query(F.data == "auto_queue")
async def auto_queue(call: CallbackQuery, db: D1Database):
    est=await next_publication_estimate(db)
    rows = await db.execute("SELECT q.id, q.article_id, q.status, q.attempts, a.title, a.score, a.category FROM publication_queue q JOIN articles a ON a.id=q.article_id WHERE q.status='queued' ORDER BY a.score DESC LIMIT 20")
    last_txt=est["latest"].astimezone(pytz.timezone("Asia/Tehran")).strftime("%H:%M") if est["latest"] else "هنوز منتشر نشده"
    if est["minutes"]<=0: next_txt="آماده انتشار طبق فاصله زمانی"
    elif est["minutes"]<60: next_txt=f"حدود {est['minutes']} دقیقه دیگر"
    else: next_txt=f"حدود {est['minutes']//60} ساعت و {est['minutes']%60} دقیقه دیگر"
    text=("📢 <b>صف و صفحه انتشار</b>\n\n"
          f"📦 آماده در صف: <b>{est['queued']}</b>\n"
          f"🕘 آخرین انتشار: <b>{last_txt}</b>\n"
          f"⏱ فاصله مدیریت‌شده: <b>{est['interval_minutes']} دقیقه</b>\n"
          f"🕐 نوبت بعدی: <b>{next_txt}</b>\n\n")
    if not rows: text+="صف فعلاً خالی است."
    else:
        for r in rows:
            text += f"#{r['article_id']} | {float(r['score'] or 0):.0f} | {r['category']} | {str(r['title'])[:65]}\n"
    kb=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚀 همین حالا منتشر کن",callback_data="auto_publish_now")],
        [InlineKeyboardButton(text="🔄 بروزرسانی",callback_data="auto_queue")],
        [InlineKeyboardButton(text="🔙 اتوماسیون",callback_data="auto_back")]
    ])
    await call.message.edit_text(text,parse_mode="HTML",reply_markup=kb); await call.answer()

@router.channel_post()
async def on_channel_post(message: Message, db: D1Database):
    configured_channel = await get_channel_id(db)
    if not configured_channel or str(message.chat.id) != str(configured_channel):
        return
    rows = await db.execute("SELECT id FROM articles WHERE published_message_id=?", [message.message_id])
    if rows:
        return
    now = datetime.now(timezone.utc).isoformat()
    await set_setting(db, "last_manual_channel_post_at", now)
    await db.execute("INSERT INTO manual_channel_events(message_id, created_at) VALUES(?,?)", [message.message_id, now])


# ============================================================
# پردازش رویدادهای کلیک روی کیبوردهای شیشه‌ای (Callback Queries)
# ============================================================
@router.callback_query(F.data.startswith("like_") | F.data.startswith("dis_"))
async def process_post_voting(call: CallbackQuery, db: D1Database):
    parts = call.data.split("_")
    new_vote = "like" if parts[0] == "like" else "dislike"
    post_id = int(parts[1])
    user_id = call.from_user.id
    
    vote_rows = await db.execute("SELECT vote_type FROM votes WHERE user_id = ? AND post_id = ?", [user_id, post_id])
    response_text = ""
    
    try:
        if not vote_rows:
            await db.execute_batch([
                {"sql": "INSERT INTO votes(user_id, post_id, vote_type) VALUES(?, ?, ?)", "params": [user_id, post_id, new_vote]},
                {"sql": f"UPDATE posts SET {new_vote}s = {new_vote}s + 1 WHERE id = ?", "params": [post_id]}
            ])
            response_text = "✅ رأی خفنت ثبت شد! 😎"
        else:
            current_vote = vote_rows[0].get("vote_type")
            if current_vote == new_vote:
                await db.execute_batch([
                    {"sql": "DELETE FROM votes WHERE user_id = ? AND post_id = ?", "params": [user_id, post_id]},
                    {"sql": f"UPDATE posts SET {new_vote}s = {new_vote}s - 1 WHERE id = ?", "params": [post_id]}
                ])
                response_text = "🔄 رأیت رو پس گرفتی! 🔙"
            else:
                await db.execute_batch([
                    {"sql": "UPDATE votes SET vote_type = ? WHERE user_id = ? AND post_id = ?", "params": [new_vote, user_id, post_id]},
                    {"sql": f"UPDATE posts SET {new_vote}s = {new_vote}s + 1, {current_vote}s = {current_vote}s - 1 WHERE id = ?", "params": [post_id]}
                ])
                response_text = "🔄 رأیت با موفقیت تغییر کرد!"
    except Exception:
        response_text = "❌ خطا در ثبت رأی"
        
    await call.answer(response_text, show_alert=True)
    
    p_rows = await db.execute("SELECT likes, dislikes FROM posts WHERE id = ?", [post_id])
    if p_rows:
        p = p_rows[0]
        s_rows = await db.execute("SELECT folder FROM saves WHERE user = ? AND post = ?", [user_id, post_id])
        kb = get_post_inline_kb(post_id, p.get("likes", 0), p.get("dislikes", 0), len(s_rows) > 0)
        try:
            await call.message.edit_reply_markup(reply_markup=kb)
        except Exception:
            pass

@router.callback_query(F.data.startswith("save_"))
async def process_save_action(call: CallbackQuery):
    post_id = int(call.data.split("_")[1])
    await call.message.answer("📂 این مطلب ارزشمند رو تو کدوم پوشه بذارم؟ 👇", reply_markup=get_save_to_folder_kb(post_id))
    await call.answer()

@router.callback_query(F.data.startswith("fsave_"))
async def process_folder_save(call: CallbackQuery, db: D1Database):
    parts = call.data.split("_")
    post_id = int(parts[1])
    folder = parts[2]
    user_id = call.from_user.id
    
    try:
        await db.execute("INSERT OR IGNORE INTO saves(user, post, folder) VALUES(?, ?, ?)", [user_id, post_id, folder])
        folder_display = FOLDER_NAMES.get(folder, folder)
        await call.answer(f"✅ با موفقیت در {folder_display} ذخیره شد!", show_alert=True)
        try:
            await call.message.delete()
        except Exception:
            pass
    except Exception:
        await call.answer("❌ خطا در ذخیره سازی", show_alert=True)

@router.callback_query(F.data.startswith("unsave_"))
async def process_unsave_action(call: CallbackQuery, db: D1Database):
    post_id = int(call.data.split("_")[1])
    user_id = call.from_user.id
    
    try:
        await db.execute("DELETE FROM saves WHERE user = ? AND post = ?", [user_id, post_id])
        await call.answer("🗑️ مطلب از ذخیره‌هات پاک شد!", show_alert=True)
        
        p_rows = await db.execute("SELECT likes, dislikes FROM posts WHERE id = ?", [post_id])
        if p_rows:
            p = p_rows[0]
            kb = get_post_inline_kb(post_id, p.get("likes", 0), p.get("dislikes", 0), False)
            try:
                await call.message.edit_reply_markup(reply_markup=kb)
            except Exception:
                pass
    except Exception:
        await call.answer("❌ خطا در حذف", show_alert=True)

@router.callback_query(F.data.startswith("f_view_"))
async def process_view_saved_folder(call: CallbackQuery, state: FSMContext, db: D1Database, bot: Bot):
    folder = call.data.split("_")[2]
    user_id = call.from_user.id
    state_data = await state.get_data()
    
    if state_data.get("cached_folder") == folder and state_data.get("cached_list"):
        cached_list = state_data["cached_list"]
        await call.answer()
        await state.update_data(current_folder=folder, current_index=0, current_list=cached_list)
        p_rows = await db.execute("SELECT text, file_id, media_type FROM posts WHERE id = ?", [cached_list[0]])
        if p_rows:
            kb = get_saved_folder_pagination_kb(cached_list[0], folder, 0)
            await send_post_content(bot, call.message.chat.id, p_rows[0], kb)
        return
        
    rows = await db.execute(
        """SELECT posts.id FROM saves JOIN posts ON saves.post = posts.id
           WHERE saves.user = ? AND saves.folder = ? AND posts.deleted = 0
           ORDER BY posts.id DESC LIMIT 30""",
        [user_id, folder]
    )
    folder_display = FOLDER_NAMES.get(folder, folder)
    if not rows:
        await call.answer(f"📭 {folder_display} فعلاً خالیه! برو از کانال چند تا مطلب خفن توش ذخیره کن 🕸️", show_alert=True)
    else:
        post_ids = [r["id"] for r in rows]
        await state.update_data(cached_folder=folder, cached_list=post_ids, current_folder=folder, current_index=0, current_list=post_ids)
        await call.answer()
        
        post_id = post_ids[0]
        p_rows = await db.execute("SELECT text, file_id, media_type FROM posts WHERE id = ?", [post_id])
        if p_rows:
            kb = get_saved_folder_pagination_kb(post_id, folder, 0)
            await send_post_content(bot, call.message.chat.id, p_rows[0], kb)

@router.callback_query(F.data.startswith("fpg_"))
async def process_folder_pagination(call: CallbackQuery, state: FSMContext, db: D1Database, bot: Bot):
    parts = call.data.split("_")
    direction = parts[1]
    folder = parts[2]
    current_index = int(parts[3])
    
    state_data = await state.get_data()
    lst = state_data.get("current_list", [])
    if lst and folder:
        new_index = current_index + 1 if direction == "next" else current_index - 1
        new_index = max(0, min(new_index, len(lst) - 1))
        
        if new_index == current_index:
            await call.answer("🚧 رسیدی به انتهای لیست!")
        else:
            await call.answer()
            post_id = lst[new_index]
            await state.update_data(current_index=new_index)
            
            p_rows = await db.execute("SELECT text, file_id, media_type FROM posts WHERE id = ?", [post_id])
            if p_rows:
                post = p_rows[0]
                kb = get_saved_folder_pagination_kb(post_id, folder, new_index)
                if post.get("file_id") and post.get("media_type"):
                    try:
                        await call.message.delete()
                    except Exception:
                        pass
                    await send_post_content(bot, call.message.chat.id, post, kb)
                else:
                    try:
                        await call.message.edit_text(text=post.get("text") or "", reply_markup=kb)
                    except Exception:
                        pass

@router.callback_query(F.data.startswith("f_srch_"))
async def process_f_search_button(call: CallbackQuery, state: FSMContext):
    folder = call.data.split("_")[2]
    state_data = await state.get_data()
    
    now = time.time() * 1000
    WINDOW_MS = 8 * 60 * 60 * 1000
    search_count = state_data.get("search_count", 0)
    window_start = state_data.get("search_window_start", 0)
    
    if now - window_start > WINDOW_MS:
        search_count = 0
        window_start = 0
        
    if search_count >= 5:
        await call.answer("🛑 به دلیل کمبود منابع در هر 8 ساعت قادر به تنها 5 بار جستوجو هستید", show_alert=True)
        unlock_time_ms = window_start + WINDOW_MS
        tehran_tz = pytz.timezone("Asia/Tehran")
        unlock_dt = datetime.fromtimestamp(unlock_time_ms / 1000, tehran_tz)
        time_str = unlock_dt.strftime("%H:%M")
        day_str = "امروز" if unlock_dt.date() == datetime.now(tehran_tz).date() else "فردا"
        
        await call.message.answer(f"⏱️ موتور جستجوی اختصاصی شما {day_str} ساعت {time_str} فعال میشه\n\n تا اون موقع می‌تونی دستی پوشه‌هات رو ورق بزنی ! 🕵️‍♂️")
        return
        
    await state.set_state(BotStates.user_search_folder)
    await state.update_data(folder=folder)
    folder_display = FOLDER_NAMES.get(folder, folder)
    await call.message.answer(f"🔍 کلمات یا واژه‌ای که می‌دونی تو پوشه {folder_display} ذخیره کردی رو بفرست تا برات سرچش کنم 🕵️‍♂️")
    await call.answer()

@router.callback_query(F.data.startswith("fspg_"))
async def process_folder_search_pagination(call: CallbackQuery, state: FSMContext, db: D1Database, bot: Bot):
    parts = call.data.split("_")
    direction = parts[1]
    folder = parts[2]
    current_index = int(parts[3])
    
    state_data = await state.get_data()
    search_ids = state_data.get("search_ids", [])
    if search_ids and folder:
        new_index = current_index + 1 if direction == "next" else current_index - 1
        new_index = max(0, min(new_index, len(search_ids) - 1))
        
        if new_index == current_index:
            await call.answer("🚧 رسیدی به انتهای نتایج!")
        else:
            await call.answer()
            post_id = search_ids[new_index]
            p_rows = await db.execute("SELECT text, file_id, media_type FROM posts WHERE id = ?", [post_id])
            if p_rows:
                post = p_rows[0]
                kb = get_saved_folder_search_pagination_kb(post_id, folder, new_index)
                if post.get("file_id") and post.get("media_type"):
                    try:
                        await call.message.delete()
                    except Exception:
                        pass
                    await send_post_content(bot, call.message.chat.id, post, kb)
                else:
                    try:
                        await call.message.edit_text(text=post.get("text") or "", reply_markup=kb)
                    except Exception:
                        pass

@router.callback_query(F.data.startswith("ask_del_"))
async def process_ask_deletion(call: CallbackQuery, state: FSMContext):
    parts = call.data.split("_")
    post_id = int(parts[2])
    folder = parts[3]
    
    await state.update_data(pending_delete={"post_id": post_id, "folder": folder})
    kb = get_confirm_delete_kb(post_id, folder)
    try:
        await call.message.edit_reply_markup(reply_markup=kb)
    except Exception:
        await call.message.answer("آیا مطمئنی می‌خوای این مطلب رو از پوشه‌ات پاک کنی؟ 🤔", reply_markup=kb)
    await call.answer()

@router.callback_query(F.data.startswith("cancel_delete_"))
async def process_cancel_deletion(call: CallbackQuery, state: FSMContext, db: D1Database, bot: Bot):
    folder = call.data.split("_")[2]
    await call.answer("✅ عملیات لغو شد.", show_alert=True)
    try:
        await call.message.delete()
    except Exception:
        pass
        
    state_data = await state.get_data()
    lst = state_data.get("current_list", [])
    idx = state_data.get("current_index", 0)
    
    if lst and idx < len(lst):
        post_id = lst[idx]
        p_rows = await db.execute("SELECT text, file_id, media_type FROM posts WHERE id = ?", [post_id])
        if p_rows:
            kb = get_saved_folder_pagination_kb(post_id, folder, idx)
            await send_post_content(bot, call.message.chat.id, p_rows[0], kb)
    else:
        user_id = call.from_user.id
        rows = await db.execute(
            """SELECT posts.id FROM saves JOIN posts ON saves.post = posts.id
               WHERE saves.user = ? AND saves.folder = ? AND posts.deleted = 0
               ORDER BY posts.id DESC LIMIT 30""",
            [user_id, folder]
        )
        if rows:
            post_id = rows[0]["id"]
            kb = get_saved_folder_pagination_kb(post_id, folder, 0)
            await send_post_content(bot, call.message.chat.id, rows[0], kb)
        else:
            await call.message.answer("📭 این پوشه خالی شد.")

@router.callback_query(F.data.startswith("f_del_save_"))
async def process_f_del_save(call: CallbackQuery, state: FSMContext, db: D1Database, bot: Bot):
    parts = call.data.split("_")
    post_id = int(parts[3])
    folder = parts[4]
    user_id = call.from_user.id
    
    try:
        await db.execute("DELETE FROM saves WHERE user = ? AND post = ?", [user_id, post_id])
        await call.answer("🗑️ مطلب با موفقیت حذف شد!", show_alert=True)
        try:
            await call.message.delete()
        except Exception:
            pass
            
        rows = await db.execute(
            """SELECT posts.id FROM saves JOIN posts ON saves.post = posts.id
               WHERE saves.user = ? AND saves.folder = ? AND posts.deleted = 0
               ORDER BY posts.id DESC LIMIT 30""",
            [user_id, folder]
        )
        if rows:
            post_ids = [r["id"] for r in rows]
            await state.update_data(current_list=post_ids, current_index=0)
            new_post_id = post_ids[0]
            p_rows = await db.execute("SELECT text, file_id, media_type FROM posts WHERE id = ?", [new_post_id])
            if p_rows:
                kb = get_saved_folder_pagination_kb(new_post_id, folder, 0)
                await send_post_content(bot, call.message.chat.id, p_rows[0], kb)
        else:
            folder_display = FOLDER_NAMES.get(folder, folder)
            await call.message.answer(f"📭 پوشه {folder_display} کاملاً خالی شد.")
    except Exception:
        await call.answer("❌ خطا در حذف", show_alert=True)

# ============================================================
# بخش‌های خلاصه لیست و مدیریت پست‌ها برای ادمین (Callback Queries)
# ============================================================
async def send_admin_all_posts_page(bot: Bot,chat_id:int,rows:List[Dict[str,Any]],page:int,total_pages:int,total_count:int,edit_message_id:Optional[int]=None):
    text="📋 <b>همه محتواها</b>\n\n"+f"صفحه {page+1}/{total_pages} · مجموع {total_count}\n"
    buttons=[]
    for p in rows:
        preview=html.escape(strip_html_text(p.get("text") or "")[:60].replace("\n"," "))
        text+=f"\n<b>#{p['id']}</b> {preview}"
        buttons.append([InlineKeyboardButton(text=f"✏️ #{p['id']}",callback_data=f"aedit_{p['id']}"),
                        InlineKeyboardButton(text=f"🗑 #{p['id']}",callback_data=f"adelete_{p['id']}")])
    buttons.append([InlineKeyboardButton(text="⏮",callback_data=f"adm_all_page_prev_{page}"),InlineKeyboardButton(text=f"{page+1}/{total_pages}",callback_data="noop"),InlineKeyboardButton(text="⏭",callback_data=f"adm_all_page_next_{page}")])
    buttons.append([InlineKeyboardButton(text="🔙 مدیریت محتوا",callback_data="admin_content")])
    kb=InlineKeyboardMarkup(inline_keyboard=buttons)
    if edit_message_id:
        try:
            await bot.edit_message_text(chat_id=chat_id,message_id=edit_message_id,text=text,parse_mode="HTML",reply_markup=kb); return
        except Exception: pass
    await bot.send_message(chat_id,text,parse_mode="HTML",reply_markup=kb)

@router.callback_query(F.data == "adm_view_all")
async def callback_admin_view_all(call: CallbackQuery):
    await call.message.edit_text("📋 <b>همه محتواها</b>\n\nبرای نمایش ادامه بده:",parse_mode="HTML",reply_markup=get_admin_view_all_confirm_kb())
    await call.answer()

@router.callback_query(F.data == "adm_view_all_cancel")
async def callback_admin_view_all_cancel(call: CallbackQuery):
    await call.message.edit_text("📁 <b>مدیریت محتوای هسته</b>",parse_mode="HTML",reply_markup=get_content_management_kb())
    await call.answer()

@router.callback_query(F.data == "adm_view_all_confirm")
async def callback_admin_view_all_confirm(call:CallbackQuery,state:FSMContext,db:D1Database,bot:Bot):
    per_page=10
    total=(await db.execute("SELECT COUNT(*) c FROM posts WHERE deleted=0"))[0].get("c",0)
    pages=max(1,math.ceil(total/per_page))
    rows=await db.execute("SELECT id,text,likes,dislikes,views,file_id,media_type FROM posts WHERE deleted=0 ORDER BY id DESC LIMIT ? OFFSET ?",[per_page,0])
    await state.set_state(BotStates.admin_view_all); await state.update_data(all_posts_page=0,all_per_page=per_page,all_total_pages=pages,all_total_count=total)
    if rows: await send_admin_all_posts_page(bot,call.message.chat.id,rows,0,pages,total,call.message.message_id)
    else: await call.message.edit_text("📭 محتوایی وجود ندارد.",reply_markup=get_content_management_kb())
    await call.answer()

@router.callback_query(F.data.startswith("adm_all_page_"))
async def callback_admin_all_posts_page(call:CallbackQuery,state:FSMContext,db:D1Database,bot:Bot):
    parts=call.data.split("_"); direction=parts[3]; current=int(parts[4]); data=await state.get_data()
    per=int(data.get("all_per_page",10)); total_pages=int(data.get("all_total_pages",1)); total=int(data.get("all_total_count",0))
    new=max(0,min(current+(1 if direction=="next" else -1),total_pages-1))
    rows=await db.execute("SELECT id,text,likes,dislikes,views,file_id,media_type FROM posts WHERE deleted=0 ORDER BY id DESC LIMIT ? OFFSET ?",[per,new*per])
    await state.update_data(all_posts_page=new)
    if rows: await send_admin_all_posts_page(bot,call.message.chat.id,rows,new,total_pages,total,call.message.message_id)
    await call.answer()

@router.callback_query(F.data == "adm_search_text")
async def callback_admin_search_text(call:CallbackQuery,state:FSMContext):
    await state.set_state(BotStates.admin_search_word); await state.update_data(search_ids=[],search_index=0)
    await call.message.edit_text("🔍 <b>جستجو</b>\n\nکلمه کلیدی یا شماره پست را بفرست.",parse_mode="HTML",reply_markup=get_exit_menu())
    await call.answer()

@router.message(F.chat.id==ADMIN_ID,StateFilter(BotStates.admin_search_word))
async def process_admin_search_word(message:Message,state:FSMContext,db:D1Database,bot:Bot):
    q=(message.text or "").strip()
    if not q: return
    rows=await db.execute("SELECT id FROM posts WHERE id=? AND deleted=0" if q.isdigit() else "SELECT id FROM posts WHERE text LIKE ? AND deleted=0 ORDER BY id DESC LIMIT 50",[int(q)] if q.isdigit() else [f"%{q}%"])
    ids=[r["id"] for r in rows]
    if not ids:
        await message.answer("❌ چیزی پیدا نشد.",reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔍 دوباره",callback_data="adm_search_text"),InlineKeyboardButton(text="🔙 مدیریت محتوا",callback_data="admin_content")]])); return
    await state.update_data(search_ids=ids,search_index=0); await state.set_state(BotStates.admin_search_word)
    p=await db.execute("SELECT id,text,file_id,media_type FROM posts WHERE id=?",[ids[0]])
    if p: await bot.send_message(message.chat.id,"نتیجه:",reply_markup=get_admin_search_pagination_kb(ids[0],0)); await send_post_content(bot,message.chat.id,p[0],get_admin_search_pagination_kb(ids[0],0))

@router.callback_query(F.data.startswith("asearch_"))
async def callback_admin_search_pagination(call:CallbackQuery,state:FSMContext,db:D1Database,bot:Bot):
    parts=call.data.split("_"); direction=parts[1]; current=int(parts[2]); data=await state.get_data(); ids=data.get("search_ids",[])
    if not ids: await call.answer("جستجو تمام شده است",show_alert=True); return
    new=max(0,min(current+(1 if direction=="next" else -1),len(ids)-1)); await state.update_data(search_index=new)
    p=await db.execute("SELECT id,text,file_id,media_type FROM posts WHERE id=?",[ids[new]])
    if p:
        kb=get_admin_search_pagination_kb(ids[new],new)
        if p[0].get("file_id"):
            try: await call.message.delete()
            except Exception: pass
            await send_post_content(bot,call.message.chat.id,p[0],kb)
        else:
            await call.message.edit_text(p[0].get("text") or "",reply_markup=kb)
    await call.answer()

@router.callback_query(F.data.startswith("aedit_"))
async def admin_edit_post_start(call:CallbackQuery,state:FSMContext,db:D1Database):
    pid=int(call.data.split("_")[1]); rows=await db.execute("SELECT id,text FROM posts WHERE id=? AND deleted=0",[pid])
    if not rows: await call.answer("پست پیدا نشد",show_alert=True); return
    await state.set_state(BotStates.admin_post_edit); await state.update_data(edit_post_id=pid,parent_message_id=call.message.message_id)
    await call.message.edit_text(f"✏️ <b>ویرایش #{pid}</b>\n\nمتن جدید را بفرست.",parse_mode="HTML",reply_markup=get_exit_menu()); await call.answer()

@router.message(F.chat.id==ADMIN_ID,StateFilter(BotStates.admin_post_edit))
async def admin_edit_post_input(message:Message,state:FSMContext,db:D1Database):
    data=await state.get_data(); pid=int(data["edit_post_id"]); new_text=message.text or message.caption or ""
    if not new_text: await message.answer("❌ متن خالی است."); return
    await db.execute("UPDATE posts SET text=? WHERE id=?",[new_text,pid]); await state.set_state(BotStates.idle)
    await message.answer(f"✅ پست #{pid} ویرایش شد.",reply_markup=get_content_management_kb())

@router.callback_query(F.data.startswith("astats_"))
async def callback_admin_search_post_stats(call:CallbackQuery,db:D1Database):
    pid=int(call.data.split("_")[1]); p=await db.execute("SELECT likes,dislikes,views FROM posts WHERE id=?",[pid])
    if not p: await call.answer("پست پیدا نشد",show_alert=True); return
    await call.answer(f"👁 {p[0].get('views',0)} | 👍 {p[0].get('likes',0)} | 👎 {p[0].get('dislikes',0)}",show_alert=True)

@router.callback_query(F.data.startswith("adelete_"))
async def callback_admin_delete_post(call:CallbackQuery,db:D1Database):
    pid=int(call.data.split("_")[1]); p=await db.execute("SELECT text FROM posts WHERE id=? AND deleted=0",[pid])
    if not p: await call.answer("پست پیدا نشد",show_alert=True); return
    kb=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🗑️ تأیید حذف",callback_data=f"adelete_confirm_{pid}"),InlineKeyboardButton(text="↩️ لغو",callback_data="admin_content")]])
    await call.message.edit_text(f"⚠️ <b>حذف #{pid}</b>\n\nآیا مطمئنی؟",parse_mode="HTML",reply_markup=kb); await call.answer()

@router.callback_query(F.data.startswith("adelete_confirm_"))
async def callback_admin_delete_post_confirm(call:CallbackQuery,db:D1Database):
    pid=int(call.data.split("_")[-1]); await db.execute("UPDATE posts SET deleted=1 WHERE id=?",[pid])
    await call.message.edit_text("🗑️ حذف شد.",reply_markup=get_content_management_kb()); await call.answer("حذف شد")


# ============================================================
# تایید ارسال‌های ادمین و راهنما (Callback Queries)
# ============================================================
@router.callback_query(F.data == "help_more")
async def callback_help_more(call: CallbackQuery):
    first_name = call.from_user.first_name or "دوست"
    detailed_text = f"""ببین {first_name} جان، داستان از این قراره! 🤖

ما اینجا یه پایگاه داده خفن و پویا از دنیای تکنولوژی، هوش مصنوعی و امنیت سایبری ساختیم.\n\n🚀 حتماً دیدی که تو کانال @TechNowAi بعضی وقتا فقط یه خلاصه کوچیک از خبرا یا آموزش‌ها رو می‌ذاریم؛ دلیلش اینه که تلگرام شلوغ نشه! اما وقتی روی لینک‌ها می‌زنی، دقیقاً هدایت میشی به همینجا تا متن کامل، جامع و تخصصی رو با خیال راحت مطالعه کنی. 📖🧐

حالا اینجا چه امکاناتی داری؟

۱. 💡 رأی‌گیری هوشمند: می‌تونی به هر پست رأی (👍 یا 👎) بدی. اینجوری ما می‌فهمیم سلیقه‌ات چیه و مطالب بهتری برات آماده می‌کنیم!

۲. 📂 پوشه‌بندی اختصاصی: یه مطلب خیلی برات کاربردی بود؟ با یه کلیک 💾 ذخیره‌اش کن و دقیقاً بندازش تو پوشه مخصوص خودش (مثلاً هوش مصنوعی یا امنیت) تا هر وقت بهش نیاز داشتی، تو سه‌سوت پیداش کنی!

۳. 🔍 جستجوی پیشرفته: تو پوشه‌هات دنبال یه کلمه خاص می‌گردی؟ دکمه جستجو رو بزن و کلمه‌ات رو بفرست تا ربات کل آرشیوت رو زیر و رو کنه!

۴. 🤖 هوش مصنوعی: با انتخاب دکمه هوش مصنوعی از منو، میتونی سوالاتت رو بپرسی یا فایل متنی/کد بفرستی تا پردازش بشه.

۵. 💬 ارتباط مستقیم: اگه ایده جذابی داشتی یا جایی به مشکل خوردی، مستقیم با خود مدیریت چت کن.

🔄 هر جا حس کردی ربات یه ذره گیج می‌زنه، فقط یه /start بفرست تا مثل روز اول سرحال بشه! ⚡️

📜 یادت نره که این ابزار برای پیشرفت و یادگیری راحت‌تر تو طراحی شده، پس حسابی ازش استفاده کن! 🎯

📌 راستی این ربات مخصوص کانال خودمون هست و همینجا کارایی داره خلاصه که ما تلاش میکنیم تا در کمک به علوم فناوری و هوش مصنوعی و امنیت سایبری برای فارسی‌زبانان سهیم باشیم ❤️

نسخه ربات: v1.5.0 🏷️"""
    try:
        await call.message.edit_text(text=detailed_text, reply_markup=get_help_got_it_kb())
    except Exception:
        pass
    await call.answer()

@router.callback_query(F.data == "help_got_it")
async def callback_help_got_it(call: CallbackQuery):
    try:
        await call.message.delete()
    except Exception:
        pass
    await call.answer("🚀 بزن بریم سراغ یادگیری!")

@router.callback_query(F.data == "conf_add_yes")
async def callback_confirm_add_post_yes(call: CallbackQuery, state: FSMContext, db: D1Database):
    state_data = await state.get_data()
    temp_text = state_data.get("temp_text")
    temp_file_id = state_data.get("temp_file_id")
    temp_media_type = state_data.get("temp_media_type")
    
    if temp_text or temp_file_id:
        try:
            res = await db.execute(
                "INSERT INTO posts(text, file_id, media_type) VALUES(?, ?, ?) RETURNING id",
                [temp_text, temp_file_id, temp_media_type]
            )
            post_id = None
            if res and isinstance(res, list) and len(res) > 0:
                post_id = res[0].get("id")
                
            if not post_id:
                last_id_rows = await db.execute("SELECT last_insert_rowid() as id")
                if last_id_rows:
                    post_id = last_id_rows[0].get("id")
                    
            await state.update_data(temp_text=None, temp_file_id=None, temp_media_type=None)
            await state.set_state(BotStates.idle)
            
            await call.message.answer(f"✅ آرشیو شد!\n🔗 لینک:\nhttps://t.me/{BOT_USERNAME}?start={post_id}")
            await call.answer("✅ ثبت شد!")
        except Exception as e:
            await call.answer(f"❌ خطا در ثبت: {e}", show_alert=True)
    else:
        await call.answer("❌ اطلاعات ناقص است", show_alert=True)

@router.callback_query(F.data == "conf_add_no")
async def callback_confirm_add_post_no(call: CallbackQuery, state: FSMContext):
    await state.update_data(temp_text=None, temp_file_id=None, temp_media_type=None)
    await state.set_state(BotStates.idle)
    await call.message.answer("❌ لغو شد.")
    await call.answer("لغو شد")

@router.callback_query(F.data == "conf_broad_yes")
async def callback_confirm_broadcast_yes(call: CallbackQuery, state: FSMContext, db: D1Database, bot: Bot):
    state_data = await state.get_data()
    temp_text = state_data.get("temp_text")
    temp_file_id = state_data.get("temp_file_id")
    temp_media_type = state_data.get("temp_media_type")
    
    if not temp_text and not temp_file_id:
        await call.answer("❌ اطلاعات ناقص است", show_alert=True)
        return
        
    users = await db.execute("SELECT id FROM users")
    if not users:
        await call.message.answer("⚠️ هیچ کاربری در دیتابیس وجود ندارد.")
        await call.answer()
        return
        
    await call.answer("🚀 ارسال همگانی شروع شد...")
    success_count, fail_count = 0, 0
    CHUNK_SIZE = 20
    
    async def send_to_user(bot_instance: Bot, uid: int, text: str, file: str, mtype: str):
        caption = text if len(text) <= 1024 else text[:1020] + "..."
        try:
            if mtype == "photo" and file:
                await bot_instance.send_photo(chat_id=uid, photo=file, caption=caption)
            elif mtype == "document" and file:
                await bot_instance.send_document(chat_id=uid, document=file, caption=caption)
            elif mtype == "video" and file:
                await bot_instance.send_video(chat_id=uid, video=file, caption=caption)
            elif mtype == "audio" and file:
                await bot_instance.send_audio(chat_id=uid, audio=file, caption=caption)
            else:
                safe_text = text if len(text) <= 4096 else text[:4090] + "..."
                await bot_instance.send_message(chat_id=uid, text=safe_text or "پیام همگانی")
            return True
        except Exception:
            return False

    for i in range(0, len(users), CHUNK_SIZE):
        chunk = users[i:i+CHUNK_SIZE]
        tasks = [send_to_user(bot, u["id"], temp_text, temp_file_id, temp_media_type) for u in chunk]
        results = await asyncio.gather(*tasks)
        success_count += sum(1 for r in results if r)
        fail_count += sum(1 for r in results if not r)
        await asyncio.sleep(0.1)
        
    await state.update_data(temp_text=None, temp_file_id=None, temp_media_type=None)
    await state.set_state(BotStates.idle)
    
    await call.message.answer(f"✅ ارسال همگانی انجام شد.\nموفق: {success_count} نفر\nناموفق: {fail_count} نفر")
    await call.answer("✅ ارسال همگانی کامل شد!")

@router.callback_query(F.data == "conf_broad_no")
async def callback_confirm_broadcast_no(call: CallbackQuery, state: FSMContext):
    await state.update_data(temp_text=None, temp_file_id=None, temp_media_type=None)
    await state.set_state(BotStates.idle)
    await call.message.answer("❌ ارسال همگانی لغو شد.")
    await call.answer("لغو شد")

@router.callback_query(F.data == "noop")
async def callback_noop_dummy(call: CallbackQuery):
    await call.answer()

# ============================================================
# متد اجرایی اصلی ربات (Startup & Main Polling)
# ============================================================
async def main():
    if not API_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not configured")
    if not (CF_ACCOUNT_ID and CF_DATABASE_ID and CF_API_TOKEN):
        raise RuntimeError("Cloudflare D1 environment variables are not fully configured")
    bot = Bot(token=API_TOKEN)
    global BOT_USERNAME
    try:
        bot_identity = await bot.get_me()
        if bot_identity.username:
            BOT_USERNAME = bot_identity.username
    except Exception:
        pass
    dp = Dispatcher(storage=MemoryStorage())
    
    db = D1Database(
        account_id=CF_ACCOUNT_ID,
        database_id=CF_DATABASE_ID,
        api_token=CF_API_TOKEN
    )
    await db.start()
    await get_http_session()
    dp["db"] = db
    
    await initialize_database(db)
    await initialize_automation_database(db)
    
    router.message.outer_middleware(RateLimitMiddleware(ADMIN_ID))
    router.callback_query.outer_middleware(RateLimitMiddleware(ADMIN_ID))
    dp.include_router(router)
    
    automation_task = asyncio.create_task(automation_loop(db, bot))
    logger.info("Bot started successfully in Long Polling mode with content automation...")
    try:
        await dp.start_polling(bot)
    finally:
        automation_task.cancel()
        try:
            await automation_task
        except asyncio.CancelledError:
            pass
        await db.close()
        await close_http_session()
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
