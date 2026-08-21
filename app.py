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
    InlineKeyboardButton
)

# بارگذاری متغیرهای محیطی در صورت وجود فایل .env
load_dotenv()

# ============================================================
# بخش تنظیمات و متغیرهای سراسری (Configuration)
# ============================================================
API_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
BOT_USERNAME = os.getenv("BOT_USERNAME", "TechNowAibot")
BUILD_VERSION = "3.0.0-admin-ux"

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
        "last_cleanup_at": "",
        "last_manual_channel_post_at": "",
        "channel_id": CHANNEL_ID,
        "channel_username": "",
        "max_workers": "2",
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
    rows = await db.execute("SELECT value FROM automation_settings WHERE key = ?", [key])
    return str(rows[0].get("value")) if rows else default


async def set_setting(db: D1Database, key: str, value: str):
    await db.execute("INSERT OR REPLACE INTO automation_settings(key, value) VALUES(?, ?)", [key, value])


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
    headers = {"User-Agent": HTTP_USER_AGENT, "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}
    timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SECONDS)
    async with session.get(url, headers=headers, timeout=timeout, allow_redirects=True) as resp:
        if resp.status >= 400:
            raise RuntimeError(f"HTTP {resp.status}")
        data = await resp.content.read(MAX_HTTP_BYTES + 1)
        if len(data) > MAX_HTTP_BYTES:
            raise RuntimeError("response too large")
        ctype = resp.headers.get("Content-Type", "")
        enc = resp.charset or "utf-8"
        return data.decode(enc, errors="ignore"), ctype


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


async def discover_source_items(source: Dict[str, Any]) -> List[Dict[str, Any]]:
    base = normalize_url(source.get("url", ""))
    session = await get_http_session()
    candidate_urls = []
    if source.get("feed_url"):
        try:
            feed_text, _ = await http_get(source["feed_url"], session)
            feed_items = parse_feed(feed_text, base)
            if feed_items:
                return feed_items[:MAX_SOURCE_ITEMS_PER_CYCLE]
        except Exception as e:
            logger.info("configured feed failed for %s: %s", base, e)
    for feed_path in ["/feed", "/rss", "/rss.xml", "/feed.xml", "/atom.xml"]:
        try:
            candidate = urllib.parse.urljoin(base + "/", feed_path.lstrip("/"))
            text, ctype = await http_get(candidate, session)
            feed_items = parse_feed(text, candidate)
            if feed_items:
                return feed_items[:MAX_SOURCE_ITEMS_PER_CYCLE]
        except Exception:
            continue
    try:
        site_text, _ = await http_get(base, session)
        parsed = extract_html_page(site_text, base)
        html_candidates = article_candidates_from_html(parsed, base)
        if html_candidates:
            return html_candidates
    except Exception as e:
        raise RuntimeError(f"source fetch failed: {e}")
    sitemap = urllib.parse.urljoin(base + "/", "sitemap.xml")
    try:
        sm_text, _ = await http_get(sitemap, session)
        root = ET.fromstring(sm_text)
        urls = []
        for loc in root.iter():
            if local_name(loc.tag) == "loc" and loc.text:
                urls.append(normalize_url(loc.text.strip()))
        for u in urls[:MAX_SOURCE_ITEMS_PER_CYCLE]:
            candidate_urls.append(u)
        results = []
        for u in candidate_urls:
            try:
                text, _ = await http_get(u, session)
                parsed = extract_html_page(text, u)
                results.append({"title": parsed["title"], "url": parsed["canonical_url"] or u, "description": parsed["description"], "body": parsed["body"][:12000], "image_url": parsed["image_url"], "published_at": ""})
            except Exception:
                continue
        return results[:MAX_SOURCE_ITEMS_PER_CYCLE]
    except Exception:
        return []

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
    """مدیریت Providerهای OpenAI-compatible با تست، cooldown و failover بدون قفل سراسری."""
    def __init__(self, db: D1Database, bot: Optional[Bot] = None):
        self.db = db
        self.bot = bot
        self._session: Optional[aiohttp.ClientSession] = None
        self._notify_lock = asyncio.Lock()
        self._last_final_notice = 0.0

    async def start(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=90))

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None

    async def providers(self) -> List[Dict[str, Any]]:
        return await self.db.execute("SELECT * FROM ai_providers WHERE enabled = 1 AND (status IS NULL OR status != 'invalid') ORDER BY priority ASC, id ASC")

    @staticmethod
    def endpoint(url: str) -> str:
        url = (url or '').strip()
        if not url:
            return ''
        # مدیر می‌تواند endpoint کامل یا Base URL رایج OpenAI-compatible بدهد.
        if re.search(r'/chat/completions/?$', url, re.I):
            return url.rstrip('/')
        if url.endswith('/'):
            url = url[:-1]
        return url + '/chat/completions'

    async def _request(self, provider: Dict[str, Any], messages: List[Dict[str, str]], temperature: float, max_tokens: int) -> Tuple[str, Dict[str, Any], int]:
        await self.start()
        key = decrypt_secret(provider.get('encrypted_api_key') or '')
        model = (provider.get('model_name') or '').strip()
        endpoint = self.endpoint(provider.get('base_url') or '')
        payload = {'model': model, 'messages': messages, 'temperature': temperature, 'max_tokens': max_tokens}
        headers = {'Authorization': f'Bearer {key}', 'Content-Type': 'application/json', 'User-Agent': HTTP_USER_AGENT}
        started = time.perf_counter()
        async with self._session.post(endpoint, headers=headers, json=payload) as resp:
            raw = await resp.text()
            latency = int((time.perf_counter() - started) * 1000)
            if resp.status != 200:
                raise RuntimeError(f'HTTP {resp.status}: {raw[:700]}')
            try:
                data = json.loads(raw)
            except Exception:
                raise RuntimeError('پاسخ JSON معتبر نبود')
            content = ((data.get('choices') or [{}])[0].get('message') or {}).get('content') or ''
            if not content:
                raise RuntimeError('پاسخ مدل خالی بود')
            return content, data, latency

    async def test_provider_values(self, base_url: str, api_key: str, model: str) -> Dict[str, Any]:
        provider = {'base_url': base_url, 'encrypted_api_key': encrypt_secret(api_key), 'model_name': model}
        try:
            content, data, latency = await self._request(provider, [{'role':'user','content':'Reply with exactly: OK'}], 0, 8)
            return {'ok': True, 'latency_ms': latency, 'preview': content.strip()[:80], 'usage': data.get('usage') or {}}
        except Exception as e:
            return {'ok': False, 'error': str(e)[:900]}

    async def test_provider(self, provider_id: int) -> Dict[str, Any]:
        rows = await self.db.execute('SELECT * FROM ai_providers WHERE id=?', [provider_id])
        if not rows:
            return {'ok': False, 'error': 'Provider یافت نشد'}
        p = rows[0]
        result = await self.test_provider_values(p.get('base_url',''), decrypt_secret(p.get('encrypted_api_key','')), p.get('model_name',''))
        now = datetime.now(timezone.utc).isoformat()
        if result['ok']:
            await self.db.execute("UPDATE ai_providers SET status='healthy', last_error=NULL, cooldown_until=NULL, last_checked_at=?, last_latency_ms=?, consecutive_failures=0, updated_at=? WHERE id=?", [now, result.get('latency_ms',0), now, provider_id])
        else:
            await self.db.execute("UPDATE ai_providers SET status='invalid', last_error=?, last_checked_at=?, updated_at=? WHERE id=?", [result.get('error','')[:1200], now, now, provider_id])
        return result

    async def _notify_failure(self, provider: Dict[str, Any], msg: str, purpose: str, final: bool = False):
        # فقط شکست نهایی یا خطای پیکربندی را به مدیر گزارش می‌کنیم؛ 429/503های موقت spam نمی‌سازند.
        if not self.bot or not ADMIN_ID:
            return
        if not final and not any(x in msg.lower() for x in ('404', '401', '403')):
            return
        text = ('🚨 خطای AI\n' if final else '⚠️ Provider AI غیرفعال شد\n') + \
               f"Provider: {html.escape(str(provider.get('name')))}\nModel: {html.escape(str(provider.get('model_name')))}\nReason: {html.escape(msg[:700])}"
        try:
            async with self._notify_lock:
                await self.bot.send_message(ADMIN_ID, text)
        except Exception:
            pass

    @staticmethod
    def _classify_error(msg: str) -> str:
        m = msg.lower()
        if '404' in m or 'model_not_found' in m:
            return 'invalid'
        if '401' in m or '403' in m or 'api key' in m or 'authentication' in m:
            return 'invalid'
        if '429' in m or 'timeout' in m or 'timed out' in m or '503' in m or '502' in m or '504' in m:
            return 'temporary'
        return 'temporary'

    async def call(self, messages: List[Dict[str, str]], temperature: float = 0.2, max_tokens: int = 2500, purpose: str = 'generic') -> Dict[str, Any]:
        providers = await self.providers()
        if not providers:
            return {'content':'', 'provider':None, 'model':None, 'tokens':0, 'error':'هیچ مدل فعالی در پنل AI تنظیم نشده است.'}
        now = datetime.now(timezone.utc)
        errors = []
        tried = 0
        for provider in providers:
            cooldown = provider.get('cooldown_until') or ''
            if cooldown:
                try:
                    if datetime.fromisoformat(cooldown.replace('Z','+00:00')) > now:
                        continue
                except Exception:
                    pass
            tried += 1
            try:
                content, data, latency = await self._request(provider, messages, temperature, max_tokens)
                await self.db.execute("UPDATE ai_providers SET status='healthy', last_error=NULL, cooldown_until=NULL, last_checked_at=?, last_latency_ms=?, consecutive_failures=0, updated_at=? WHERE id=?", [datetime.now(timezone.utc).isoformat(), latency, datetime.now(timezone.utc).isoformat(), provider['id']])
                usage = data.get('usage') or {}
                await log_automation(self.db, 'INFO', 'ai_success', f"{purpose} | {provider.get('name')} | {provider.get('model_name')} | {latency}ms")
                return {'content':content, 'provider':provider.get('name'), 'model':provider.get('model_name'), 'tokens':usage.get('total_tokens',0), 'error':None}
            except Exception as e:
                msg = str(e)
                errors.append(f"{provider.get('name')}: {msg[:250]}")
                kind = self._classify_error(msg)
                if kind == 'invalid':
                    status = 'invalid'
                    cooldown_until = None
                else:
                    status = 'cooldown'
                    cooldown_until = (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()
                failures = int(provider.get('consecutive_failures') or 0) + 1
                was_invalid = (provider.get('status') == 'invalid')
                await self.db.execute("UPDATE ai_providers SET status=?, last_error=?, cooldown_until=?, consecutive_failures=?, last_checked_at=?, updated_at=? WHERE id=?", [status, msg[:1200], cooldown_until, failures, datetime.now(timezone.utc).isoformat(), datetime.now(timezone.utc).isoformat(), provider['id']])
                if status != 'invalid' or not was_invalid:
                    await self._notify_failure(provider, msg, purpose, final=False)
                continue
        if tried == 0:
            reason = 'تمام Providerها در cooldown هستند یا فعال نیستند.'
        else:
            reason = 'تمام Providerهای قابل استفاده خطا دادند.'
        final_msg = reason + (' | ' + ' | '.join(errors) if errors else '')
        await log_automation(self.db, 'ERROR', 'ai_all_providers_failed', final_msg[:2000])
        if purpose != 'user_chat' and time.time() - self._last_final_notice > 600:
            self._last_final_notice = time.time()
            await self._notify_failure({'name':'AI System','model':'-'}, final_msg, purpose, final=True)
        return {'content':'', 'provider':None, 'model':None, 'tokens':0, 'error':final_msg}


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


async def fetch_source_cycle(db: D1Database, source: Dict[str, Any], ai: AIProviderManager):
    source_id = source["id"]
    now = datetime.now(timezone.utc)
    try:
        items = await discover_source_items(source)
        await db.execute("UPDATE sources SET last_checked_at = ?, next_check_at = ?, last_error = NULL WHERE id = ?", [now.isoformat(), (now + timedelta(minutes=int(source.get('interval_minutes') or DEFAULT_SOURCE_INTERVAL_MINUTES))).isoformat(), source_id])
        recent_rows = await db.execute("SELECT title FROM articles WHERE status = 'published' ORDER BY id DESC LIMIT 20")
        recent_titles = [r.get("title", "") for r in recent_rows]
        analyzed = 0
        for raw in items[:MAX_SOURCE_ITEMS_PER_CYCLE]:
            item = await enrich_candidate_content(raw)
            url = normalize_url(item.get("url"))
            title = strip_html_text(item.get("title", ""))[:500]
            if not url or not title:
                continue
            if not heuristic_topic_match(title, item.get("description", ""), source.get("category", "tech")):
                continue
            existing = await db.execute("SELECT id FROM source_items WHERE source_id = ? AND canonical_url = ?", [source_id, url])
            if existing:
                continue
            # global duplicate checks
            global_dup = await db.execute("SELECT id FROM source_items WHERE content_hash = ? LIMIT 1", [text_hash(title + " " + (item.get("body") or item.get("description") or ""))])
            if global_dup:
                continue
            discovered = now.isoformat()
            content_hash = text_hash(title + " " + (item.get("body") or item.get("description") or ""))
            ins = await db.execute("INSERT INTO source_items(source_id, canonical_url, title, description, content, image_url, published_at, discovered_at, content_hash, status, category) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, 'analyzing', ?) RETURNING id", [source_id, url, title, item.get("description", "")[:2000], (item.get("body") or "")[:14000], item.get("image_url", "")[:1000], item.get("published_at", "")[:100], discovered, content_hash, source.get("category", "tech")])
            item_id = ins[0].get("id") if ins else 0
            analysis = await ai_analyze_candidate(ai, item, source, recent_titles)
            analyzed += 1
            score = float(analysis.get("score", 0) or 0)
            min_score = float(await get_setting(db, "min_content_score", str(DEFAULT_MIN_CONTENT_SCORE)))
            accept = bool(analysis.get("accept")) and score >= min_score and int(analysis.get("duplicate_risk", 0) or 0) < 7
            await db.execute("UPDATE source_items SET status = ?, score = ?, category = ?, last_error = ? WHERE id = ?", ["qualified" if accept else "rejected", score, analysis.get("category", source.get("category", "tech")), None if accept else str(analysis.get("why") or analysis.get("reason") or "score below threshold")[:1000], item_id])
            if not accept:
                continue
            generated = await ai_generate_content(ai, item, analysis, source)
            if generated.get("error"):
                await db.execute("UPDATE source_items SET status='error', last_error=? WHERE id=?", [str(generated.get("error"))[:1000], item_id])
                continue
            verify = await ai_verify_content(ai, item, generated)
            if not verify.get("ok") or float(verify.get("confidence", 0) or 0) < 80:
                await db.execute("UPDATE source_items SET status='rejected', last_error=? WHERE id=?", [json.dumps(verify, ensure_ascii=False)[:1000], item_id])
                continue
            title_out = strip_html_text(generated.get("title") or title)[:500]
            channel_text = strip_html_text(generated.get("channel_text") or generated.get("article_text")[:700])[:900]
            article_text = strip_html_text(generated.get("article_text") or "")
            if len(article_text) < 500:
                await db.execute("UPDATE source_items SET status='rejected', last_error='article too short' WHERE id=?", [item_id])
                continue
            ins_art = await db.execute("INSERT INTO articles(source_item_id, title, channel_text, body, source_url, image_url, category, score, status, created_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?, 'ready', ?) RETURNING id", [item_id, title_out, channel_text, article_text[:18000], url, item.get("image_url", ""), generated.get("category") or analysis.get("category") or source.get("category", "tech"), score, now.isoformat()])
            article_id = ins_art[0].get("id") if ins_art else 0
            deep_token = make_deep_token(int(article_id))
            await db.execute("UPDATE articles SET deep_token = ? WHERE id = ?", [deep_token, article_id])
            await db.execute("UPDATE source_items SET status='ready', article_id=? WHERE id=?", [article_id, item_id])
            await db.execute("INSERT OR IGNORE INTO publication_queue(article_id, scheduled_at, status, attempts, created_at) VALUES(?, ?, 'queued', 0, ?)", [article_id, now.isoformat(), now.isoformat()])
            recent_titles.append(title_out)
        return analyzed
    except Exception as e:
        await db.execute("UPDATE sources SET last_checked_at = ?, next_check_at = ?, last_error = ? WHERE id = ?", [now.isoformat(), (now + timedelta(minutes=int(source.get('interval_minutes') or DEFAULT_SOURCE_INTERVAL_MINUTES))).isoformat(), str(e)[:1200], source_id])
        await log_automation(db, "ERROR", "source_cycle_failed", f"source={source_id} {e}")
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
    count_rows = await db.execute("SELECT COUNT(*) as c FROM articles WHERE status='published' AND created_at >= ?", [tehran.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc).isoformat()])
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


async def publish_next_article(db: D1Database, bot: Bot) -> bool:
    if not await can_publish_now(db):
        return False
    rows = await db.execute("SELECT q.id as queue_id, q.article_id, a.* FROM publication_queue q JOIN articles a ON a.id=q.article_id WHERE q.status='queued' AND a.status='ready' ORDER BY a.score DESC, q.created_at ASC LIMIT 1")
    if not rows:
        return False
    row = rows[0]
    queue_id = row["queue_id"]
    article_id = row["article_id"]
    await db.execute("UPDATE publication_queue SET status='publishing', attempts=attempts+1 WHERE id=?", [queue_id])
    try:
        token = row.get("deep_token")
        bot_username = BOT_USERNAME.lstrip("@")
        deep_link = f"https://t.me/{bot_username}?start=auto_{token}"
        channel_id = await get_channel_id(db)
        channel_text = html.escape(row.get("channel_text") or row.get("title") or "")
        channel_text += f"\n\n<a href=\"{html.escape(deep_link)}\">📖 بیشتر بخوانید</a>"
        image_url = row.get("image_url") or ""
        sent = None
        if image_url:
            try:
                sent = await bot.send_photo(chat_id=channel_id, photo=image_url, caption=channel_text[:1024], parse_mode="HTML")
            except Exception:
                sent = None
        if sent is None:
            sent = await bot.send_message(chat_id=channel_id, text=channel_text[:4096], parse_mode="HTML", disable_web_page_preview=False)
        published_at = datetime.now(timezone.utc).isoformat()
        await db.execute("UPDATE articles SET status='published', published_message_id=? WHERE id=?", [getattr(sent, "message_id", 0), article_id])
        await db.execute("UPDATE publication_queue SET status='published', published_at=? WHERE id=?", [published_at, queue_id])
        await log_automation(db, "INFO", "published", f"article={article_id} message={getattr(sent,'message_id',0)}")
        return True
    except Exception as e:
        await db.execute("UPDATE publication_queue SET status='failed', last_error=? WHERE id=?", [str(e)[:1500], queue_id])
        await db.execute("UPDATE articles SET status='ready' WHERE id=?", [article_id])
        await log_automation(db, "ERROR", "publication_failed", f"article={article_id} {e}")
        try:
            await bot.send_message(ADMIN_ID, f"❌ خطا در انتشار خودکار\nArticle: {article_id}\nError: {html.escape(str(e)[:800])}")
        except Exception:
            pass
        return False


async def automation_loop(db: D1Database, bot: Bot):
    ai = AIProviderManager(db, bot)
    last_cleanup = 0.0
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
                    if due_sources:
                        await asyncio.gather(*(run_source(src) for src in due_sources), return_exceptions=True)
                    # انتشار یک پست در هر tick؛ فاصله واقعی در can_publish_now کنترل می‌شود.
                    await publish_next_article(db, bot)
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
    published = await db.execute("SELECT COUNT(*) as c FROM articles WHERE status='published' AND created_at >= ?", [datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()])
    failed = await db.execute("SELECT COUNT(*) as c FROM publication_queue WHERE status='failed' AND created_at >= ?", [(datetime.now(timezone.utc) - timedelta(days=1)).isoformat()])
    channel = await get_channel_id(db)
    channel_label = await get_setting(db, 'channel_username', '') or channel
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
    state_text = "⏸ خاموش کردن" if enabled else "▶️ فعال کردن"
    state_cb = "auto_off" if enabled else "auto_on"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=state_text, callback_data=state_cb), InlineKeyboardButton(text="📢 کانال", callback_data="auto_channel")],
        [InlineKeyboardButton(text="🌐 منابع", callback_data="auto_sources"), InlineKeyboardButton(text="🤖 مدل‌های AI", callback_data="auto_providers")],
        [InlineKeyboardButton(text="🧠 قوانین محتوا", callback_data="auto_settings"), InlineKeyboardButton(text="📥 صف انتشار", callback_data="auto_queue")],
        [InlineKeyboardButton(text="📊 گزارش سلامت", callback_data="auto_report")],
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

def setting_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔢 سقف تقریبی پست روزانه", callback_data="set_max_daily")],
        [InlineKeyboardButton(text="⭐ حداقل امتیاز انتشار", callback_data="set_min_score")],
        [InlineKeyboardButton(text="⏱ حداقل فاصله پست‌ها", callback_data="set_min_gap")],
        [InlineKeyboardButton(text="🌐 فاصله بررسی منابع", callback_data="set_default_interval")],
        [InlineKeyboardButton(text="⚡ Workerهای همزمان", callback_data="set_workers")],
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
        [InlineKeyboardButton(text="🌐 منابع خبری", callback_data="admin_sources"), InlineKeyboardButton(text="🤖 مدل‌های هوش مصنوعی", callback_data="admin_ai")],
        [InlineKeyboardButton(text="📢 کانال و انتشار", callback_data="admin_publish"), InlineKeyboardButton(text="🧠 کیفیت و قوانین", callback_data="admin_quality")],
        [InlineKeyboardButton(text="📊 آمار و سلامت", callback_data="admin_monitor"), InlineKeyboardButton(text="📁 مدیریت محتوای هسته", callback_data="admin_content")],
        [InlineKeyboardButton(text="📢 ارسال همگانی", callback_data="admin_broadcast"), InlineKeyboardButton(text="➕ افزودن پست", callback_data="admin_add_post")],
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
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔍 جستجو بر اساس کلمات یا کد", callback_data="adm_search_text")],
            [InlineKeyboardButton(text="📋 نمایش خلاصه همه محتواها", callback_data="adm_view_all")]
        ]
    )

def get_admin_search_pagination_kb(post_id: int, index: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="⏮ قبلی", callback_data=f"asearch_prev_{index}"),
                InlineKeyboardButton(text="⏭ بعدی", callback_data=f"asearch_next_{index}")
            ],
            [
                InlineKeyboardButton(text="📊 آمار", callback_data=f"astats_{post_id}"),
                InlineKeyboardButton(text="🗑️ حذف", callback_data=f"adelete_{post_id}")
            ]
        ]
    )

def get_admin_all_posts_kb(posts: list, page: int, total_pages: int) -> InlineKeyboardMarkup:
    inline_keyboard = []
    stat_buttons = [
        InlineKeyboardButton(text=f"📊 #{p['id']}", callback_data=f"adm_all_stat_{p['id']}")
        for p in posts
    ]
    for i in range(0, len(stat_buttons), 3):
        inline_keyboard.append(stat_buttons[i:i+3])
        
    inline_keyboard.append([
        InlineKeyboardButton(text="⏮ قبلی", callback_data=f"adm_all_page_prev_{page}"),
        InlineKeyboardButton(text=f"📄 {page + 1}/{total_pages}", callback_data="noop"),
        InlineKeyboardButton(text="⏭ بعدی", callback_data=f"adm_all_page_next_{page}")
    ])
    return InlineKeyboardMarkup(inline_keyboard=inline_keyboard)

def get_admin_view_all_confirm_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ بله، همه رو نشون بده", callback_data="adm_view_all_confirm")],
            [InlineKeyboardButton(text="❌ خیر، لغو", callback_data="adm_view_all_cancel")]
        ]
    )

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
            article_rows = await db.execute("SELECT * FROM articles WHERE deep_token=? AND status='published'", [token])
            if article_rows:
                article = article_rows[0]
                text = f"<b>{html.escape(article.get('title') or '')}</b>\n\n{html.escape(article.get('body') or '')}\n\n<a href=\"{html.escape(article.get('source_url') or '')}\">🔗 منبع اصلی</a>"
                await db.execute("UPDATE posts SET views = views + 1 WHERE id = (SELECT id FROM posts WHERE text LIKE ? LIMIT 1)", [f"%{article.get('title') or ''}%"])
                for i in range(0, len(text), 3800):
                    await message.answer(text[i:i+3800], parse_mode="HTML", disable_web_page_preview=True)
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

@router.message(StateFilter(BotStates.admin_search_word))
async def process_admin_search_word(message: Message, state: FSMContext, db: D1Database, bot: Bot):
    if message.from_user.id != ADMIN_ID:
        return
        
    query_text = (message.text or "").strip()
    if not query_text:
        return
        
    results = []
    if query_text.isdigit():
        id_num = int(query_text)
        results = await db.execute("SELECT id FROM posts WHERE id = ? AND deleted = 0", [id_num])
    else:
        results = await db.execute(
            "SELECT id FROM posts WHERE text LIKE ? AND deleted = 0 ORDER BY id DESC LIMIT 50",
            [f"%{query_text}%"]
        )
        
    if not results:
        await message.answer("❌ پستی با این کلیدواژه یا کد پیدا نشد!\nدوباره امتحان کن:")
        return
        
    search_ids = [r["id"] for r in results]
    await state.update_data(search_ids=search_ids, search_index=0)
    
    first_post_id = search_ids[0]
    post_rows = await db.execute("SELECT text, file_id, media_type FROM posts WHERE id = ?", [first_post_id])
    if post_rows:
        kb = get_admin_search_pagination_kb(first_post_id, 0)
        await send_post_content(bot, message.chat.id, post_rows[0], kb)

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
    await message.answer("دستور ناشناس ❌\nلطفا از دکمه ها استفاده کنید 👇🏻")


# ============================================================
# پنل مدیریت اتوماسیون محتوا
# ============================================================

@router.message(F.chat.id == ADMIN_ID, StateFilter(BotStates.admin_add_source))
async def admin_add_source_input(message: Message, state: FSMContext, db: D1Database, bot: Bot):
    url = (message.text or "").strip()
    if not url or not re.match(r"^https?://", url, re.I):
        await message.answer("❌ URL معتبر نیست. نمونه:\nhttps://example.com")
        return
    try:
        source_id = await add_source(db, url)
        data = await state.get_data()
        panel_id = data.get('panel_message_id')
        await state.set_state(BotStates.idle)
        try:
            await message.delete()
        except Exception:
            pass
        rows = await db.execute("SELECT * FROM sources ORDER BY priority DESC, id DESC")
        if panel_id:
            try:
                await bot.edit_message_text(chat_id=message.chat.id, message_id=panel_id, text=f"✅ منبع اضافه شد.\n\nشناسه: {source_id}", reply_markup=source_list_kb(rows))
                return
            except Exception:
                pass
        await message.answer(f"✅ منبع با موفقیت اضافه شد.\nشناسه: {source_id}", reply_markup=source_list_kb(rows))
    except Exception as e:
        await message.answer(f"❌ افزودن منبع ناموفق بود:\n{html.escape(str(e))}")



@router.message(F.chat.id == ADMIN_ID, StateFilter(BotStates.admin_automation_setting))
async def admin_automation_setting_input(message: Message, state: FSMContext, db: D1Database):
    data = await state.get_data()
    key = data.get("automation_setting_key")
    value = (message.text or "").strip()
    try:
        if key == "__source_interval__":
            sid = int(data.get("source_interval_id"))
            value = str(max(1, int(value)))
            await db.execute("UPDATE sources SET interval_minutes=?, next_check_at=? WHERE id=?", [int(value), datetime.now(timezone.utc).isoformat(), sid])
            await state.set_state(BotStates.idle)
            await message.answer("✅ فاصله بررسی منبع تغییر کرد.", reply_markup=get_admin_menu())
            return
        if key == "__provider_priority__":
            pid = int(data.get("provider_priority_id"))
            value = str(max(1, int(value)))
            await db.execute("UPDATE ai_providers SET priority=?, updated_at=? WHERE id=?", [int(value), datetime.now(timezone.utc).isoformat(), pid])
            await state.set_state(BotStates.idle)
            rows = await db.execute("SELECT id,name,base_url,model_name,priority,enabled,status,last_error,last_latency_ms FROM ai_providers ORDER BY priority ASC, id ASC")
            await message.answer("✅ اولویت مدل تغییر کرد.", reply_markup=provider_list_kb(rows))
            return
        if key in {"max_daily_posts", "default_source_interval"}:
            value = str(max(1, int(value)))
        elif key == "max_workers":
            value = str(max(1, min(4, int(value))))
        elif key == "min_content_score":
            value = str(max(0.0, min(100.0, float(value))))
        elif key == "min_hours_between_posts":
            value = str(max(0.0, float(value)))
        else:
            raise ValueError("setting not supported")
        await set_setting(db, key, value)
        await state.set_state(BotStates.idle)
        await message.answer("✅ تنظیم ذخیره شد.", reply_markup=get_admin_menu())
    except Exception as e:
        await message.answer(f"❌ مقدار نامعتبر است: {e}")



async def render_admin_home(call: CallbackQuery, db: D1Database):
    report = await automation_report(db)
    text = "🛠 <b>پنل مدیریت</b>\n<code>Build: " + BUILD_VERSION + "</code>\n\n" + report + "\n\nیک بخش را انتخاب کن:" 
    await call.message.edit_text(text, parse_mode='HTML', reply_markup=get_admin_menu())
    await call.answer()


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
    channel = await get_channel_id(db)
    enabled = (await get_setting(db, 'automation_enabled', '0')) == '1'
    text = ("📢 <b>کانال و انتشار</b>\n\n"
            f"کانال: <code>{html.escape(channel or 'تنظیم نشده')}</code>\n"
            f"اتوماسیون: {'🟢 فعال' if enabled else '🔴 خاموش'}\n\n"
            "ربات پست‌های دستی مدیر را ثبت می‌کند و بین انتشار خودکار فاصله را رعایت می‌کند.")
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📢 تنظیم/تغییر کانال", callback_data="auto_channel")],
        [InlineKeyboardButton(text="⏱ تنظیمات انتشار", callback_data="auto_settings")],
        [InlineKeyboardButton(text="📥 صف انتشار", callback_data="auto_queue")],
        [InlineKeyboardButton(text="🔙 پنل اصلی", callback_data="admin_home")]
    ])
    await call.message.edit_text(text, parse_mode='HTML', reply_markup=kb)
    await call.answer()


@router.callback_query(F.data == "admin_quality")
async def admin_quality(call: CallbackQuery, db: D1Database):
    if call.from_user.id != ADMIN_ID: return
    await auto_settings(call, db)


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


@router.callback_query(F.data == "auto_channel")
async def auto_channel(call: CallbackQuery, state: FSMContext, db: D1Database):
    if call.from_user.id != ADMIN_ID: return
    current = await get_channel_id(db)
    text = ("📢 <b>تنظیم کانال انتشار</b>\n\n"
            f"کانال فعلی: <code>{html.escape(current or 'تنظیم نشده')}</code>\n\n"
            "آیدی کانال یا @username را بفرست.\n"
            "مثال: <code>@my_channel</code> یا <code>-1001234567890</code>\n\n"
            "ربات باید در کانال دسترسی لازم برای انتشار داشته باشد.")
    await state.set_state(BotStates.admin_channel_input)
    await state.update_data(panel_message_id=call.message.message_id)
    await call.message.edit_text(text, parse_mode='HTML', reply_markup=get_exit_menu())
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
            await message.answer('❌ ربات در این کانال ادمین نیست یا دسترسی کافی ندارد.')
            return
        await set_setting(db, 'channel_id', str(chat.id))
        await set_setting(db, 'channel_username', '@' + chat.username if getattr(chat, 'username', None) else '')
        await state.set_state(BotStates.idle)
        try: await message.delete()
        except Exception: pass
        label = '@' + chat.username if getattr(chat, 'username', None) else str(chat.id)
        await message.answer(f"✅ کانال با موفقیت تنظیم شد.\n\n📢 {html.escape(label)}\n🆔 <code>{chat.id}</code>", parse_mode='HTML', reply_markup=automation_menu_kb((await get_setting(db,'automation_enabled','0'))=='1'))
    except Exception as e:
        await message.answer(f"❌ نتوانستم کانال را تأیید کنم:\n{html.escape(str(e)[:1000])}\n\nآیدی/@username را دوباره بفرست.")


@router.callback_query(F.data == "cancel_state")
async def cancel_state(call: CallbackQuery, state: FSMContext, db: D1Database):
    await state.set_state(BotStates.idle)
    await state.update_data(panel_message_id=None, provider_base_url=None, provider_token=None, provider_edit_id=None)
    if call.from_user.id == ADMIN_ID:
        await render_admin_home(call, db)
    else:
        await call.message.edit_text("لغو شد.", reply_markup=get_main_menu())
        await call.answer('لغو شد')


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
async def auto_report(call: CallbackQuery, db: D1Database):
    await call.message.edit_text(await automation_report(db), reply_markup=automation_menu_kb((await get_setting(db,"automation_enabled","0"))=="1"))
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


@router.callback_query(F.data.startswith("source_interval_"))
async def source_interval(call: CallbackQuery, state: FSMContext):
    source_id = int(call.data.split("_")[-1])
    await state.update_data(source_interval_id=source_id)
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
        items = await discover_source_items(rows[0])
        await call.message.answer(f"✅ تست منبع موفق بود. {len(items)} محتوای احتمالی پیدا شد.")
    except Exception as e:
        await db.execute("UPDATE sources SET last_error=? WHERE id=?", [str(e)[:1000], source_id])
        await call.message.answer(f"❌ تست منبع ناموفق:\n{html.escape(str(e))}")


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
    await state.update_data(provider_draft={})
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
        await message.answer("❌ Base URL معتبر نیست. باید با http:// یا https:// شروع شود.")
        return
    data = await state.get_data()
    panel_id = data.get('panel_message_id')
    await state.update_data(provider_base_url=base_url)
    await state.set_state(BotStates.admin_provider_token)
    try:
        await message.delete()
    except Exception:
        pass
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
        await message.answer("❌ توکن خیلی کوتاه است. دوباره ارسال کنید.")
        return
    data = await state.get_data()
    panel_id = data.get('panel_message_id')
    await state.update_data(provider_token=token)
    await state.set_state(BotStates.admin_provider_model)
    try:
        await message.delete()
    except Exception:
        pass
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
        await message.answer("❌ نام مدل خالی است.")
        return
    panel_id = data.get('panel_message_id')
    if panel_id:
        try:
            await bot.edit_message_text(chat_id=message.chat.id, message_id=panel_id, text="🧪 در حال تست اتصال و نام دقیق مدل...\nاین مرحله فقط یک درخواست بسیار کوچک می‌فرستد.")
        except Exception:
            await message.answer("🧪 در حال تست اتصال و نام دقیق مدل...")
    else:
        await message.answer("🧪 در حال تست اتصال و نام دقیق مدل...")
    tester = AIProviderManager(db)
    try:
        result = await tester.test_provider_values(base_url, token, model)
    finally:
        await tester.close()
    if not result.get('ok'):
        await state.set_state(BotStates.idle)
        await state.update_data(provider_base_url=None, provider_token=None, provider_edit_id=None, panel_message_id=None)
        try:
            await message.delete()
        except Exception:
            pass
        await message.answer(
            "❌ این مدل در تست اولیه قبول نشد.\n\n"
            f"HTTP/API: {html.escape(str(result.get('error','unknown'))[:1000])}\n\n"
            "هیچ چیزی ذخیره نشد. Base URL یا نام دقیق مدل را بررسی کن.",
            reply_markup=get_admin_menu()
        )
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
    try:
        await message.delete()
    except Exception:
        pass
    rows = await db.execute("SELECT id,name,base_url,model_name,priority,enabled,status,last_error,last_latency_ms FROM ai_providers ORDER BY priority ASC, id ASC")
    await message.answer(
        f"✅ {action_text}\n\n🤖 Model: <code>{html.escape(model)}</code>\n⚡ زمان پاسخ تست: {result.get('latency_ms',0)}ms\n🔢 اولویت: {priority}",
        parse_mode='HTML', reply_markup=provider_list_kb(rows)
    )


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
            "از صفحه هر مدل می‌توانی آن را ویرایش، تست، فعال/غیرفعال، اولویت‌بندی یا حذف کنی.")
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
    await state.update_data(provider_edit_id=pid, provider_base_url=None, provider_token=None, panel_message_id=call.message.message_id)
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
    await state.update_data(automation_setting_key="__provider_priority__", provider_priority_id=pid)
    await call.message.edit_text("🔢 اولویت این مدل را به عدد بفرست.\nعدد کمتر = اولویت بالاتر.", reply_markup=get_exit_menu())
    await call.answer()


@router.callback_query(F.data.startswith("provider_toggle_"))
async def provider_toggle(call: CallbackQuery, db: D1Database):
    pid = int(call.data.split("_")[-1])
    rows = await db.execute("SELECT enabled FROM ai_providers WHERE id=?", [pid])
    if rows:
        await db.execute("UPDATE ai_providers SET enabled=?, updated_at=? WHERE id=?", [0 if rows[0].get("enabled") else 1, datetime.now(timezone.utc).isoformat(), pid])
    await provider_view(call, db)


@router.callback_query(F.data.startswith("provider_delete_"))
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


@router.callback_query(F.data.startswith("provider_delete_confirm_"))
async def provider_delete_confirm(call: CallbackQuery, db: D1Database):
    pid = int(call.data.split("_")[-1])
    await db.execute("DELETE FROM ai_providers WHERE id=?", [pid])
    rows = await db.execute("SELECT id,name,base_url,model_name,priority,enabled,status,last_error,last_latency_ms FROM ai_providers ORDER BY priority ASC, id ASC")
    await call.message.edit_text("🗑️ <b>مدل حذف شد.</b>\n\nفهرست مدل‌ها:", parse_mode='HTML', reply_markup=provider_list_kb(rows))
    await call.answer("حذف شد")


@router.callback_query(F.data == "auto_settings")
async def auto_settings(call: CallbackQuery, db: D1Database):
    text = (await automation_report(db)) + "\n\nتنظیمات را با دکمه‌های زیر تغییر بده:"
    await call.message.edit_text(text, reply_markup=setting_menu_kb())
    await call.answer()


async def prompt_for_setting(call: CallbackQuery, state: FSMContext, key: str, label: str):
    await state.set_state(BotStates.admin_automation_setting)
    await state.update_data(automation_setting_key=key, panel_message_id=call.message.message_id)
    await call.message.edit_text(label, reply_markup=get_exit_menu())
    await call.answer()


@router.callback_query(F.data == "set_max_daily")
async def set_max_daily(call: CallbackQuery, state: FSMContext):
    await prompt_for_setting(call, state, "max_daily_posts", "🔢 تعداد تقریبی/حداکثر پست روزانه را به عدد بفرست. مثلاً 6")

@router.callback_query(F.data == "set_min_score")
async def set_min_score(call: CallbackQuery, state: FSMContext):
    await prompt_for_setting(call, state, "min_content_score", "⭐ حداقل امتیاز انتشار را بین 0 تا 100 بفرست. پیشنهاد: 75")

@router.callback_query(F.data == "set_min_gap")
async def set_min_gap(call: CallbackQuery, state: FSMContext):
    await prompt_for_setting(call, state, "min_hours_between_posts", "⏱ حداقل فاصله بین دو پست را بر حسب ساعت بفرست. مثلاً 2")

@router.callback_query(F.data == "set_default_interval")
async def set_default_interval(call: CallbackQuery, state: FSMContext):
    await prompt_for_setting(call, state, "default_source_interval", "🌐 فاصله بررسی پیش‌فرض منابع را بر حسب دقیقه بفرست. مثلاً 15")

@router.callback_query(F.data == "set_workers")
async def set_workers(call: CallbackQuery, state: FSMContext):
    await prompt_for_setting(call, state, "max_workers", "⚡ تعداد Workerهای همزمان را بین 1 تا 4 بفرست. پیشنهاد برای Railway کوچک: 2")

@router.callback_query(F.data == "auto_queue")
async def auto_queue(call: CallbackQuery, db: D1Database):
    rows = await db.execute("SELECT q.id, q.article_id, q.status, q.attempts, a.title, a.score, a.category FROM publication_queue q JOIN articles a ON a.id=q.article_id WHERE q.status='queued' ORDER BY a.score DESC LIMIT 20")
    text = "📥 صف انتشار\n\n"
    if not rows:
        text += "صف خالی است."
    else:
        for r in rows:
            text += f"#{r['article_id']} | {r['score']:.0f} | {r['category']} | {r['title'][:70]}\n"
    await call.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 بازگشت", callback_data="auto_back")]]))
    await call.answer()


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
@router.callback_query(F.data == "adm_view_all")
async def callback_admin_view_all(call: CallbackQuery):
    await call.message.answer("⚠️ این عمل تمام محتواها را به صورت خلاصه نمایش می‌دهد. ادامه می‌دهید؟", reply_markup=get_admin_view_all_confirm_kb())
    await call.answer()

@router.callback_query(F.data == "adm_view_all_cancel")
async def callback_admin_view_all_cancel(call: CallbackQuery):
    try:
        await call.message.delete()
    except Exception:
        pass
    await call.answer("لغو شد")

@router.callback_query(F.data == "adm_view_all_confirm")
async def callback_admin_view_all_confirm(call: CallbackQuery, state: FSMContext, db: D1Database, bot: Bot):
    try:
        await call.message.delete()
    except Exception:
        pass
    per_page = 10
    page = 0
    rows = await db.execute("SELECT id, text, likes, dislikes, views FROM posts WHERE deleted = 0 ORDER BY id DESC LIMIT ? OFFSET ?", [per_page, page * per_page])
    if not rows:
        await call.message.answer("📭 هیچ محتوایی در پایگاه داده وجود ندارد.")
        await call.answer()
        return
        
    counts = await db.execute("SELECT COUNT(*) as c FROM posts WHERE deleted = 0")
    total_count = counts[0].get("c", 0) if counts else 0
    total_pages = math.ceil(total_count / per_page) if total_count else 1
    
    await state.set_state(BotStates.admin_view_all)
    await state.update_data(all_posts_page=page, all_per_page=per_page, all_total_pages=total_pages, all_total_count=total_count)
    await send_admin_all_posts_page(bot, call.message.chat.id, rows, page, total_pages, total_count)
    await call.answer(f"📋 {total_count} پست یافت شد")

@router.callback_query(F.data.startswith("adm_all_page_"))
async def callback_admin_all_posts_page(call: CallbackQuery, state: FSMContext, db: D1Database, bot: Bot):
    parts = call.data.split("_")
    direction = parts[3]
    current_page = int(parts[4])
    
    state_data = await state.get_data()
    per_page = state_data.get("all_per_page", 10)
    total_pages = state_data.get("all_total_pages", 1)
    total_count = state_data.get("all_total_count", 0)
    
    new_page = current_page + 1 if direction == "next" else current_page - 1
    new_page = max(0, min(new_page, total_pages - 1))
    
    if new_page == current_page:
        await call.answer("🚧 انتهای لیست!")
        return
        
    await call.answer()
    await state.update_data(all_posts_page=new_page)
    rows = await db.execute("SELECT id, text, likes, dislikes, views FROM posts WHERE deleted = 0 ORDER BY id DESC LIMIT ? OFFSET ?", [per_page, new_page * per_page])
    if rows:
        await send_admin_all_posts_page(bot, call.message.chat.id, rows, new_page, total_pages, total_count, call.message.message_id)

@router.callback_query(F.data.startswith("adm_all_stat_"))
async def callback_admin_all_post_statistics(call: CallbackQuery, db: D1Database):
    post_id = int(call.data.split("_")[3])
    p_rows = await db.execute("SELECT likes, dislikes, views FROM posts WHERE id = ?", [post_id])
    if p_rows:
        p = p_rows[0]
        s_count = (await db.execute("SELECT COUNT(*) as c FROM saves WHERE post = ?", [post_id]))[0].get("c", 0)
        details = f"📊 آمار پست #{post_id}:\n👁️ بازدید: {p.get('views') or 0}\n👍 لایک: {p.get('likes') or 0}\n👎 دیس‌لایک: {p.get('dislikes') or 0}\n💾 ذخیره شده توسط: {s_count} نفر"
        await call.answer(details, show_alert=True)
    else:
        await call.answer("❌ پست یافت نشد")

@router.callback_query(F.data == "adm_search_text")
async def callback_admin_search_text(call: CallbackQuery, state: FSMContext):
    await state.set_state(BotStates.admin_search_word)
    await call.message.answer("🔍 کلیدواژه یا کد پست را وارد کنید:")
    await call.answer()

@router.callback_query(F.data.startswith("asearch_"))
async def callback_admin_search_pagination(call: CallbackQuery, state: FSMContext, db: D1Database, bot: Bot):
    parts = call.data.split("_")
    direction = parts[1]
    current_index = int(parts[2])
    
    state_data = await state.get_data()
    search_ids = state_data.get("search_ids", [])
    if search_ids:
        new_index = current_index + 1 if direction == "next" else current_index - 1
        new_index = max(0, min(new_index, len(search_ids) - 1))
        
        if new_index == current_index:
            await call.answer("🚧 انتهای لیست!")
        else:
            await call.answer()
            post_id = search_ids[new_index]
            await state.update_data(search_index=new_index)
            
            p_rows = await db.execute("SELECT text, file_id, media_type FROM posts WHERE id = ?", [post_id])
            if p_rows:
                post = p_rows[0]
                kb = get_admin_search_pagination_kb(post_id, new_index)
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

@router.callback_query(F.data.startswith("astats_"))
async def callback_admin_search_post_stats(call: CallbackQuery, db: D1Database):
    post_id = int(call.data.split("_")[1])
    p_rows = await db.execute("SELECT likes, dislikes, views FROM posts WHERE id = ?", [post_id])
    if p_rows:
        p = p_rows[0]
        s_count = (await db.execute("SELECT COUNT(*) as c FROM saves WHERE post = ?", [post_id]))[0].get("c", 0)
        details = f"📊 آمار پست:\n👁️ بازدید: {p.get('views') or 0}\n👍 لایک: {p.get('likes') or 0}\n👎 دیس‌لایک: {p.get('dislikes') or 0}\n💾 ذخیره شده توسط: {s_count} نفر"
        await call.answer(details, show_alert=True)
    else:
        await call.answer("❌ پست یافت نشد")

@router.callback_query(F.data.startswith("adelete_"))
async def callback_admin_delete_post(call: CallbackQuery, db: D1Database):
    post_id = int(call.data.split("_")[1])
    try:
        await db.execute("UPDATE posts SET deleted = 1 WHERE id = ?", [post_id])
        await call.answer("🗑️ پست با موفقیت حذف شد!", show_alert=True)
        try:
            await call.message.delete()
        except Exception:
            pass
    except Exception as e:
        await call.answer(f"❌ خطا: {e}", show_alert=True)

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