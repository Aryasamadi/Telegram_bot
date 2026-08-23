# VERSION 10.28.0 — FULL FIXED / OPERATIONAL / D1-FREE-PLAN + SPEED + QUOTA SAFE
# PART 1/4
import os, io, re, time, math, random, logging, asyncio, html, hashlib, json, urllib.parse
import struct, zlib
from html.parser import HTMLParser
from difflib import SequenceMatcher
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional, Tuple
import pytz, aiohttp
from dotenv import load_dotenv
from cryptography.fernet import Fernet, InvalidToken
from aiogram import Bot, Dispatcher, Router, F, BaseMiddleware
from aiogram.filters import Command, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (Message, CallbackQuery, TelegramObject, ReplyKeyboardMarkup,
                           KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile)

load_dotenv()

API_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
BOT_USERNAME = os.getenv("BOT_USERNAME", "TechNowAibot")
BUILD_VERSION = "10.28.0-full-fixed"
DEFAULT_MAX_WORKERS = 3
DEFAULT_MAX_AI_WORKERS = 3
AI_VERIFY_ENABLED_DEFAULT = os.getenv("AI_VERIFY_ENABLED", "auto").lower()
AI_PROVIDER_RECHECK_MINUTES = int(os.getenv("AI_PROVIDER_RECHECK_MINUTES", "10"))
BOT_USERNAME_RUNTIME = ""
CF_ACCOUNT_ID = os.getenv("CF_ACCOUNT_ID")
CF_DATABASE_ID = os.getenv("CF_DATABASE_ID")
CF_API_TOKEN = os.getenv("CF_API_TOKEN")
AI_API_KEY = os.getenv("AI_API_KEY")
AI_API_URL = os.getenv("AI_API_URL", "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions")
AI_MODEL_NAME = os.getenv("AI_MODEL_NAME", "gemini-1.5-flash")
CHANNEL_ID = os.getenv("CHANNEL_ID", "")
AUTOMATION_ENABLED_DEFAULT = os.getenv("AUTOMATION_ENABLED", "true").lower() in {"1", "true", "yes", "on"}
DEFAULT_SOURCE_INTERVAL_MINUTES = int(os.getenv("DEFAULT_SOURCE_INTERVAL_MINUTES", "60"))
WEBSCOUT_FRESHNESS_HOURS = float(os.getenv("WEBSCOUT_FRESHNESS_HOURS", "3"))
WEBSCOUT_SUCCESS_INTERVAL_MINUTES = max(1, int(os.getenv("WEBSCOUT_SUCCESS_INTERVAL_MINUTES", "60")))
WEBSCOUT_EMPTY_RETRY_MINUTES = max(10, int(os.getenv("WEBSCOUT_EMPTY_RETRY_MINUTES", "15")))
WEBSCOUT_HEARTBEAT_SECONDS = max(120, int(os.getenv("WEBSCOUT_HEARTBEAT_SECONDS", "240")))
WEBSCOUT_LOOP_SLEEP_SECONDS = max(10, int(os.getenv("WEBSCOUT_LOOP_SLEEP_SECONDS", "15")))
AUTOMATION_CLEANUP_INTERVAL_SECONDS = max(3600, int(os.getenv("AUTOMATION_CLEANUP_INTERVAL_SECONDS", "21600")))
DEFAULT_MAX_DAILY_POSTS = int(os.getenv("MAX_DAILY_POSTS", "6"))
DEFAULT_MIN_CONTENT_SCORE = float(os.getenv("MIN_CONTENT_SCORE", "65"))
MANAGER_SCORE_TOLERANCE = float(os.getenv("MANAGER_SCORE_TOLERANCE", "8"))
DEFAULT_MIN_HOURS_BETWEEN_POSTS = float(os.getenv("MIN_HOURS_BETWEEN_POSTS", "2"))
DEFAULT_MIN_POST_GAP_MINUTES = max(1, int(round(DEFAULT_MIN_HOURS_BETWEEN_POSTS * 60)))
DEFAULT_PUBLISH_START_HOUR = int(os.getenv("PUBLISH_START_HOUR", "8"))
DEFAULT_PUBLISH_END_HOUR = int(os.getenv("PUBLISH_END_HOUR", "23"))
CONTENT_RETENTION_DAYS = int(os.getenv("CONTENT_RETENTION_DAYS", "1"))
NEWS_FRESHNESS_MAX_HOURS = float(os.getenv("NEWS_FRESHNESS_MAX_HOURS", "24"))
NEWS_PRIORITY_HOURS = float(os.getenv("NEWS_PRIORITY_HOURS", "6"))
NEWS_FRESHNESS_STRICT = os.getenv("NEWS_FRESHNESS_STRICT", "true").lower() in {"1", "true", "yes", "on"}
LOG_RETENTION_DAYS = int(os.getenv("LOG_RETENTION_DAYS", "14"))
AI_PROVIDER_ENCRYPTION_KEY = os.getenv("AI_PROVIDER_ENCRYPTION_KEY", "")
HTTP_USER_AGENT = os.getenv("HTTP_USER_AGENT", "TechNowAI/2.0 (+content automation)")
HTTP_TIMEOUT_SECONDS = int(os.getenv("HTTP_TIMEOUT_SECONDS", "20"))
MAX_HTTP_BYTES = int(os.getenv("MAX_HTTP_BYTES", "1500000"))
MAX_SOURCE_ITEMS_PER_CYCLE = int(os.getenv("MAX_SOURCE_ITEMS_PER_CYCLE", "5"))
MAX_AUTOMATION_SOURCES = max(1, int(os.getenv("MAX_AUTOMATION_SOURCES", "50")))

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SETTINGS_CACHE: Dict[str, Tuple[str, float]] = {}
SETTINGS_CACHE_TTL = 120.0          # FIX سرعت: کش بلندتر
SOURCES_CACHE: Dict[str, Any] = {"ts": 0.0, "rows": []}
PROVIDERS_CACHE: Dict[str, Any] = {"ts": 0.0, "rows": []}
SOURCES_CACHE_TTL = 60.0
PROVIDERS_CACHE_TTL = 30.0
PUBLISH_LOCK = asyncio.Lock()       # FIX رقابت انتشار
LAST_RECOVER = 0.0

class D1Database:
    def __init__(self, account_id, database_id, api_token):
        self.account_id = account_id; self.database_id = database_id; self.api_token = api_token
        self.url = f"https://api.cloudflare.com/client/v4/accounts/{account_id}/d1/database/{database_id}/query"
        self.headers = {"Authorization": f"Bearer {api_token}", "Content-Type": "application/json"}
        self.session: Optional[aiohttp.ClientSession] = None
    async def start(self):
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SECONDS))
    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
        self.session = None
    async def execute(self, sql: str, params: List[Any] = None) -> List[Dict[str, Any]]:
        payload = {"sql": sql}
        if params: payload["params"] = params
        session = self.session; temporary_session = False
        if session is None or session.closed:
            session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SECONDS)); temporary_session = True
        try:
            async with session.post(self.url, headers=self.headers, json=payload) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    logger.error("D1 API Error (status %s): %.200s", resp.status, text)
                    raise Exception(f"Cloudflare D1 API returned status {resp.status}: {text}")
                data = await resp.json()
                if not data.get("success"):
                    raise Exception(f"D1 Query failed: {data.get('errors')}")
                result = data.get("result", [])
                if isinstance(result, list) and len(result) > 0: return result[0].get("results", [])
                elif isinstance(result, dict): return result.get("results", [])
                return []
        except Exception as e:
            # FIX امنیت: params لاگ نمی‌شود (جلوگیری از نشت توکن)
            logger.error("SQL error: %.160s | %s", sql.replace("\n", " "), e)
            raise
        finally:
            if temporary_session: await session.close()
    async def execute_batch(self, queries):
        session = self.session; temporary_session = False
        if session is None or session.closed:
            session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SECONDS)); temporary_session = True
        output = []
        try:
            for query in queries:
                payload = {"sql": query["sql"]}
                if query.get("params"): payload["params"] = query["params"]
                async with session.post(self.url, headers=self.headers, json=payload) as resp:
                    if resp.status != 200:
                        text = await resp.text(); raise Exception(f"D1 Batch status {resp.status}: {text}")
                    data = await resp.json()
                    if not data.get("success"): raise Exception(f"D1 Batch failed: {data.get('errors')}")
                    result = data.get("result", [])
                    if isinstance(result, list) and result: output.append(result[0].get("results", []))
                    elif isinstance(result, dict): output.append(result.get("results", []))
                    else: output.append([])
            return output
        finally:
            if temporary_session: await session.close()

async def initialize_database(db: D1Database):
    await db.execute_batch([
        {"sql": "CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY, joined_at TEXT, role TEXT DEFAULT 'user', tokens_used INTEGER DEFAULT 0, last_reset_date TEXT)"},
        {"sql": "CREATE TABLE IF NOT EXISTS posts(id INTEGER PRIMARY KEY AUTOINCREMENT, text TEXT, file_id TEXT, media_type TEXT, likes INTEGER DEFAULT 0, dislikes INTEGER DEFAULT 0, views INTEGER DEFAULT 0, deleted INTEGER DEFAULT 0)"},
        {"sql": "CREATE INDEX IF NOT EXISTS idx_posts_deleted ON posts(deleted)"},
        {"sql": "CREATE TABLE IF NOT EXISTS user_content_saves(user_id INTEGER NOT NULL, content_type TEXT NOT NULL, content_id INTEGER NOT NULL, folder TEXT NOT NULL, created_at TEXT, PRIMARY KEY(user_id, content_type, content_id))"},
        {"sql": "CREATE INDEX IF NOT EXISTS idx_user_content_saves_user_folder ON user_content_saves(user_id, folder)"},
        {"sql": "CREATE TABLE IF NOT EXISTS user_content_votes(user_id INTEGER NOT NULL, content_type TEXT NOT NULL, content_id INTEGER NOT NULL, vote_type TEXT NOT NULL, created_at TEXT, PRIMARY KEY(user_id, content_type, content_id))"},
        {"sql": "CREATE INDEX IF NOT EXISTS idx_user_content_votes_content ON user_content_votes(content_type, content_id)"},
        {"sql": "CREATE TABLE IF NOT EXISTS user_states(user_id INTEGER PRIMARY KEY, state TEXT, data TEXT)"},
        {"sql": "CREATE TABLE IF NOT EXISTS processed_updates(update_id INTEGER PRIMARY KEY, processed_at TEXT)"},
    ])
    for sql in ["ALTER TABLE posts ADD COLUMN views INTEGER DEFAULT 0",
                "ALTER TABLE users ADD COLUMN tokens_used INTEGER DEFAULT 0",
                "ALTER TABLE users ADD COLUMN last_reset_date TEXT"]:
        try: await db.execute(sql)
        except Exception: pass

async def migrate_unified_user_interactions(db: D1Database):
    # FIX: DROP فقط بعد از موفقیت INSERT ها
    try:
        rows = await db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('saves','votes','article_saves','article_votes')")
        existing = {str(r.get('name') or '') for r in rows}
        inserts = []
        if 'saves' in existing: inserts.append("INSERT OR IGNORE INTO user_content_saves(user_id,content_type,content_id,folder,created_at) SELECT user,'post',post,folder,NULL FROM saves")
        if 'article_saves' in existing: inserts.append("INSERT OR IGNORE INTO user_content_saves(user_id,content_type,content_id,folder,created_at) SELECT user_id,'article',article_id,folder,NULL FROM article_saves")
        if 'votes' in existing: inserts.append("INSERT OR IGNORE INTO user_content_votes(user_id,content_type,content_id,vote_type,created_at) SELECT user_id,'post',post_id,vote_type,NULL FROM votes")
        if 'article_votes' in existing: inserts.append("INSERT OR IGNORE INTO user_content_votes(user_id,content_type,content_id,vote_type,created_at) SELECT user_id,'article',article_id,vote_type,NULL FROM article_votes")
        for sql in inserts: await db.execute(sql)
        for legacy in ('saves', 'votes', 'article_saves', 'article_votes'):
            if legacy in existing:
                try: await db.execute(f"DROP TABLE IF EXISTS {legacy}")
                except Exception: pass
    except Exception:
        logger.exception("migration failed")

async def _add_column_if_missing(db, table, column, definition):
    try:
        t = await db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", [table])
        if not t: return
        cols = await db.execute(f"PRAGMA table_info({table})")
        if any(str(c.get("name")) == column for c in cols): return
        await db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
    except Exception:
        logger.warning("add column skipped: %s.%s", table, column)

async def initialize_automation_database(db: D1Database):
    # ۱) جدول‌ها
    await db.execute_batch([
        {"sql": "CREATE TABLE IF NOT EXISTS sources(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, url TEXT UNIQUE, feed_url TEXT, category TEXT DEFAULT 'tech', enabled INTEGER DEFAULT 1, interval_minutes INTEGER DEFAULT 15, priority INTEGER DEFAULT 5, last_checked_at TEXT, next_check_at TEXT, last_error TEXT, trust_score REAL DEFAULT 80, created_at TEXT, last_seen_published_at TEXT, last_seen_url TEXT)"},
        {"sql": "CREATE TABLE IF NOT EXISTS source_items(id INTEGER PRIMARY KEY AUTOINCREMENT, source_id INTEGER NOT NULL, canonical_url TEXT NOT NULL, title TEXT, description TEXT, content TEXT, image_url TEXT, published_at TEXT, discovered_at TEXT, content_hash TEXT, status TEXT DEFAULT 'new', score REAL DEFAULT 0, category TEXT, article_id INTEGER, last_error TEXT, retry_after TEXT, UNIQUE(source_id, canonical_url))"},
        {"sql": "CREATE TABLE IF NOT EXISTS articles(id INTEGER PRIMARY KEY AUTOINCREMENT, source_item_id INTEGER UNIQUE, title TEXT, channel_text TEXT, body TEXT, source_url TEXT, image_url TEXT, category TEXT, score REAL, status TEXT DEFAULT 'ready', deep_token TEXT UNIQUE, created_at TEXT, verified_at TEXT, published_message_id INTEGER, source_published_at TEXT, deep_views INTEGER DEFAULT 0, content_hash TEXT, published_at TEXT)"},
        {"sql": "CREATE TABLE IF NOT EXISTS publication_queue(id INTEGER PRIMARY KEY AUTOINCREMENT, article_id INTEGER UNIQUE, scheduled_at TEXT, status TEXT DEFAULT 'queued', attempts INTEGER DEFAULT 0, last_error TEXT, created_at TEXT, published_at TEXT, last_attempt_at TEXT)"},
        {"sql": "CREATE TABLE IF NOT EXISTS ai_providers(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE, base_url TEXT, encrypted_api_key TEXT, model_name TEXT, priority INTEGER DEFAULT 10, enabled INTEGER DEFAULT 1, web_enabled INTEGER DEFAULT 0, created_at TEXT, updated_at TEXT, status TEXT DEFAULT 'unknown', last_error TEXT, cooldown_until TEXT, last_checked_at TEXT, last_latency_ms INTEGER DEFAULT 0, consecutive_failures INTEGER DEFAULT 0)"},
        {"sql": "CREATE TABLE IF NOT EXISTS automation_settings(key TEXT PRIMARY KEY, value TEXT)"},
        {"sql": "CREATE TABLE IF NOT EXISTS automation_logs(id INTEGER PRIMARY KEY AUTOINCREMENT, level TEXT, event TEXT, details TEXT, created_at TEXT)"},
        {"sql": "CREATE TABLE IF NOT EXISTS manual_channel_events(id INTEGER PRIMARY KEY AUTOINCREMENT, message_id INTEGER, created_at TEXT)"},
        {"sql": "CREATE TABLE IF NOT EXISTS test_history(id INTEGER PRIMARY KEY AUTOINCREMENT, source_url TEXT, content_hash TEXT, title TEXT, tested_at TEXT)"},
    ])
    # ۲) مهاجرت ستون‌ها «قبل» از ایندکس‌ها (FIX خطای 400)
    for sql in ["ALTER TABLE ai_providers ADD COLUMN status TEXT DEFAULT 'unknown'",
                "ALTER TABLE ai_providers ADD COLUMN last_error TEXT",
                "ALTER TABLE ai_providers ADD COLUMN cooldown_until TEXT",
                "ALTER TABLE ai_providers ADD COLUMN last_checked_at TEXT",
                "ALTER TABLE ai_providers ADD COLUMN last_latency_ms INTEGER DEFAULT 0",
                "ALTER TABLE ai_providers ADD COLUMN consecutive_failures INTEGER DEFAULT 0",
                "ALTER TABLE ai_providers ADD COLUMN web_enabled INTEGER DEFAULT 0",
                "ALTER TABLE articles ADD COLUMN published_at TEXT",
                "ALTER TABLE articles ADD COLUMN deep_views INTEGER DEFAULT 0",
                "ALTER TABLE articles ADD COLUMN source_published_at TEXT",
                "ALTER TABLE articles ADD COLUMN content_hash TEXT",
                "ALTER TABLE publication_queue ADD COLUMN last_attempt_at TEXT",
                "ALTER TABLE sources ADD COLUMN last_seen_published_at TEXT",
                "ALTER TABLE sources ADD COLUMN last_seen_url TEXT",
                "ALTER TABLE source_items ADD COLUMN retry_after TEXT"]:
        try: await db.execute(sql)
        except Exception: pass
    # ۳) ایندکس‌ها بعد از مهاجرت
    for sql in ["CREATE INDEX IF NOT EXISTS idx_sources_due ON sources(enabled, next_check_at)",
                "CREATE INDEX IF NOT EXISTS idx_source_items_status ON source_items(status)",
                "CREATE INDEX IF NOT EXISTS idx_source_items_hash ON source_items(content_hash)",
                "CREATE INDEX IF NOT EXISTS idx_articles_hash ON articles(content_hash)",
                "CREATE INDEX IF NOT EXISTS idx_publication_queue_due ON publication_queue(status, scheduled_at)",
                "CREATE INDEX IF NOT EXISTS idx_automation_logs_created ON automation_logs(created_at)",
                "CREATE INDEX IF NOT EXISTS idx_test_history_hash ON test_history(content_hash)"]:
        try: await db.execute(sql)
        except Exception: pass

    defaults = {
        "automation_enabled": "1" if AUTOMATION_ENABLED_DEFAULT else "0",
        "max_daily_posts": str(DEFAULT_MAX_DAILY_POSTS),
        "min_content_score": str(DEFAULT_MIN_CONTENT_SCORE),
        "min_hours_between_posts": str(DEFAULT_MIN_HOURS_BETWEEN_POSTS),
        "min_post_gap_minutes": str(DEFAULT_MIN_POST_GAP_MINUTES),
        "publish_start_hour": str(DEFAULT_PUBLISH_START_HOUR),
        "publish_end_hour": str(DEFAULT_PUBLISH_END_HOUR),
        "default_source_interval": str(DEFAULT_SOURCE_INTERVAL_MINUTES),
        "webscout_freshness_hours": str(WEBSCOUT_FRESHNESS_HOURS),
        "webscout_success_interval_minutes": str(WEBSCOUT_SUCCESS_INTERVAL_MINUTES),
        "webscout_empty_retry_minutes": str(WEBSCOUT_EMPTY_RETRY_MINUTES),
        "webscout_next_run_at": "",
        "news_freshness_max_hours": str(int(NEWS_FRESHNESS_MAX_HOURS) if NEWS_FRESHNESS_MAX_HOURS.is_integer() else NEWS_FRESHNESS_MAX_HOURS),
        "news_priority_hours": str(int(NEWS_PRIORITY_HOURS) if NEWS_PRIORITY_HOURS.is_integer() else NEWS_PRIORITY_HOURS),
        "ai_verify_mode": AI_VERIFY_ENABLED_DEFAULT,
        "last_cleanup_at": "", "last_manual_channel_post_at": "",
        "channel_id": CHANNEL_ID, "channel_username": "",
        "max_workers": str(DEFAULT_MAX_WORKERS), "max_ai_workers": str(DEFAULT_MAX_AI_WORKERS),
        "worker_heartbeat_at": "", "worker_started_at": "",
        "last_cycle_started_at": "", "last_cycle_finished_at": "", "last_cycle_result": "",
        "weight_global": "15", "weight_technology": "15", "weight_ai": "15", "weight_cyber": "15",
        "weight_education": "10", "weight_iran": "15", "weight_freshness": "10",
        "weight_source": "5", "weight_novelty": "10",
        "editorial_prompt_channel": "فقط محتوای فنی و واقعاً ارزشمند برای مخاطب فناوری و هوش مصنوعی را پوشش بده؛ خبرهای سطحی، عمومی، تبلیغاتی، تکراری و پیش‌پاافتاده را کنار بگذار. نکات فنی مهم، قابلیت جدید، تغییر مهم، عدد و جزئیات قابل اتکا را در نسخه کوتاه بیاور.",
        "editorial_prompt_article": "نسخه کامل باید یک محتوای فنی و غنی باشد؛ جزئیات واقعی رویداد، نحوه کار یا فناوری، نکات فنی مهم، زمینه لازم و اثرات قابل فهم را پوشش بده. از حرف‌های کلی، کلیشه‌ای، نتیجه‌گیری شخصی و سؤال‌سازی به جای پاسخ خودداری کن. فقط بر پایه اطلاعات منبع بنویس.",
    }
    have = {r.get("key") for r in await db.execute("SELECT key FROM automation_settings")}
    for k, v in defaults.items():
        if k not in have:
            await db.execute("INSERT OR IGNORE INTO automation_settings(key, value) VALUES(?, ?)", [k, v])
    try:
        gap_rows = await db.execute("SELECT value FROM automation_settings WHERE key='min_post_gap_minutes'")
        legacy_rows = await db.execute("SELECT value FROM automation_settings WHERE key='min_hours_between_posts'")
        current_gap = float(gap_rows[0].get('value')) if gap_rows and gap_rows[0].get('value') not in (None, '') else 0
        legacy_gap = float(legacy_rows[0].get('value')) if legacy_rows and legacy_rows[0].get('value') not in (None, '') else 0
        if current_gap == DEFAULT_MIN_POST_GAP_MINUTES and legacy_gap > 0 and abs(legacy_gap * 60 - current_gap) > 0.01:
            await db.execute("UPDATE automation_settings SET value=? WHERE key='min_post_gap_minutes'", [str(int(round(legacy_gap * 60)))])
    except Exception: pass
    try:
        await db.execute("UPDATE ai_providers SET enabled=0, status='invalid', last_error='Environment Default disabled by managed-provider mode' WHERE name='Environment Default'")
    except Exception: pass
    try:
        await db.execute("UPDATE articles SET image_url='' WHERE image_url IS NOT NULL AND image_url!=''")
    except Exception: pass

def encrypt_secret(value: str) -> str:
    if not value: return ""
    if not AI_PROVIDER_ENCRYPTION_KEY: return value
    try: return Fernet(AI_PROVIDER_ENCRYPTION_KEY.encode()).encrypt(value.encode()).decode()
    except Exception:
        logger.exception("Failed to encrypt AI provider secret"); return value

def decrypt_secret(value: str) -> str:
    if not value: return ""
    if not AI_PROVIDER_ENCRYPTION_KEY: return value
    try: return Fernet(AI_PROVIDER_ENCRYPTION_KEY.encode()).decrypt(value.encode()).decode()
    except Exception: return value

async def get_setting(db, key, default=""):
    now = time.monotonic()
    cached = SETTINGS_CACHE.get(key)
    if cached and now - cached[1] < SETTINGS_CACHE_TTL: return cached[0]
    rows = await db.execute("SELECT value FROM automation_settings WHERE key = ?", [key])
    value = str(rows[0].get("value")) if rows else str(default)
    SETTINGS_CACHE[key] = (value, now)
    return value

async def set_setting(db, key, value):
    await db.execute("INSERT OR REPLACE INTO automation_settings(key, value) VALUES(?, ?)", [key, str(value)])
    SETTINGS_CACHE[key] = (str(value), time.monotonic())

async def get_channel_id(db): return (await get_setting(db, "channel_id", CHANNEL_ID)).strip()
def invalidate_sources(): SOURCES_CACHE["ts"] = 0.0
def invalidate_providers(): PROVIDERS_CACHE["ts"] = 0.0

async def get_enabled_sources(db, force=False):
    now = time.monotonic()
    if not force and now - SOURCES_CACHE["ts"] < SOURCES_CACHE_TTL: return SOURCES_CACHE["rows"]
    rows = await db.execute("SELECT * FROM sources WHERE enabled=1 ORDER BY priority ASC,id ASC")
    SOURCES_CACHE["ts"] = now; SOURCES_CACHE["rows"] = rows
    return rows

async def get_enabled_providers(db, force=False):
    now = time.monotonic()
    if not force and now - PROVIDERS_CACHE["ts"] < PROVIDERS_CACHE_TTL: return PROVIDERS_CACHE["rows"]
    rows = await db.execute("SELECT * FROM ai_providers WHERE enabled=1 AND status!='invalid' AND (cooldown_until IS NULL OR cooldown_until<=?) ORDER BY priority ASC,id ASC", [datetime.now(timezone.utc).isoformat()])
    PROVIDERS_CACHE["ts"] = now; PROVIDERS_CACHE["rows"] = rows
    return rows

async def log_automation(db, level, event, details=""):
    try:
        if len(details) > 2000: details = details[:2000]
        await db.execute("INSERT INTO automation_logs(level,event,details,created_at) VALUES(?,?,?,?)", [level, event, details, datetime.now(timezone.utc).isoformat()])
    except Exception:
        logger.exception("automation log failed")

async def cleanup_automation_data(db: D1Database):
    now = datetime.now(timezone.utc)
    cutoff_content = (now - timedelta(days=max(1, CONTENT_RETENTION_DAYS))).isoformat()
    cutoff_logs = (now - timedelta(days=LOG_RETENTION_DAYS)).isoformat()
    await db.execute("DELETE FROM automation_logs WHERE created_at < ?", [cutoff_logs])
    await db.execute("DELETE FROM publication_queue WHERE status IN ('published','failed') AND created_at < ?", [cutoff_content])
    await db.execute("DELETE FROM test_history WHERE tested_at < ?", [cutoff_logs])
    await db.execute("DELETE FROM manual_channel_events WHERE created_at < ?", [cutoff_logs])
    await db.execute("UPDATE articles SET image_url='' WHERE status='published' AND COALESCE(published_at,created_at) < ?", [cutoff_content])
    await set_setting(db, "last_cleanup_at", now.isoformat())

def normalize_url(url: str) -> str:
    url = (url or "").strip()
    if not url: return ""
    parsed = urllib.parse.urlsplit(url if "://" in url else "https://" + url)
    query = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    query = [(k, v) for k, v in query if not k.lower().startswith(("utm_", "fbclid", "gclid"))]
    return urllib.parse.urlunsplit((parsed.scheme.lower(), parsed.netloc.lower(), parsed.path.rstrip('/') or '/', urllib.parse.urlencode(query), ""))

def text_hash(text: str) -> str:
    return hashlib.sha256(re.sub(r"\s+", " ", text or "").strip().lower().encode("utf-8", errors="ignore")).hexdigest()

def normalize_model_text(value: str) -> str:
    if value is None: return ""
    text = str(value)
    text = text.replace("\\r\n", "\n").replace("\\n", "\n").replace("\\r", "\n").replace("\\t", "\t")
    text = text.replace("\r\n", "\n").replace("\r", "\n").replace("\u00a0", " ")
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n", text)
    return text.strip()

def strip_html_text(value: str) -> str:
    if not value: return ""
    value = normalize_model_text(value)
    value = re.sub(r"<script.*?</script>|<style.*?</style>|<noscript.*?</noscript>", " ", value, flags=re.I | re.S)
    value = re.sub(r"<[^>]+>", " ", value)
    return re.sub(r"\s+", " ", html.unescape(value)).strip()

def parse_publication_datetime(raw: str) -> Optional[datetime]:
    raw = normalize_model_text(raw or "").strip()
    if not raw: return None
    for value in [raw, raw.replace("Z", "+00:00")]:
        try:
            dt = datetime.fromisoformat(value)
            if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception: pass
    try:
        from email.utils import parsedate_to_datetime
        dt = parsedate_to_datetime(raw)
        if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception: return None

PAYWALL_KEYWORDS = ["ادامه مطلب", "برای مشاهده", "اشتراک", "محدود", "ثبت نام", "عضویت", "خرید اشتراک", "دسترسی کامل", "متن کامل", "نمایش کامل", "بیشتر بخوانید", "continue reading", "subscribe", "sign up", "register", "full access", "premium", "paywall", "limited access", "you have reached", "already a member", "log in", "login"]

def is_insufficient_content(title: str, body: str, description: str) -> Tuple[bool, str]:
    title_plain = strip_html_text(title or "").strip()
    desc_plain = strip_html_text(description or "").strip()
    body_plain = strip_html_text(body or "").strip()
    combined = (title_plain + " " + desc_plain + " " + body_plain).lower()
    if len(body_plain) < 20 and len(title_plain) < 30 and len(desc_plain) < 50:
        return True, "محتوا بسیار کوتاه و فاقد اطلاعات کافی است"
    if any(kw in combined for kw in PAYWALL_KEYWORDS) and len(body_plain) < 500:
        return True, "محتوای سرویس اشتراک/پشت پرده است و اطلاعات کافی ندارد"
    if len(body_plain) < 100:
        if len(re.findall(r"\w+", title_plain + " " + desc_plain)) < 20:
            return True, "محتوا بسیار کوتاه و فاقد اطلاعات کافی است"
    if len(body_plain) < 300:
        if re.search(r"\d{4}[-/]\d{1,2}[-/]\d{1,2}", body_plain) or re.search(r"\d+%", body_plain):
            return False, ""
        if len(body_plain) < 180:
            return True, "محتوا برای تولید مقاله غنی کافی نیست"
    return False, ""

def recent_semantic_similarity(title: str, recent_titles: List[str]) -> float:
    best = 0.0
    a = (title or "").lower()
    for t in recent_titles:
        best = max(best, SequenceMatcher(None, a, (t or "").lower()).ratio())
    return best

def parse_json_object(text: str) -> Dict[str, Any]:
    text = (text or "").strip()
    if text.startswith("```"): text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text, flags=re.I | re.S).strip()
    def _loads(candidate):
        try:
            obj = json.loads(candidate); return obj if isinstance(obj, dict) else {}
        except Exception:
            repaired = re.sub(r"\\(?![\"/bfnrt]|u[0-9a-fA-F]{4})", lambda m: "\\\\", candidate)
            try:
                obj = json.loads(repaired); return obj if isinstance(obj, dict) else {}
            except Exception: return {}
    obj = _loads(text)
    if obj: return obj
    m = re.search(r"\{.*\}", text, flags=re.S)
    return _loads(m.group(0)) if m else {}

class AIProviderManager:
    def __init__(self, db, bot=None):
        self.db = db; self.bot = bot
        self._session: Optional[aiohttp.ClientSession] = None
        self._last_final_notice = 0.0
        self._last_ws_notice = 0.0
        self._webscout_cooldowns: Dict[int, float] = {}
    async def start(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=120))
    async def close(self):
        if self._session and not self._session.closed: await self._session.close()
        self._session = None
    async def providers(self):
        return await get_enabled_providers(self.db, force=True)
    @staticmethod
    def protocol(url):
        u = (url or "").lower().rstrip("/")
        if "generativelanguage.googleapis.com" in u:
            return "openai" if ("/openai" in u or "chat/completions" in u) else "gemini"
        if "api.anthropic.com" in u and "/chat/completions" not in u: return "anthropic"
        return "openai"
    @staticmethod
    def endpoint(url, protocol, model=""):
        u = (url or "").strip().rstrip("/")
        if protocol == "gemini":
            if u.endswith(":generateContent"): return u
            if "/models/" in u: return u + ":generateContent"
            return u + f"/models/{urllib.parse.quote(model, safe='')}:generateContent"
        if protocol == "anthropic":
            return u if u.endswith("/messages") else (u + "/messages" if u.endswith("/v1") else u + "/v1/messages")
        if u.endswith("/chat/completions"): return u
        if u.endswith(("/v1", "/openai")): return u + "/chat/completions"
        return u + "/chat/completions"
    @staticmethod
    def google_openai_endpoint(base_url):
        u = (base_url or "").strip().rstrip("/")
        if "generativelanguage.googleapis.com" not in u: return ""
        if "/openai" in u: return u if u.endswith("/chat/completions") else u + "/chat/completions"
        return "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions"
    @staticmethod
    def _extract_content(protocol, data):
        if not isinstance(data, dict): return ""
        if protocol == "anthropic":
            return "".join(b.get("text", "") for b in data.get("content", []) if isinstance(b, dict) and b.get("type") == "text")
        if protocol == "gemini":
            parts = []
            for c in data.get("candidates") or []:
                for part in (c.get("content") or {}).get("parts") or []:
                    if isinstance(part, dict) and part.get("text"): parts.append(str(part["text"]))
            return "".join(parts).strip()
        choice = (data.get("choices") or [{}])[0] or {}
        content = (choice.get("message") or {}).get("content")
        if isinstance(content, str): return content.strip()
        if isinstance(content, list): return "".join(str(x.get("text", "")) for x in content if isinstance(x, dict)).strip()
        return str(content or "").strip()
    @staticmethod
    def _empty_response_reason(protocol, data):
        if not isinstance(data, dict): return "بدنه پاسخ JSON قابل تشخیص نبود."
        if protocol == "gemini":
            reasons = []
            pf = data.get("promptFeedback") or {}
            if pf.get("blockReason"): reasons.append(f"blockReason={pf.get('blockReason')}")
            for c in data.get("candidates") or []:
                if c.get("finishReason"): reasons.append(f"finishReason={c.get('finishReason')}")
            return "؛ ".join(reasons) or "Gemini پاسخ موفق داد اما متن نداشت."
        if protocol == "anthropic": return str(data.get("stop_reason") or data.get("error") or "Anthropic متن نداشت.")
        choices = data.get("choices") or []
        if choices: return f"finish_reason={(choices[0] or {}).get('finish_reason') or '-'}؛ content خالی."
        return "پاسخ فاقد choices بود."
    @staticmethod
    def _usage_tokens(protocol, usage):
        # FIX شمارش توکن Gemini/Anthropic
        if not isinstance(usage, dict): return 0
        if protocol == "gemini": return int(usage.get("totalTokenCount") or usage.get("total_tokens") or 0)
        if protocol == "anthropic": return int((usage.get("input_tokens") or 0) + (usage.get("output_tokens") or 0) or 0)
        return int(usage.get("total_tokens") or 0)
    async def _request(self, provider, messages, temperature, max_tokens, forced_protocol=None, forced_endpoint=None, webscout=False):
        await self.start()
        key = decrypt_secret(provider.get("encrypted_api_key") or "")
        model = (provider.get("model_name") or "").strip()
        base = provider.get("base_url") or ""
        protocol = forced_protocol or self.protocol(base)
        endpoint = forced_endpoint or self.endpoint(base, protocol, model)
        headers = {"Content-Type": "application/json", "User-Agent": HTTP_USER_AGENT}
        started = time.perf_counter()
        if protocol == "anthropic":
            headers["x-api-key"] = key; headers["anthropic-version"] = "2023-06-01"
            system = "\n".join(m.get("content", "") for m in messages if m.get("role") == "system").strip()
            msgs = [{"role": "assistant" if m.get("role") == "assistant" else "user", "content": m.get("content", "")} for m in messages if m.get("role") != "system"]
            while msgs and msgs[0]["role"] != "user": msgs.pop(0)
            payload = {"model": model, "messages": msgs, "max_tokens": max_tokens, "temperature": temperature}
            if system: payload["system"] = system
        elif protocol == "gemini":
            headers["x-goog-api-key"] = key
            # FIX تاریخچه واقعی user/model به‌جای یک‌کاسه‌کردن
            contents = [{"role": "model" if m.get("role") == "assistant" else "user", "parts": [{"text": m.get("content", "")}]} for m in messages if m.get("role") != "system"]
            if not contents: contents = [{"role": "user", "parts": [{"text": ""}]}]
            payload = {"contents": contents, "generationConfig": {"temperature": temperature, "maxOutputTokens": max_tokens}}
            sys = "\n".join(m.get("content", "") for m in messages if m.get("role") == "system").strip()
            if sys: payload["systemInstruction"] = {"parts": [{"text": sys}]}
            if webscout: payload["tools"] = [{"url_context": {}}, {"google_search": {}}]
        else:
            headers["Authorization"] = f"Bearer {key}"
            payload = {"model": model, "messages": messages, "temperature": temperature, "max_tokens": max_tokens}
            if webscout and "openrouter.ai" in (base or "").lower():
                payload["tools"] = [{"type": "openrouter:web_search"}, {"type": "openrouter:web_fetch"}]
                payload["max_tool_calls"] = 6
        async with self._session.post(endpoint, headers=headers, json=payload) as resp:
            raw = await resp.text(); latency = int((time.perf_counter() - started) * 1000)
            if resp.status != 200: raise RuntimeError(f"HTTP {resp.status} | endpoint={endpoint} | body={raw[:1200]}")
            try: data = json.loads(raw)
            except Exception as e: raise RuntimeError(f"HTTP 200 ولی JSON نامعتبر: {e}")
            content = self._extract_content(protocol, data)
            usage = (data.get("usageMetadata") if protocol == "gemini" else data.get("usage")) or {}
            if not content: raise RuntimeError(f"پاسخ مدل خالی بود | {self._empty_response_reason(protocol, data)}")
            return content, data, latency, usage, protocol, endpoint
    @staticmethod
    def classify_error(msg):
        m = msg.lower()
        if any(x in m for x in ("429", "resource_exhausted", "quota", "rate limit", "too many requests")): return "quota"
        if any(x in m for x in ("402", "404", "model_not_found", "401", "403", "authentication", "invalid api", "outstanding_balance")): return "invalid"
        return "temporary"
    async def _mark_health(self, provider, status, error="", latency=0, cooldown_minutes=0):
        now = datetime.now(timezone.utc)
        cd = (now + timedelta(minutes=cooldown_minutes)).isoformat() if cooldown_minutes else None
        try:
            await self.db.execute("UPDATE ai_providers SET status=?, last_error=?, cooldown_until=?, last_checked_at=?, last_latency_ms=?, updated_at=? WHERE id=?", [status, error[:1000], cd, now.isoformat(), latency, now.isoformat(), provider.get("id")])
            invalidate_providers()
        except Exception: pass
    async def test_provider_values(self, base_url, api_key, model):
        await self.start()
        base, key, mdl = (base_url or "").strip(), (api_key or "").strip(), (model or "").strip()
        if not base: return {"ok": False, "error": "Base URL خالی است."}
        if not key: return {"ok": False, "error": "API Key خالی است."}
        if not mdl: return {"ok": False, "error": "نام مدل خالی است."}
        detected = self.protocol(base)
        candidates = [(detected, self.endpoint(base, detected, mdl))]
        if "generativelanguage.googleapis.com" in base:
            compat = self.google_openai_endpoint(base)
            if compat and all(ep != compat for _, ep in candidates): candidates.append(("openai", compat))
            native = self.endpoint("https://generativelanguage.googleapis.com/v1beta", "gemini", mdl)
            if all(ep != native for _, ep in candidates): candidates.append(("gemini", native))
        diagnostics = []
        for proto, endpoint in candidates:
            started = time.perf_counter()
            try:
                headers = {"Content-Type": "application/json", "User-Agent": HTTP_USER_AGENT}
                if proto == "anthropic":
                    headers["x-api-key"] = key; headers["anthropic-version"] = "2023-06-01"
                    payload = {"model": mdl, "messages": [{"role": "user", "content": "Reply with exactly: TEST_OK"}], "max_tokens": 32}
                elif proto == "gemini":
                    headers["x-goog-api-key"] = key
                    payload = {"contents": [{"role": "user", "parts": [{"text": "Reply with exactly: TEST_OK"}]}], "generationConfig": {"maxOutputTokens": 32}}
                else:
                    headers["Authorization"] = f"Bearer {key}"
                    payload = {"model": mdl, "messages": [{"role": "user", "content": "Reply with exactly: TEST_OK"}]}
                async with self._session.post(endpoint, headers=headers, json=payload, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    raw = await resp.text(); latency = int((time.perf_counter() - started) * 1000)
                    try: data = json.loads(raw)
                    except Exception: data = {}
                    if resp.status == 200 and self._extract_content(proto, data):
                        ws = await self._probe_webscout(proto, endpoint, headers, mdl, base)
                        return {"ok": True, "latency_ms": latency, "preview": self._extract_content(proto, data).strip()[:120], "protocol": proto, "endpoint": endpoint, "webscout_ok": ws}
                    if proto == "openai" and resp.status in {400, 422}:
                        async with self._session.post(endpoint, headers=headers, json={**payload, "temperature": 0, "max_tokens": 32}, timeout=aiohttp.ClientTimeout(total=30)) as r2:
                            raw2 = await r2.text()
                            if r2.status == 200:
                                try: d2 = json.loads(raw2)
                                except Exception: d2 = {}
                                if self._extract_content("openai", d2):
                                    return {"ok": True, "latency_ms": latency, "preview": self._extract_content("openai", d2).strip()[:120], "protocol": "openai", "endpoint": endpoint, "webscout_ok": None}
                    diagnostics.append(f"{proto} HTTP {resp.status}: {raw[:500]}")
            except Exception as e:
                diagnostics.append(f"{proto}: {str(e)[:500]}")
        return {"ok": False, "error": "\n".join(diagnostics)[:5000]}
    async def _probe_webscout(self, proto, endpoint, headers, model, base):
        # FIX: تست واقعی قابلیت سرچ وب
        try:
            if proto == "gemini":
                payload = {"model": model, "contents": [{"role": "user", "parts": [{"text": "Reply with exactly: WS_OK"}]}], "generationConfig": {"maxOutputTokens": 16}, "tools": [{"google_search": {}}]}
            elif "openrouter.ai" in (base or "").lower():
                payload = {"model": model, "messages": [{"role": "user", "content": "Reply with exactly: WS_OK"}], "max_tokens": 16, "tools": [{"type": "openrouter:web_search"}]}
            else:
                return None
            async with self._session.post(endpoint, headers=headers, json=payload, timeout=aiohttp.ClientTimeout(total=30)) as r:
                return r.status == 200
        except Exception:
            return False
    async def test_provider(self, provider_id):
        rows = await self.db.execute("SELECT * FROM ai_providers WHERE id=?", [provider_id])
        if not rows: return {"ok": False, "error": "Provider یافت نشد"}
        p = rows[0]
        result = await self.test_provider_values(p.get("base_url", ""), decrypt_secret(p.get("encrypted_api_key", "")), p.get("model_name", ""))
        now = datetime.now(timezone.utc).isoformat()
        if result["ok"]:
            await self.db.execute("UPDATE ai_providers SET status='healthy', last_error=NULL, cooldown_until=NULL, last_checked_at=?, last_latency_ms=?, consecutive_failures=0, updated_at=? WHERE id=?", [now, result.get("latency_ms", 0), now, provider_id])
        else:
            kind = self.classify_error(str(result.get("error", "")))
            minutes = {"invalid": AI_PROVIDER_RECHECK_MINUTES, "quota": 120}.get(kind, max(3, AI_PROVIDER_RECHECK_MINUTES))
            await self.db.execute("UPDATE ai_providers SET status=?, last_error=?, cooldown_until=?, last_checked_at=?, updated_at=? WHERE id=?", ["invalid" if kind == "invalid" else "cooldown", str(result.get("error", ""))[:1200], (datetime.now(timezone.utc) + timedelta(minutes=minutes)).isoformat(), now, now, provider_id])
        invalidate_providers()
        return result
    async def call(self, messages, temperature=0.2, max_tokens=2500, purpose="generic", persist_health=True):
        providers = await self.providers()
        if not providers: return {"content": "", "provider": None, "model": None, "tokens": 0, "error": "هیچ مدل فعالی وجود ندارد."}
        errors = []; tried = 0; now = datetime.now(timezone.utc)
        for p in providers:
            cd = p.get("cooldown_until") or ""
            if cd:
                try:
                    if datetime.fromisoformat(cd.replace("Z", "+00:00")) > now: continue
                except Exception: pass
            tried += 1
            try:
                content, data, latency, usage, protocol, _ = await self._request(p, messages, temperature, max_tokens)
                if persist_health: await self._mark_health(p, "healthy", "", latency, 0)
                return {"content": content, "provider": p.get("name"), "model": p.get("model_name"), "tokens": self._usage_tokens(protocol, usage), "error": None}
            except Exception as e:
                msg = str(e); errors.append(f"{p.get('name')}: {msg[:260]}")
                if persist_health:
                    kind = self.classify_error(msg)
                    minutes = {"invalid": AI_PROVIDER_RECHECK_MINUTES, "quota": 120}.get(kind, 5)
                    await self._mark_health(p, "invalid" if kind == "invalid" else "cooldown", msg, 0, minutes)
        final = ("همه مدل‌ها در cooldown/نامعتبرند." if tried == 0 else "همه مدل‌ها خطا دادند.") + " | " + " | ".join(errors)
        if purpose != "user_chat" and self.bot and ADMIN_ID and time.time() - self._last_final_notice > 1800:
            self._last_final_notice = time.time()
            try: await self.bot.send_message(ADMIN_ID, "🚨 خطای نهایی AI\n" + html.escape(final[:1200]))
            except Exception: pass
        return {"content": "", "provider": None, "model": None, "tokens": 0, "error": final}
    async def webscout_call(self, url, scout_prompt, max_tokens=9000):
        # FIX: فیلتر status/cooldown از DB + ثبت سلامت + استراحت طولانی روی quota
        now_iso = datetime.now(timezone.utc).isoformat()
        providers = await self.db.execute("SELECT * FROM ai_providers WHERE enabled=1 AND web_enabled=1 AND status!='invalid' AND (cooldown_until IS NULL OR cooldown_until<=?) ORDER BY priority ASC,id ASC", [now_iso])
        if not providers: return {"ok": False, "error": "هیچ WebScout فعال و سالمی وجود ندارد."}
        errors = []
        for p in providers:
            pid = int(p.get("id") or 0)
            if pid and time.time() < self._webscout_cooldowns.get(pid, 0): continue
            model = str(p.get("model_name") or ""); base = str(p.get("base_url") or "")
            if "generativelanguage.googleapis.com" in base.lower():
                protocol, endpoint = "gemini", self.endpoint("https://generativelanguage.googleapis.com/v1beta", "gemini", model)
            elif "openrouter.ai" in base.lower():
                protocol, endpoint = "openai", self.endpoint(base, "openai", model)
            else:
                errors.append(f"{p.get('name')}: پشتیبانی WebScout ندارد")
                continue
            messages = [
                {"role": "system", "content": "You are the WebScout research engine. Use web tools, inspect the supplied URL, never rely on memory."},
                {"role": "user", "content": scout_prompt + f"\nTARGET URL:\n{url}"}]
            try:
                content, data, latency, usage, _, _ = await self._request(p, messages, 0.1, max_tokens, forced_protocol=protocol, forced_endpoint=endpoint, webscout=True)
                if pid: self._webscout_cooldowns.pop(pid, None)
                await self._mark_health(p, "healthy", "", latency, 0)
                return {"ok": True, "content": content, "provider": p.get("name"), "model": model, "latency_ms": latency, "usage": usage}
            except Exception as e:
                msg = str(e); errors.append(f"{p.get('name')}: {msg[:700]}")
                kind = self.classify_error(msg)
                status = "invalid" if kind == "invalid" else "cooldown"
                cool = {"invalid": AI_PROVIDER_RECHECK_MINUTES, "quota": 120}.get(kind, 10)
                if pid:
                    self._webscout_cooldowns[pid] = time.time() + cool * 60
                    await self._mark_health(p, status, msg, 0, cool)
                if self.bot and ADMIN_ID and time.time() - self._last_ws_notice > 900:
                    self._last_ws_notice = time.time()
                    try: await self.bot.send_message(ADMIN_ID, "⚠️ <b>خطای WebScout</b>\n<code>" + html.escape(f"{p.get('name')}: {msg[:400]}") + "</code>", parse_mode="HTML")
                    except Exception: pass
        return {"ok": False, "error": "\n".join(errors)[:7000]}

def make_deep_token(article_id: int) -> str:
    return hashlib.sha256(f"techhow-{article_id}-{time.time_ns()}".encode()).hexdigest()[:18]
    
    # ============================================================
# PART 2/4 — Sanitizer + Formatting + Caption + AI editorial helpers
# ============================================================
class TelegramHTMLSanitizer(HTMLParser):
    ALLOWED = {"b","strong","i","em","u","s","del","code","pre","blockquote","a","tg-spoiler"}
    BLOCK = {"p","div","section","article","header","footer","h1","h2","h3","h4","h5","h6","ul","ol","li"}
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.out = []
        self.stack = []      # FIX: بالانس واقعی تگ‌ها
    def _newline(self, count=1):
        if not self.out: return
        current = "".join(self.out)
        target = "\n" * count
        if not current.endswith(target):
            if count == 2 and current.endswith("\n"): self.out.append("\n")
            else: self.out.append(target)
    def handle_data(self, data):
        data = str(data or "").replace("\r\n","\n").replace("\r","\n").replace("\t"," ").replace("\u00a0"," ")
        data = re.sub(r"\n{3,}", "\n", data)
        if data: self.out.append(html.escape(data, quote=False))
    def handle_starttag(self, tag, attrs):
        tag = tag.lower()
        if tag in self.BLOCK:
            self._newline(2 if tag in {"p","div","section","article","header","footer","h1","h2","h3","h4","h5","h6"} else 1)
            if tag == "li": self.out.append("• ")
            return
        if tag not in self.ALLOWED: return
        if tag == "a":
            href = dict(attrs).get("href","")
            if href.startswith(("https://","http://","tg://")):
                self.out.append(f'<a href="{html.escape(href, quote=True)}">')
                self.stack.append("a")
            return   # FIX: هرگز <a> بدون href تولید نشود
        self.out.append(f"<{tag}>")
        self.stack.append(tag)
    def handle_startendtag(self, tag, attrs):
        if tag.lower() == "br": self._newline(1)
    def handle_endtag(self, tag):
        tag = tag.lower()
        if tag in self.BLOCK:
            self._newline(1); return
        # FIX: فقط تگ‌هایی که باز شده‌اند بسته شوند (بدون </a> یتیم)
        if tag in self.ALLOWED and tag in self.stack:
            while self.stack:
                t = self.stack.pop()
                self.out.append(f"</{t}>")
                if t == tag: break
    def get_result(self):
        while self.stack:   # FIX: بستن تگ‌های باز مانده در انتها
            self.out.append(f"</{self.stack.pop()}>")
        return "".join(self.out)

def sanitize_telegram_html(value: str) -> str:
    value = normalize_model_text(value)
    value = re.sub(r"&lt;\s*(/?\s*(?:blockquote|b|strong|i|em|u|s|del|code|pre|tg-spoiler|a))(.*?)\s*&gt;", r"<\1\2>", value, flags=re.I|re.S)
    value = re.sub(r"<[^>]+>", lambda m: re.sub(r"[‎‏‪-‮⁦-⁩]", "", m.group(0)), value)
    if not value: return ""
    try:
        p = TelegramHTMLSanitizer(); p.feed(value); p.close()
        result = p.get_result()
        result = re.sub(r"[ \t]+\n", "\n", result)
        result = re.sub(r"\n[ \t]+", "\n", result)
        result = re.sub(r"\n{3,}", "\n", result)
        return result.strip()
    except Exception:
        return html.escape(strip_html_text(value), quote=False)

def plain_len(value: str) -> int:
    return len(strip_html_text(value or ""))

def _protect_bidi_latin(text: str) -> str:
    # FIX: هرگز داخل تگ‌ها یا موجودیت‌های HTML (&amp; و…) کاراکتر جهت درج نشود
    if not text: return text
    parts = re.split(r"(<[^>]+>|&[a-zA-Z#0-9]+;)", text, flags=re.I|re.S)
    out = []
    for part in parts:
        if (part.startswith("<") and part.endswith(">")) or (part.startswith("&") and part.endswith(";")):
            out.append(re.sub(r"[‎‏‪-‮⁦-⁩]", "", part))
        else:
            out.append(re.sub(r"(?<![A-Za-z0-9])([A-Za-z][A-Za-z0-9@._+/#:-]{1,64})(?![A-Za-z0-9])", lambda m: "\u200e"+m.group(1)+"\u200e", part))
    return "".join(out)

def _format_technical_tokens(text: str) -> str:
    patterns = [
        r"\b(?:GPT-\d+(?:\.\d+)?|GPT-4o|LLM|API|JSON|Python|JavaScript|TypeScript|HTML|CSS|SQL|HTTP|HTTPS|OAuth|WebSocket|RAG|GPU|CPU|SDK)\b",
        r"\b(?:Generative AI|Machine Learning|Zero[- ]Day|Phishing|Ransomware)\b",
    ]
    out = text
    for pat in patterns:
        out = re.sub(pat, lambda m: f"<code>{m.group(0)}</code>", out, flags=re.I)
    return out

def _normalize_text_blocks(value: str) -> str:
    value = (value or "").replace("\r\n","\n").replace("\r","\n").replace("\\n","\n")
    value = re.sub(r"<br\s*/?>", "\n", value, flags=re.I)
    value = re.sub(r"[ \t]+", " ", value)
    value = re.sub(r"[ \t]*\n[ \t]*", "\n", value)
    value = re.sub(r"\n{3,}", "\n", value)
    return value.strip()

def _split_readable_paragraphs(value: str, max_chars: int = 520) -> List[str]:
    raw = _normalize_text_blocks(value or "")
    blocks = [x.strip() for x in re.split(r"\n\s*\n+", raw) if strip_html_text(x).strip()]
    if not blocks: return []
    out = []
    for block in blocks:
        plain = strip_html_text(block).strip()
        if len(plain) <= max_chars:
            out.append(block); continue
        sentences = re.split(r"(?<=[.!?؟:])\s+", block)
        current = ""
        for sent in sentences:
            sent = sent.strip()
            if not sent: continue
            candidate = (current + " " + sent).strip()
            if current and len(strip_html_text(candidate)) > max_chars:
                out.append(current.strip()); current = sent
            else:
                current = candidate
        if current: out.append(current.strip())
    return out

def _remove_duplicate_title_from_body(title: str, value: str) -> str:
    text = _normalize_text_blocks(value or "")
    title_plain = strip_html_text(title or "").strip()
    if not text or not title_plain: return text
    blocks = [x.strip() for x in re.split(r"\n\s*\n+", text) if strip_html_text(x).strip()]
    if not blocks: return text
    kept = []; skipping = True
    for block in blocks:
        plain = strip_html_text(block).strip()
        sim = SequenceMatcher(None, plain.lower(), title_plain.lower()).ratio() if plain else 0
        looks_like_title = (sim >= 0.72 or (title_plain.lower() in plain.lower() and len(plain) <= max(40, len(title_plain)*1.8)))
        if skipping and looks_like_title: continue
        skipping = False
        kept.append(block)
    return "\n".join(kept)

def _mandatory_quote_block(paragraphs: List[str], start_index: int = 1) -> Tuple[str, int]:
    if not paragraphs: return "", -1
    order = list(range(start_index, len(paragraphs))) + list(range(0, start_index))
    for idx in order:
        plain = strip_html_text(paragraphs[idx]).strip()
        if len(plain) < 20: continue
        sentences = [x.strip() for x in re.split(r"(?<=[.!?؟])\s+", plain) if x.strip()]
        excerpt = next((x for x in sentences if 20 <= len(x) <= 220), "")
        if not excerpt:
            excerpt = plain[:180].rsplit(" ",1)[0] + ("…" if len(plain) > 180 else "")
        return f"<blockquote>🔎 {html.escape(excerpt, quote=False)}</blockquote>", idx
    return "", -1

def dedupe_adjacent_emojis(text: str) -> str:
    emojis = ["💻","⚙️","🚀","","🤖","","⚡","","🛡️","🔐","","🧩","","💡","🧭","📝","🌐","✨","📌","","📱","","🛰️","","🛠️","🎯","📢","📰",""]
    for e in emojis:
        while f"{e} {e}" in text: text = text.replace(f"{e} {e}", e)
        while f"{e}{e}" in text: text = text.replace(f"{e}{e}", e)
    return text

def _visualize_plain_paragraphs(title: str, value: str, category: str, article: bool = False) -> str:
    value = _remove_duplicate_title_from_body(title, value or "")
    value = re.sub(r"</?blockquote[^>]*>", "", value, flags=re.I)
    clean = sanitize_telegram_html(_normalize_text_blocks(value))
    plain = strip_html_text(clean)
    if not plain: return ""
    emoji_map = {"ai":["🤖","🧠","","⚡","🧩"],"cyber":["🛡️","🔐","🚨","⚠️","🔎"],"tech":["💻","⚙️","","🔎","🧪"],"edu":["📚","","🧭","","🎓"],"general":["🌐","✨","📌","🔭","🧭"]}
    icons = emoji_map.get(category, emoji_map["tech"])
    paragraphs = _split_readable_paragraphs(clean, max_chars=430 if not article else 560) or [clean]
    out = [f"<b>{icons[0]} {html.escape(strip_html_text(title)[:220])}</b>"]
    quote, quote_index = _mandatory_quote_block(paragraphs, start_index=1)
    last_icon = None
    for i, para in enumerate(paragraphs[:12]):
        pplain = strip_html_text(para).strip()
        title_similarity = SequenceMatcher(None, pplain.lower(), strip_html_text(title).lower()).ratio()
        if not pplain or title_similarity > 0.82: continue
        if i == quote_index:
            out.append(quote); continue
        icon = icons[i % len(icons)]
        if icon == last_icon: icon = icons[(i+1) % len(icons)]
        last_icon = icon
        has_rich = any(tag in para.lower() for tag in ("<b>","<strong>","<i>","<em>","<u>","<s>","<a ","<pre>","<code>"))
        if has_rich:
            formatted = _protect_bidi_latin(para.strip())
        else:
            formatted = _format_technical_tokens(_protect_bidi_latin(html.escape(pplain, quote=False)))
        if i == 1: formatted = f"{icon} <b>{formatted}</b>"
        elif i == 3 and len(pplain) <= 140: formatted = f"{icon} <i>{formatted}</i>"
        else: formatted = f"{icon} {formatted}"
        out.append(formatted)
    return dedupe_adjacent_emojis("\n".join(out))

def clean_channel_copy(value: str) -> str:
    text = normalize_model_text(value or "")
    for pat in [r"(?:📖\s*)?(?:بیشتر بخوانید|ادامه مطلب|برای ادامه(?: متن| مطلب)?(?: روی| از) لینک(?: زیر)? کلیک کنید)\s*", r"(?:روی لینک|از طریق لینک) (?:زیر|بالا) کلیک کنید", r"لینک ادامه مطلب\s*", r"<a\s+href=[^>]+>\s*(?:منبع اصلی|منبع)\s*</a>"]:
        text = re.sub(pat, "", text, flags=re.I|re.S)
    return re.sub(r"\n{3,}", "\n", text).strip()

def ensure_rich_channel_format(title: str, value: str, category: str = "tech") -> str:
    return _visualize_plain_paragraphs(title, clean_channel_copy(value or ""), category, article=False)

def ensure_rich_article_format(title: str, value: str, source_url: str, category: str = "tech") -> str:
    clean = _normalize_text_blocks(value or "")
    if not strip_html_text(sanitize_telegram_html(clean)): return ""
    return _visualize_plain_paragraphs(title, clean, category, article=True)

def remove_article_metadata_blocks(value: str) -> str:
    text = _normalize_text_blocks(value or "")
    text = re.sub(r"(?:<u>)?\s*🔗\s*لینک(?:‌| )های مرتبط.*$", "", text, flags=re.I|re.S)
    text = re.sub(r"\n+.*?تاریخ انتشار\s*:.+?(?=\n|$)", "", text, flags=re.I)
    text = re.sub(r"\n+<i>⏱.*?پیش</i>", "", text, flags=re.I|re.S)
    return _normalize_text_blocks(text)

def relative_time_label(value: str) -> str:
    if not value: return "زمان نامشخص"
    try:
        dt = datetime.fromisoformat(str(value).replace('Z','+00:00'))
        if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        seconds = max(0, int((now - dt.astimezone(timezone.utc)).total_seconds()))
        def fa(n): return str(n).translate(str.maketrans("0123456789","۰۱۲۳۴۵۶۷۸۹"))
        if seconds < 60: return "همین الان"
        minutes = seconds // 60
        if minutes < 60: return f"{fa(minutes)} دقیقه پیش"
        hours = minutes // 60
        if hours < 24: return f"{fa(hours)} ساعت پیش"
        days = hours // 24
        if days < 7: return f"{fa(days)} روز پیش"
        weeks = days // 7
        if weeks < 5: return f"{fa(weeks)} هفته پیش"
        months = days // 30
        if months < 12: return f"{fa(months)} ماه پیش"
        return f"{fa(days // 365)} سال پیش"
    except Exception:
        return "زمان نامشخص"

def rich_article_fallback(title: str, text: str, source_url: str = "") -> str:
    clean = sanitize_telegram_html(_normalize_text_blocks(text or ""))
    plain = strip_html_text(clean).strip()
    if not plain: plain = "اطلاعات کافی برای تهیه متن کامل از منبع دریافت شد."
    if len(plain) > 3600: plain = plain[:3600].rsplit(" ",1)[0] + "…"
    paragraphs = [x.strip() for x in re.split(r"\n\s*\n+", plain) if x.strip()]
    chunks = [f"<b>📰 {html.escape(title[:220], quote=False)}</b>"]
    for i, paragraph in enumerate(paragraphs[:8]):
        safe = html.escape(paragraph, quote=False)
        if i == 0: chunks.append(f"🔎 {safe}")
        elif i in (2,5) and len(paragraph) >= 80: chunks.append(f"<blockquote>💡 {safe}</blockquote>")
        else: chunks.append(f"📌 {safe}")
    body = "\n".join(chunks)
    main = normalize_url(source_url or "")
    if main: body = body.rstrip() + f'\n<a href="{html.escape(main, quote=True)}">منبع اصلی</a>'
    return dedupe_adjacent_emojis(body)

def rich_channel_fallback(title: str, text: str) -> str:
    clean = strip_html_text(text or "")
    if len(clean) > 700: clean = clean[:700].rsplit(" ",1)[0] + "…"
    return f"<b>🔎 {html.escape(title[:180])}</b>\n{html.escape(clean)}"

def sanitize_resource_links(raw_links):
    out = []; seen = set()
    if not isinstance(raw_links, list): return out
    for item in raw_links:
        if not isinstance(item, dict): continue
        url = normalize_url(str(item.get("url") or ""))
        label = strip_html_text(str(item.get("label") or item.get("title") or "")).strip()
        if not url.startswith(("http://","https://")) or not label or url in seen: continue
        seen.add(url); out.append({"label": label[:120], "url": url})
    return out[:5]

def append_resource_links(article_html: str, resource_links, source_url: str = "") -> str:
    clean = remove_article_metadata_blocks(article_html)
    main = normalize_url(source_url or "")
    if main:
        clean = re.sub(r'<a\s+href=["\']'+re.escape(main)+r'["\'][^>]*>.*?</a>', '', clean, flags=re.I|re.S)
        clean = re.sub(r'<a\s+href=["\'][^"\']+["\'][^>]*>\s*(?:منبع اصلی|منبع)\s*</a>', '', clean, flags=re.I|re.S)
        clean = re.sub(r'(?:<u>|<b>|<strong>|<i>|<em>)?\s*🔗?\s*(?:لینک(?:‌| )های مرتبط|منبع اصلی|منبع)\s*(?:</u>|</b>|</strong>|</i>|</em>)?', '', clean, flags=re.I)
        clean = re.sub(r'\n{3,}','\n',clean).strip()
    rendered = []
    if main: rendered.append(f'<a href="{html.escape(main, quote=True)}">منبع اصلی</a>')
    for x in sanitize_resource_links(resource_links):
        label = x["label"]; url = normalize_url(x["url"])
        if url == main or not re.search(r"ثبت[-‌ ]?نام|عضویت|دانلود|دریافت|مستندات|docs|register|signup|خرید|قیمت|demo|دمو|مشاهده", label, re.I): continue
        rendered.append(f'<a href="{html.escape(url, quote=True)}">{html.escape(label)}</a>')
        break
    return clean.rstrip() + "\n" + " · ".join(rendered) if rendered else clean

async def resolve_article_image(db: D1Database, article: dict) -> str:
    return normalize_url(article.get("image_url") or "")

def make_article_png(width=1280, height=720):
    return b""

def extract_xml_locs_resilient(text: str) -> List[str]:
    found = []
    for m in re.finditer(r"<loc[^>]*>\s*(.*?)\s*</loc>", text or "", flags=re.I|re.S):
        u = html.unescape(re.sub(r"<[^>]+>","",m.group(1)).strip())
        if u: found.append(normalize_url(u))
    return [u for u in dict.fromkeys(found) if u]

def format_source_publication_date(raw: str) -> str:
    raw = normalize_model_text(raw or "").strip()
    if not raw: return ""
    try:
        dt = datetime.fromisoformat(raw.replace("Z","+00:00"))
        return dt.strftime("%Y-%m-%d")
    except Exception:
        m = re.search(r"(20\d{2})[-/](\d{1,2})[-/](\d{1,2})", raw)
        if m: return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
        return ""

def manager_accepts_score(score: float, min_score: float) -> bool:
    try: score = float(score or 0); minimum = float(min_score or 0)
    except Exception: return False
    if minimum <= 1: return True
    return score >= max(0.0, minimum - MANAGER_SCORE_TOLERANCE)

def _persian_ratio(text: str) -> float:
    plain = strip_html_text(text or "")
    if not plain: return 0.0
    fa = len(re.findall(r"[؀-ۿ]", plain))
    return fa / max(1, len(re.sub(r"\s+","",plain)))

def _latin_ratio(text: str) -> float:
    plain = strip_html_text(text or "")
    if not plain: return 0.0
    latin = len(re.findall(r"[A-Za-z]", plain))
    return latin / max(1, len(re.sub(r"\s+","",plain)))

def _needs_persian_rewrite(title: str, channel: str, article: str) -> bool:
    sample = " ".join([title or "", channel or "", article or ""])[:9000]
    if _latin_ratio(sample) > 0.42 and _persian_ratio(sample) < 0.45: return True
    for block in re.split(r"\n\s*\n+", strip_html_text(sample)):
        letters = len(re.findall(r"[A-Za-z]", block))
        fa = len(re.findall(r"[؀-ۿ]", block))
        if letters >= 80 and letters > fa * 2.2: return True
    return False

# FIX جدید: برش امن HTML بدون شکستن تگ‌ها (برای ارسال چندبخشی مقاله)
def split_html_safe(value: str, limit: int = 3800) -> List[str]:
    value = sanitize_telegram_html(value or "")
    if not value: return []
    if plain_len(value) <= limit: return [value]
    blocks = [b.strip() for b in re.split(r"\n\s*\n+", value) if strip_html_text(b).strip()]
    chunks, cur = [], ""
    for b in blocks:
        cand = (cur + "\n\n" + b).strip() if cur else b
        if plain_len(cand) <= limit:
            cur = cand
        else:
            if cur: chunks.append(cur)
            if plain_len(b) <= limit:
                cur = b
            else:
                plain = strip_html_text(b)
                for i in range(0, len(plain), limit):
                    chunks.append(html.escape(plain[i:i+limit], quote=False))
                cur = ""
    if cur: chunks.append(cur)
    return chunks

def _trim_rich_blocks_to_limit(value: str, max_plain_chars: int = 760) -> str:
    clean = sanitize_telegram_html(clean_channel_copy(value or ''))
    if plain_len(clean) <= max_plain_chars: return clean
    blocks = [b.strip() for b in re.split(r"\n\s*\n+", clean) if strip_html_text(b).strip()]
    while len(blocks) > 1 and plain_len("\n".join(blocks)) > max_plain_chars:
        blocks.pop()
    trimmed = "\n".join(blocks)
    if plain_len(trimmed) > max_plain_chars:
        plain = strip_html_text(trimmed)[:max_plain_chars].rsplit(' ',1)[0] + "…"
        return html.escape(plain, quote=False)
    return trimmed

def publication_caption(title: str, channel_html: str, deep_link: str) -> str:
    link = f'<a href="{html.escape(deep_link, quote=True)}">📖 بیشتر بخوانید</a>'
    clean = _trim_rich_blocks_to_limit(channel_html, 760)
    caption = (clean.strip() + "\n" + link).strip()
    if len(caption) <= 1024: return caption
    blocks = [b.strip() for b in re.split(r"\n\s*\n+", sanitize_telegram_html(clean)) if strip_html_text(b).strip()]
    while len(blocks) > 1 and len("\n".join(blocks) + "\n" + link) > 1024:
        blocks.pop()
    candidate = ("\n".join(blocks) + "\n" + link).strip()
    if len(candidate) <= 1024: return candidate
    plain = strip_html_text("\n".join(blocks))
    budget = max(120, 1024 - len(strip_html_text(link)) - 8)
    plain = plain[:budget].rsplit(' ',1)[0] + "…"
    return html.escape(plain, quote=False) + "\n" + link

async def ai_analyze_candidate(ai, item: Dict[str, Any], source: Dict[str, Any], recent_titles: List[str]) -> Dict[str, Any]:
    body = (item.get("body") or item.get("description") or "")[:10000]
    sim = recent_semantic_similarity(item.get("title",""), recent_titles)
    prompt = f"""تو سردبیر ارشد یک کانال فارسی درباره تکنولوژی، هوش مصنوعی، ابزارها، مدل‌های AI، امنیت سایبری و اخبار مهم فناوری هستی.
منبع: {source.get('name')} | دسته: {source.get('category')}
عنوان: {item.get('title')} | لینک: {item.get('url')} | تاریخ: {item.get('published_at')}
خلاصه/متن: {body}
شباهت با عناوین اخیر: {sim:.2f}
clickbait، تبلیغ کم‌ارزش، شایعه، تکراری و فاقد ارزش را رد کن. فقط JSON معتبر:
{{"accept": true/false, "score": 0-100, "category": "ai|tech|cyber|edu|general", "importance_reason": "...", "iran_relevance": 0-10, "freshness": 0-10, "reliability": 0-10, "duplicate_risk": 0-10, "event_date": "...", "why": "..."}}"""
    result = await ai.call([{"role":"system","content":"You are a strict editorial gate. Output JSON only."},{"role":"user","content":prompt}], temperature=0.1, max_tokens=900, purpose="candidate_scoring")
    obj = parse_json_object(result.get("content",""))
    if not obj: return {"accept": False, "score": 0, "reason": "AI returned invalid JSON", "ai": result}
    return {**obj, "ai": result}

async def ai_generate_content(ai, item: Dict[str, Any], analysis: Dict[str, Any], source: Dict[str, Any]) -> Dict[str, Any]:
    source_text = (item.get("body") or item.get("description") or "")[:14000]
    prompt = f"""برای یک کانال فارسی حرفه‌ای در حوزه تکنولوژی و AI محتوا تولید کن.
منبع: {source.get('name')} | URL: {item.get('url')} | عنوان: {item.get('title')}
تحلیل قبلی: {json.dumps(analysis, ensure_ascii=False)}
متن منبع:
{source_text}
خروجی فقط JSON معتبر:
{{"title": "...", "channel_text": "400-600 کاراکتر، خود خبر، بدون لینک", "article_text": "مقاله عمیق‌تر مستقل", "category": "ai|tech|cyber|edu|general", "facts": ["..."], "image_note": "..."}}
هیچ ادعای بدون پشتیبانی نساز."""
    result = await ai.call([{"role":"system","content":"You are an expert Persian technology editor. Output JSON only."},{"role":"user","content":prompt}], temperature=0.35, max_tokens=4500, purpose="content_generation")
    obj = parse_json_object(result.get("content",""))
    if not obj: return {"error": "invalid generation JSON", "ai": result}
    return {**obj, "ai": result}

async def ai_verify_content(ai, item: Dict[str, Any], generated: Dict[str, Any]) -> Dict[str, Any]:
    prompt = f"""محتوای زیر را با منبع مقایسه کن.
SOURCE: عنوان: {item.get('title')} | URL: {item.get('url')} | متن: {(item.get('body') or item.get('description') or '')[:12000]}
GENERATED:
{json.dumps(generated, ensure_ascii=False)}
فقط JSON: {{"ok": true/false, "issues": ["..."], "confidence": 0-100}}"""
    result = await ai.call([{"role":"system","content":"You are a strict fact-checking editor. Output JSON only."},{"role":"user","content":prompt}], temperature=0, max_tokens=1200, purpose="content_verification")
    obj = parse_json_object(result.get("content",""))
    return obj if obj else {"ok": False, "issues": ["invalid verifier response"], "confidence": 0}
    
    # ============================================================
# PART 3/4 — Editorial pipeline + WebScout cycle + Publication + Loop + Reports
# ============================================================
async def ai_editorial_process(ai: AIProviderManager, item: Dict[str, Any], source: Dict[str, Any], recent_titles: List[str], weights: Dict[str, float], manager_prompts: Optional[Dict[str, str]] = None):
    body = (item.get("webscout_research") or item.get("body") or item.get("description") or "")[:30000]
    manager_prompts = manager_prompts or {}
    channel_scope = manager_prompts.get("channel") or "تمرکز روی خبرهای فنی و ارزشمند؛ محتوای سطحی و کلیشه‌ای را کنار بگذار."
    article_scope = manager_prompts.get("article") or "نسخه کامل را فنی، غنی و مبتنی بر واقعیت‌های منبع بنویس."
    editorial_schema = {
        "accept": True, "score": 0, "global_relevance": 0, "technology_relevance": 0,
        "ai_relevance": 0, "cyber_relevance": 0, "education_relevance": 0, "iran_relevance": 0,
        "freshness": 0, "reliability": 0, "duplicate_risk": 0, "category": "ai|tech|cyber|edu|general",
        "why": "...", "title": "...", "channel_html": "...", "article_html": "...",
        "facts": ["..."], "resource_links": [{"label": "...", "url": "https://..."}]
    }
    prompt = f"""تو موتور تحریریه و تولید محتوای یک کانال فارسی حرفه‌ای هستی؛ نه قاضی و نه مفسر.
وظیفه تو ساخت محتوای فنی، غنی، دقیق، بی‌طرف و قابل‌فهم از منبع داده‌شده است. انتخاب نهایی فقط بر اساس معیارهای عددی مدیر انجام می‌شود؛ در متن نهایی قضاوت یا توصیه ننویس.
موضوعات کانال: فناوری، هوش مصنوعی، ابزارها و مدل‌ها، امنیت سایبری، آموزش و اخبار مهم جهان. مخاطب: فارسی‌زبان‌ها.
منبع: {source.get('name')}
عنوان: {item.get('title')}
URL: {item.get('url')}
تاریخ انتشار منبع: {item.get('published_at') or 'نامشخص'}
لینک‌های داخل صفحه:
{json.dumps(item.get('links') or [], ensure_ascii=False)[:5000]}
متن پژوهش WebScout و محتوای واقعاً بازیابی‌شده:
{body}
وزن‌های مدیر:
{json.dumps(weights, ensure_ascii=False)}
دستور مدیر برای نسخه کوتاه کانال (حدود 400 تا 600 کاراکتر):
{channel_scope}
دستور مدیر برای نسخه کامل داخل ربات (حدود 2000 تا 3000 کاراکتر):
{article_scope}
این دستورها فقط مشخص می‌کنند چه اطلاعاتی پوشش داده شود؛ قالب‌بندی وظیفه ربات است.
اول امتیاز 0 تا 100 بده؛ accept فقط توضیح داخلی است و دروازه انتشار نیست. صرفاً به‌دلیل کوتاهی متن accept=false نده. تاریخ را حدس نزن. اگر منبع کوتاه است، بهترین محتوای کوتاه و دقیق ممکن را فقط بر پایه همان اطلاعات بساز؛ جزئیات، عدد یا ادعای ساختگی اضافه نکن.
اگر accept=true، همزمان محتوای نهایی را تولید کن:
1) channel_html: حدود 400 تا 600 کاراکتر «خودِ خبر»؛ نه teaser. ساختار بصری: شروع با <b>، حداکثر 1 بخش کوتاه <i> یا <blockquote> فقط وقتی طبیعی است، 1 تا 3 ایموجی دقیق.
2) article_html: حدود 2000 تا 3000 کاراکتر متناسب با غنای WebScout؛ اگر اطلاعات کمتر بود کوتاه‌تر بمان و هرگز با حرف اضافه حجم را پر نکن. مستقل و غنی‌تر از متن کانال؛ تیترهای کوتاه <b>، پاراگراف کوتاه، در صورت مناسب <blockquote>.
3) title کوتاه، جذاب و غیرکلیک‌بیتی. 4) category و facts.
قواعد نگارش:
- فارسی روان، دوستانه و خوش‌خوان؛ رسمی و خشک نباش.
- اصطلاح فنی: معادل فارسی اول، انگلیسی فقط داخل پرانتز یا <code>. پاراگراف کامل انگلیسی ممنوع؛ فقط نام مدل‌ها/شرکت‌ها/محصولات می‌توانند انگلیسی بمانند.
- در هر پاراگراف حداکثر یک ایموجی؛ دو ایموجی کنار هم ممنوع.
- نسخه کانال و کامل هر دو حداقل یک Quote کوتاه واقعی داشته باشند؛ اگر جمله مستقیم نیست، یک جمله عیناً از منبع را Quote کن، نه نقل‌قول ساختگی. Quote هرگز کل مقاله نباشد.
- کد/دستور/API دقیق داخل <code>؛ قطعه‌کد واقعی داخل <pre>.
- کاراکتر متنی "\\n" نده؛ newline واقعی استفاده کن.
- سؤال‌ها را رها نکن؛ پاسخ و اطلاعات منبع را مستقیم بیان کن. نتیجه‌گیری شخصی یا قضاوت تحمیل نکن.
- «طبق منبع» فقط وقتی لازم است برای نسبت‌دادن ادعا.
- متن فقط از تحقیق WebScout ساخته شود؛ از دانش عمومی یا حدس برای پر کردن حجم استفاده نکن.
- channel_html و article_html با HTML سازگار Telegram؛ Markdown ممنوع.
- اگر صفحه ثبت‌نام/دوره/ابزار/مستندات را معرفی می‌کند و URL آن در لینک‌های داخل صفحه هست، در resource_links بیاور؛ URL را حدس نزن.
- لینک Deep Link توسط برنامه اضافه می‌شود؛ عبارت «ادامه مطلب را از لینک زیر بخوانید» یا مشابه آن را در متن کانال ننویس.
فقط JSON معتبر:
{json.dumps(editorial_schema, ensure_ascii=False)}"""
    # FIX: persist_health=True تا خرابی تحریریه هم سلامت مدل را ثبت کند
    result = await ai.call([{"role": "system", "content": "You are a Persian technology content producer. Be neutral and factual. Use only the WebScout research supplied; do not invent missing facts. Return JSON only."}, {"role": "user", "content": prompt}], 0.25, 6000, "editorial")
    obj = parse_json_object(result.get("content", ""))
    if not obj:
        repair_prompt = ("پاسخ زیر را فقط به JSON معتبر تبدیل کن؛ محتوا را تغییر نده. فیلدها: accept,score,category,iran_relevance,freshness,reliability,duplicate_risk,why,title,channel_html,article_html,facts.\n" + str(result.get("content", ""))[:14000])
        retry = await ai.call([{"role": "system", "content": "Return valid JSON only."}, {"role": "user", "content": repair_prompt}], 0, 4200, "editorial_json_repair")
        obj = parse_json_object(retry.get("content", "")); result = retry
    if not obj: return {"error": "پاسخ AI JSON معتبر نبود", "ai": result}
    raw_title = strip_html_text(obj.get("title") or item.get("title") or "")[:240]
    raw_ch = str(obj.get("channel_html") or obj.get("channel_text") or "")
    raw_ar = str(obj.get("article_html") or obj.get("article_text") or "")
    if _needs_persian_rewrite(raw_title, raw_ch, raw_ar):
        repair = ("متن زیر خروجی تحریریه است اما بخش زیادی انگلیسی شده. فقط بازنویسی فارسی انجام بده و هیچ واقعیتی را تغییر نده. نام شرکت‌ها/مدل‌ها/اصطلاحات فنی شناخته‌شده را نگه دار. خروجی فقط JSON با سه کلید title, channel_html, article_html. قالب Telegram HTML مجاز و یک Quote کوتاه نگه دار/ایجاد کن.\n" + json.dumps({"title": raw_title, "channel_html": raw_ch, "article_html": raw_ar}, ensure_ascii=False)[:20000])
        repaired = await ai.call([{"role": "system", "content": "Rewrite to fluent Persian. Return JSON only."}, {"role": "user", "content": repair}], 0.15, 4200, "editorial_persian_repair")
        pobj = parse_json_object(repaired.get("content", ""))
        if pobj:
            raw_title = strip_html_text(pobj.get("title") or raw_title)[:240]
            raw_ch = str(pobj.get("channel_html") or raw_ch)
            raw_ar = str(pobj.get("article_html") or raw_ar)
            obj["title"] = raw_title; obj["channel_html"] = raw_ch; obj["article_html"] = raw_ar
            result = repaired
    title = raw_title
    category = str(obj.get("category") or source.get("category") or "tech")
    ch = ensure_rich_channel_format(title, raw_ch, category)
    ar = ensure_rich_article_format(title, raw_ar, item.get("url") or "", category)
    if not ar: return {"error": "تولید محتوای کامل ناموفق بود - خروجی خالی", "ai": result}
    # FIX: اگر نسخه کانال خالی ماند، هرگز متن خالی منتشر نشود
    if not ch: ch = f"<b>{html.escape(title[:200])}</b>\n{html.escape(strip_html_text(raw_ar or raw_ch or title)[:500], quote=False)}"
    resource_links = sanitize_resource_links(obj.get("resource_links"))
    ar = append_resource_links(ar, resource_links, item.get("url") or "")
    obj["title"] = title; obj["channel_html"] = ch; obj["article_html"] = ar; obj["resource_links"] = resource_links
    dims = {
        "global": float(obj.get("global_relevance", 5) or 0),
        "technology": float(obj.get("technology_relevance", 5) or 0),
        "ai": float(obj.get("ai_relevance", 5) or 0),
        "cyber": float(obj.get("cyber_relevance", 5) or 0),
        "education": float(obj.get("education_relevance", 5) or 0),
        "iran": float(obj.get("iran_relevance", 0) or 0),
        "freshness": float(obj.get("freshness", 5) or 0),
        "source": max(0, min(10, float(source.get("trust_score") or 80) / 10)),
        "novelty": 10 - max(0, min(10, float(obj.get("duplicate_risk", 0) or 0)))
    }
    total_weight = sum(max(0, float(weights.get(k, 0))) for k in dims)
    weighted = sum(max(0, min(10, v)) * max(0, float(weights.get(k, 0))) for k, v in dims.items())
    obj["score"] = round((weighted / (total_weight * 10)) * 100, 1) if total_weight else round(float(obj.get("score", 0) or 0), 1)
    return {**obj, "ai": result}

async def get_manager_editorial_prompts(db: D1Database) -> Dict[str, str]:
    return {
        "channel": await get_setting(db, "editorial_prompt_channel", "فقط محتوای فنی و واقعاً ارزشمند برای مخاطب فناوری و هوش مصنوعی را پوشش بده."),
        "article": await get_setting(db, "editorial_prompt_article", "نسخه کامل باید فنی، غنی و مبتنی بر واقعیت‌های منبع باشد.")
    }

async def add_source(db: D1Database, url: str, category: str = "tech", interval_minutes: Optional[int] = None, priority: int = 5) -> int:
    clean = normalize_url(url)
    if not clean: raise ValueError("invalid URL")
    count = await db.execute("SELECT COUNT(*) c FROM sources")
    if (count[0].get("c", 0) if count else 0) >= MAX_AUTOMATION_SOURCES:
        raise ValueError(f"حداکثر {MAX_AUTOMATION_SOURCES} منبع مجاز است.")
    parsed = urllib.parse.urlsplit(clean)
    name = parsed.netloc or clean
    interval = interval_minutes or int(await get_setting(db, "default_source_interval", str(DEFAULT_SOURCE_INTERVAL_MINUTES)))
    now = datetime.now(timezone.utc)
    try:
        res = await db.execute("INSERT INTO sources(name,url,category,enabled,interval_minutes,priority,next_check_at,created_at) VALUES(?,?,?,1,?,?,?,?) RETURNING id", [name, clean, category, interval, priority, now.isoformat(), now.isoformat()])
        source_id = res[0].get("id") if res else 0
    except Exception:
        source_id = 0
    if not source_id:
        r = await db.execute("SELECT id FROM sources WHERE url=?", [clean])
        source_id = r[0].get("id") if r else 0
    invalidate_sources()
    return int(source_id)

def source_is_due(s: Dict[str, Any], now_dt: datetime) -> bool:
    raw = s.get("next_check_at") or ""
    if not raw: return True
    try: return datetime.fromisoformat(raw.replace("Z", "+00:00")) <= now_dt
    except Exception: return True

async def update_source_after_check(db: D1Database, source: Dict[str, Any], interval_minutes: int, error: Optional[str] = None):
    now = datetime.now(timezone.utc)
    try:
        await db.execute("UPDATE sources SET last_checked_at=?, next_check_at=?, last_error=? WHERE id=?", [now.isoformat(), (now + timedelta(minutes=max(1, int(interval_minutes)))).isoformat(), error, source.get("id")])
        invalidate_sources()
    except Exception: pass

async def fetch_source_cycle(db: D1Database, source: Dict[str, Any], ai: AIProviderManager, progress=None, allow_old_test=False):
    stats = {'source': source.get('name') or source.get('url'), 'found': 0, 'seen': 0, 'candidates': 0, 'processed': 0, 'accepted': 0, 'rejected': 0, 'errors': 0, 'queued': 0, 'method': 'webscout', 'diagnostics': []}
    url = normalize_url(source.get('url') or '')
    if not url:
        stats['errors'] = 1; stats['diagnostics'] = ['URL منبع نامعتبر است']; return stats
    # FIX: بررسی کانال قبل از هر پردازش سنگین (جلوگیری از مقاله یتیم)
    channel_id = await get_channel_id(db)
    if not channel_id: raise RuntimeError('CHANNEL_ID تنظیم نشده است')
    freshness = float(await get_setting(db, 'webscout_freshness_hours', str(WEBSCOUT_FRESHNESS_HOURS)) or WEBSCOUT_FRESHNESS_HOURS)
    weights = {k: float(await get_setting(db, 'weight_' + k, '10')) for k in ['global', 'technology', 'ai', 'cyber', 'education', 'iran', 'freshness', 'source', 'novelty']}
    prompts = await get_manager_editorial_prompts(db)
    min_score = float(await get_setting(db, 'min_content_score', str(DEFAULT_MIN_CONTENT_SCORE)))
    recent_titles = [r.get('title', '') for r in await db.execute("SELECT title FROM articles WHERE status IN ('published','ready') ORDER BY id DESC LIMIT 50")]
    scout_prompt = f"""You are the WebScout selection engine for a Persian technology news automation system.
Open and inspect the TARGET URL and search within that site for the newest substantive item published within the last {freshness:g} hours. Use web search and page fetching; do not rely on model memory.
Manager weights: {json.dumps(weights, ensure_ascii=False)}
Channel instruction: {prompts.get('channel','')}
Article instruction: {prompts.get('article','')}
Required decision:
1) Find a real, current item on this site satisfying the manager criteria.
2) Verify publication time is within the last {freshness:g} hours.
3) Verify the page is substantive, not a teaser/paywall shell/listing/advertisement/duplicate.
4) If no qualifying item exists, return exactly: FALSE
5) Otherwise return ONLY valid JSON: title, article_url, published_at, image_url, score, research_text, resource_links, facts.
6) research_text must contain rich factual material actually retrieved; preserve details, numbers, names, examples, technical explanations, limitations and context. Do not invent.
7) score 0-100 reflects match with manager criteria.
8) resource_links = real URLs actually encountered.
9) If the publication timestamp cannot be verified, return FALSE rather than guessing.
10) Do not write the final Telegram article here; research/selection only.
11) Return FALSE when no item satisfies all criteria; never return a weak substitute."""
    if progress: await progress('scout', f"🌐 {source.get('name')}: WebScout در حال بررسی {url}…")
    scout = await ai.webscout_call(url, scout_prompt, max_tokens=9000)
    if not scout.get('ok'):
        stats['errors'] = 1; stats['diagnostics'] = [str(scout.get('error') or 'WebScout failed')]; return stats
    raw = str(scout.get('content') or '').strip()
    if raw.upper() == "FALSE" or raw.upper().startswith("FALSE\n"):
        stats['diagnostics'] = ['WebScout: FALSE — موردی با معیارهای مدیر پیدا نشد']; return stats
    obj = parse_json_object(raw)
    if obj and obj.get("found") is False:
        stats['diagnostics'] = ['WebScout: FALSE — موردی با معیارهای مدیر پیدا نشد']; return stats
    if not obj or not obj.get('article_url') or not obj.get('research_text'):
        stats['errors'] = 1; stats['diagnostics'] = ['WebScout پاسخ ساختاریافته و قابل استفاده نداد']; return stats
    pub = parse_publication_datetime(str(obj.get('published_at') or ''))
    if pub:
        age = (datetime.now(timezone.utc) - pub).total_seconds() / 3600.0
        if age < -0.5 or age > freshness:
            stats['diagnostics'] = [f'WebScout زمان انتشار خارج از پنجره بود: {age:.1f} ساعت']; return stats
    else:
        # FIX: در حالت سخت‌گیر، تاریخ غیرقابل تأیید = رد
        if NEWS_FRESHNESS_STRICT and not allow_old_test:
            stats['rejected'] = 1; stats['processed'] = 1
            stats['diagnostics'] = ['تاریخ انتشار قابل تأیید نبود']; return stats
    stats['found'] = 1; stats['candidates'] = 1
    item = {
        'title': strip_html_text(str(obj.get('title') or ''))[:500],
        'url': normalize_url(str(obj.get('article_url') or url)),
        'description': '',
        'body': str(obj.get('research_text') or ''),
        'webscout_research': str(obj.get('research_text') or ''),
        'image_url': normalize_url(str(obj.get('image_url') or '')),
        'published_at': str(obj.get('published_at') or ''),
        'links': obj.get('resource_links') if isinstance(obj.get('resource_links'), list) else [],
        'webscout_score': float(obj.get('score') or 0)
    }
    # FIX: تشخیص paywall/محتوای ناکافی
    insuff, reason = is_insufficient_content(item['title'], item['body'], item.get('description', ''))
    if insuff:
        stats['rejected'] = 1; stats['processed'] = 1; stats['diagnostics'] = [reason]
        await log_automation(db, 'INFO', 'content_rejected', f"{source.get('name')} | {reason}")
        return stats
    # FIX: جلوگیری از تکرار با content_hash + source_url
    content_hash = text_hash((item.get('title') or '') + " " + (item.get('body') or '')[:5000])
    if await db.execute("SELECT id FROM articles WHERE source_url=? OR content_hash=? LIMIT 1", [item['url'], content_hash]):
        stats['rejected'] = 1; stats['processed'] = 1; stats['diagnostics'] = ['محتوا تکراری است']; return stats
    out = await ai_editorial_process(ai, item, source, recent_titles, weights, prompts)
    stats['processed'] = 1
    if out.get('error'):
        stats['errors'] = 1; stats['diagnostics'] = [str(out.get('error'))]; return stats
    if not manager_accepts_score(float(out.get('score', 0) or 0), min_score):
        stats['rejected'] = 1; stats['diagnostics'] = [f"امتیاز نهایی {out.get('score','-')} کمتر از حد مدیر {min_score:g}"]
        await log_automation(db, 'INFO', 'content_rejected', f"{source.get('name')} | score={out.get('score')}")
        return stats
    now = datetime.now(timezone.utc).isoformat()
    art = await db.execute("INSERT INTO articles(source_item_id,title,channel_text,body,source_url,image_url,category,score,status,created_at,source_published_at,content_hash) VALUES(NULL,?,?,?,?,?,?,?,'ready',?,?,?) RETURNING id",
        [out.get('title') or item['title'], out.get('channel_html') or out.get('channel_text') or '', out.get('article_html') or out.get('article_text') or '', item['url'], item.get('image_url') or '', out.get('category') or source.get('category', 'tech'), float(out.get('score') or 0), now, item.get('published_at', '')[:100], content_hash])
    aid = int(art[0]['id']) if art else 0
    if not aid: raise RuntimeError('ذخیره مقاله ناموفق بود')
    await db.execute('UPDATE articles SET deep_token=? WHERE id=?', [make_deep_token(aid), aid])
    await db.execute("INSERT INTO publication_queue(article_id,scheduled_at,status,attempts,last_error,created_at) VALUES(?,?, 'queued',0,NULL,?)", [aid, now, now])
    stats['accepted'] = 1; stats['queued'] = 1
    stats['article_id'] = aid; stats['scheduled_at'] = now
    stats['provider'] = scout.get('provider'); stats['model'] = scout.get('model')
    stats['interval_minutes'] = int(await get_setting(db, 'webscout_success_interval_minutes', str(WEBSCOUT_SUCCESS_INTERVAL_MINUTES)) or WEBSCOUT_SUCCESS_INTERVAL_MINUTES)
    stats['diagnostics'] = [f"WebScout: TRUE · {item['title'][:120]}"]
    return stats

async def can_publish_now(db: D1Database) -> bool:
    if not await get_channel_id(db): return False
    if await get_setting(db, "automation_enabled", "0") != "1": return False
    tehran = datetime.now(pytz.timezone("Asia/Tehran"))
    start_h = int(await get_setting(db, "publish_start_hour", str(DEFAULT_PUBLISH_START_HOUR)))
    end_h = int(await get_setting(db, "publish_end_hour", str(DEFAULT_PUBLISH_END_HOUR)))
    # FIX: پشتیبانی از پنجره شبانه (مثلاً 22 تا 2)
    in_window = (start_h <= tehran.hour <= end_h) if start_h <= end_h else (tehran.hour >= start_h or tehran.hour <= end_h)
    if not in_window: return False
    day_start = tehran.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc).isoformat()
    count_rows = await db.execute("SELECT COUNT(*) c FROM articles WHERE status='published' AND COALESCE(published_at,created_at) >= ?", [day_start])
    max_daily = int(await get_setting(db, "max_daily_posts", str(DEFAULT_MAX_DAILY_POSTS)))
    if (count_rows[0].get("c", 0) if count_rows else 0) >= max_daily: return False
    last_manual = await get_setting(db, "last_manual_channel_post_at", "")
    last_pub = await db.execute("SELECT published_at FROM publication_queue WHERE status='published' ORDER BY id DESC LIMIT 1")
    latest_times = [x for x in [last_manual, last_pub[0].get("published_at") if last_pub else ""] if x]
    if latest_times:
        try:
            delta = datetime.now(timezone.utc) - datetime.fromisoformat(max(latest_times).replace("Z", "+00:00"))
            if delta.total_seconds() < float(await get_setting(db, "min_post_gap_minutes", str(DEFAULT_MIN_POST_GAP_MINUTES))) * 60: return False
        except Exception: pass
    return True

async def get_runtime_bot_username(bot: Bot) -> str:
    global BOT_USERNAME_RUNTIME
    if BOT_USERNAME_RUNTIME: return BOT_USERNAME_RUNTIME
    try:
        me = await bot.get_me()
        BOT_USERNAME_RUNTIME = me.username or BOT_USERNAME.lstrip("@")
    except Exception:
        BOT_USERNAME_RUNTIME = BOT_USERNAME.lstrip("@")
    return BOT_USERNAME_RUNTIME

async def recover_publication_queue(db: D1Database):
    # FIX: آزادسازی صف گیرکرده (publishing قدیمی + failed قابل تلاش)
    try:
        await db.execute("UPDATE publication_queue SET status='queued', last_error='recovered' WHERE status='publishing' AND COALESCE(last_attempt_at,created_at) < ?", [(datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()])
        await db.execute("UPDATE publication_queue SET status='queued', last_error='retry' WHERE status='failed' AND attempts < 3 AND created_at > ?", [(datetime.now(timezone.utc) - timedelta(days=1)).isoformat()])
    except Exception: pass

async def publish_next_article(db: D1Database, bot: Bot, force: bool = False) -> bool:
    global LAST_RECOVER
    async with PUBLISH_LOCK:
        if time.time() - LAST_RECOVER > 600:
            await recover_publication_queue(db); LAST_RECOVER = time.time()
        if force:
            channel_id = await get_channel_id(db)
            if not channel_id: return False
            tehran = datetime.now(pytz.timezone("Asia/Tehran"))
            count_rows = await db.execute("SELECT COUNT(*) c FROM articles WHERE status='published' AND COALESCE(published_at,created_at) >= ?", [tehran.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc).isoformat()])
            if (count_rows[0].get("c", 0) if count_rows else 0) >= int(await get_setting(db, "max_daily_posts", str(DEFAULT_MAX_DAILY_POSTS))): return False
        elif not await can_publish_now(db):
            return False
        now_iso = datetime.now(timezone.utc).isoformat()
        schedule_filter = "" if force else " AND (q.scheduled_at IS NULL OR q.scheduled_at <= ?)"
        params = [now_iso] if not force else []
        rows = await db.execute("SELECT q.id as queue_id,q.article_id,a.* FROM publication_queue q JOIN articles a ON a.id=q.article_id WHERE q.status='queued' AND a.status='ready'" + schedule_filter + " ORDER BY COALESCE(a.source_published_at,a.created_at) DESC, a.score DESC, q.created_at ASC LIMIT 1", params)
        if not rows: return False
        row = rows[0]; queue_id = row["queue_id"]; article_id = row["article_id"]
        await db.execute("UPDATE publication_queue SET status='publishing',attempts=attempts+1,last_attempt_at=? WHERE id=? AND status='queued'", [now_iso, queue_id])
        try:
            token = row.get("deep_token")
            bot_username = await get_runtime_bot_username(bot)
            if not token or not bot_username: raise RuntimeError("deep link token یا نام کاربری ربات تنظیم نشده است")
            deep_link = f"https://t.me/{bot_username}?start=article_{token}"
            channel_id = await get_channel_id(db)
            title_out = str(row.get("title") or "مطلب")
            channel_text = sanitize_telegram_html(row.get("channel_text") or "")
            if not strip_html_text(channel_text):
                channel_text = f"<b>{html.escape(title_out[:200])}</b>\n{html.escape(strip_html_text(row.get('body') or '')[:400], quote=False)}"
            image_url = await resolve_article_image(db, row)
            caption = publication_caption(title_out, channel_text, deep_link)
            sent = None
            if image_url:
                try:
                    sent = await bot.send_photo(chat_id=channel_id, photo=image_url, caption=caption, parse_mode="HTML")
                except Exception as img_error:
                    await log_automation(db, "WARN", "source_image_failed", f"article={article_id} {img_error}")
            if sent is None:
                # FIX: fallback متنی حتماً با لینک عمیق و برش امن
                try:
                    sent = await bot.send_message(chat_id=channel_id, text=caption, parse_mode="HTML", disable_web_page_preview=True)
                except Exception:
                    sent = await bot.send_message(chat_id=channel_id, text=strip_html_text(channel_text)[:3500] + f"\n{deep_link}", disable_web_page_preview=True)
            published_at = datetime.now(timezone.utc).isoformat()
            await db.execute("UPDATE articles SET status='published',published_message_id=?,published_at=?,image_url='' WHERE id=?", [getattr(sent, "message_id", 0), published_at, article_id])
            await db.execute("UPDATE publication_queue SET status='published',published_at=? WHERE id=?", [published_at, queue_id])
            await log_automation(db, "INFO", "published", f"article={article_id} message={getattr(sent,'message_id',0)} force={force}")
            return True
        except Exception as e:
            await db.execute("UPDATE publication_queue SET status='failed',last_error=? WHERE id=?", [str(e)[:1500], queue_id])
            await db.execute("UPDATE articles SET status='ready' WHERE id=?", [article_id])
            await log_automation(db, "ERROR", "publication_failed", f"article={article_id} {e}")
            try:
                if ADMIN_ID: await bot.send_message(ADMIN_ID, f"❌ خطا در انتشار خودکار\nArticle: {article_id}\nError: {html.escape(str(e)[:800])}")
            except Exception: pass
            return False

async def recheck_failed_providers(db: D1Database, bot: Bot, manager: AIProviderManager):
    now = datetime.now(timezone.utc).isoformat()
    rows = await db.execute("SELECT id,name,model_name,status,cooldown_until FROM ai_providers WHERE enabled=1 AND status IN ('invalid','cooldown') AND (cooldown_until IS NULL OR cooldown_until <= ?) ORDER BY priority ASC LIMIT 8", [now])
    for p in rows:
        try:
            result = await manager.test_provider(int(p["id"]))
            if result.get("ok") and ADMIN_ID:
                try: await bot.send_message(ADMIN_ID, f"✅ <b>مدل دوباره در دسترس است</b>\nProvider: {html.escape(str(p.get('name')))}\nModel: <code>{html.escape(str(p.get('model_name')))}</code>\nLatency: {result.get('latency_ms',0)}ms", parse_mode="HTML")
                except Exception: pass
        except Exception as e:
            await log_automation(db, "ERROR", "provider_recheck_failed", f"provider={p.get('id')} {e}")

async def automation_loop(db: D1Database, bot: Bot):
    global LAST_SOURCE_ERROR_NOTICE
    ai = AIProviderManager(db, bot)
    await set_setting(db, 'worker_started_at', datetime.now(timezone.utc).isoformat())
    last_cleanup = last_provider_recheck = last_heartbeat = 0.0
    last_publish_try = last_recover = 0.0
    cursor = 0
    try:
        while True:
            now_dt = datetime.now(timezone.utc)
            try:
                if time.time() - last_heartbeat >= WEBSCOUT_HEARTBEAT_SECONDS:
                    await set_setting(db, 'worker_heartbeat_at', now_dt.isoformat()); last_heartbeat = time.time()
                if await get_setting(db, 'automation_enabled', '0') == '1':
                    if time.time() - last_recover > 600:
                        await recover_publication_queue(db); last_recover = time.time()
                    # FIX: انتشار مستقل از due بودنِ وب‌اسکات
                    if time.time() - last_publish_try >= PUBLISH_ATTEMPT_INTERVAL:
                        last_publish_try = time.time()
                        await publish_next_article(db, bot)
                    next_run_raw = await get_setting(db, 'webscout_next_run_at', '')
                    due = True
                    if next_run_raw:
                        try: due = datetime.fromisoformat(next_run_raw.replace('Z', '+00:00')) <= now_dt
                        except Exception: due = True
                    if due:
                        await set_setting(db, 'last_cycle_started_at', now_dt.isoformat())
                        rows = await get_enabled_sources(db)
                        due_rows = [s for s in rows if source_is_due(s, now_dt)]
                        # FIX: سقف منابع در هر چرخه (صرفه‌جویی سهمیه)
                        ordered = [due_rows[(cursor + i) % len(due_rows)] for i in range(min(len(due_rows), MAX_SOURCE_ITEMS_PER_CYCLE))] if due_rows else []
                        results = []; success = None
                        empty_retry = max(1, int(await get_setting(db, 'webscout_empty_retry_minutes', str(WEBSCOUT_EMPTY_RETRY_MINUTES)) or WEBSCOUT_EMPTY_RETRY_MINUTES))
                        for src in ordered:
                            cursor = (cursor + 1) % len(due_rows) if due_rows else 0
                            try:
                                r = await fetch_source_cycle(db, src, ai)
                                results.append(r)
                                interval = r.get('interval_minutes') if r.get('accepted') else max(int(src.get('interval_minutes') or empty_retry), empty_retry)
                                await update_source_after_check(db, src, interval, None)
                                if r.get('accepted'): success = r; break
                            except Exception as exc:
                                results.append({'errors': 1, 'found': 0, 'candidates': 0, 'processed': 0, 'accepted': 0, 'queued': 0, 'rejected': 0, 'diagnostics': [str(exc)[:300]]})
                                await update_source_after_check(db, src, empty_retry, str(exc)[:500])
                                if ai.bot and ADMIN_ID and time.time() - LAST_SOURCE_ERROR_NOTICE > 1800:
                                    LAST_SOURCE_ERROR_NOTICE = time.time()
                                    try: await ai.bot.send_message(ADMIN_ID, f"❌ <b>WebScout source error</b>\n{html.escape(str(src.get('name') or src.get('url')))}\n<code>{html.escape(str(exc)[:800])}</code>", parse_mode='HTML')
                                    except Exception: pass
                        end_now = datetime.now(timezone.utc)   # FIX: زمان‌بندی از پایان چرخه
                        if success:
                            wait = max(1, int(success.get('interval_minutes') or await get_setting(db, 'webscout_success_interval_minutes', str(WEBSCOUT_SUCCESS_INTERVAL_MINUTES)) or WEBSCOUT_SUCCESS_INTERVAL_MINUTES))
                            await set_setting(db, 'webscout_next_run_at', (end_now + timedelta(minutes=wait)).isoformat())
                        else:
                            nexts = []
                            for s in rows:
                                try: nexts.append(datetime.fromisoformat((s.get('next_check_at') or '').replace('Z', '+00:00')))
                                except Exception: pass
                            nxt = min([end_now + timedelta(minutes=empty_retry)] + [n for n in nexts if n > end_now])
                            await set_setting(db, 'webscout_next_run_at', nxt.isoformat())
                        summary = {'sources_checked': len(results), 'processed': sum((r.get('processed', 0) if isinstance(r, dict) else 0) for r in results), 'accepted': sum((r.get('accepted', 0) if isinstance(r, dict) else 0) for r in results), 'rejected': sum((r.get('rejected', 0) if isinstance(r, dict) else 0) for r in results), 'errors': sum((r.get('errors', 0) if isinstance(r, dict) else 0) for r in results), 'queued': sum((r.get('queued', 0) if isinstance(r, dict) else 0) for r in results), 'published': False, 'mode': 'webscout'}
                        published = await publish_next_article(db, bot)
                        summary['published'] = bool(published); last_publish_try = time.time()
                        await set_setting(db, 'last_cycle_result', json.dumps(summary, ensure_ascii=False))
                        await set_setting(db, 'last_cycle_finished_at', datetime.now(timezone.utc).isoformat())
                    if time.time() - last_provider_recheck > AI_PROVIDER_RECHECK_MINUTES * 60:
                        await recheck_failed_providers(db, bot, ai); last_provider_recheck = time.time()
                    if time.time() - last_cleanup > AUTOMATION_CLEANUP_INTERVAL_SECONDS:
                        await cleanup_automation_data(db); last_cleanup = time.time()
            except asyncio.CancelledError: raise
            except Exception as e:
                logger.exception('automation loop error')
                await log_automation(db, 'ERROR', 'automation_loop_failed', str(e)[:1500])
                await set_setting(db, 'last_cycle_result', json.dumps({'error': str(e)[:1000]}, ensure_ascii=False))
            await asyncio.sleep(WEBSCOUT_LOOP_SLEEP_SECONDS)
    finally:
        await ai.close()

def format_duration_minutes(value) -> str:
    try: m = max(0, int(float(value)))
    except Exception: m = 0
    if m < 60: return f"{m} دقیقه"
    h = m // 60; rem = m % 60
    return f"{h} ساعت" if rem == 0 else f"{h} ساعت و {rem} دقیقه"

async def next_publication_estimate(db: D1Database) -> Dict[str, Any]:
    now = datetime.now(timezone.utc)
    last_manual = await get_setting(db, "last_manual_channel_post_at", "")
    last_pub = await db.execute("SELECT published_at FROM publication_queue WHERE status='published' AND published_at IS NOT NULL ORDER BY id DESC LIMIT 1")
    latest = None
    for raw in [x for x in [last_manual, last_pub[0].get("published_at") if last_pub else ""] if x]:
        try:
            dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
            if latest is None or dt > latest: latest = dt
        except Exception: pass
    interval_minutes = float(await get_setting(db, "min_post_gap_minutes", str(DEFAULT_MIN_POST_GAP_MINUTES)))
    target = max(now, latest + timedelta(minutes=interval_minutes) if latest else now)
    queued = await db.execute("SELECT COUNT(*) c FROM publication_queue WHERE status='queued'")
    return {"target": target, "minutes": max(0, int((target - now).total_seconds() / 60)) if target > now else 0, "latest": latest, "interval_minutes": int(interval_minutes), "queued": int(queued[0].get('c', 0)) if queued else 0}

async def get_schedule_panel(db: D1Database):
    channel_id = await get_channel_id(db); channel_username = await get_setting(db, "channel_username", "")
    shown = html.escape(channel_username) if channel_username else ("✅ کانال خصوصی تنظیم شده" if channel_id else "⛔ تنظیم نشده")
    enabled = (await get_setting(db, "automation_enabled", "0")) == "1"
    est = await next_publication_estimate(db)
    nxt = "آماده انتشار طبق برنامه" if est["minutes"] <= 0 else (f"حدود {est['minutes']} دقیقه دیگر" if est["minutes"] < 60 else f"حدود {est['minutes']//60} ساعت و {est['minutes']%60} دقیقه دیگر")
    text = ("📢 <b>انتشار و زمان‌بندی</b>\n"
            f"📢 کانال: <b>{shown}</b>\n🤖 اتوماسیون: <b>{'🟢 فعال' if enabled else '🔴 خاموش'}</b>\n"
            f"🔢 سقف روزانه: <b>{await get_setting(db,'max_daily_posts',str(DEFAULT_MAX_DAILY_POSTS))}</b> پست\n"
            f"⏱ فاصله انتشار: <b>{format_duration_minutes(await get_setting(db,'min_post_gap_minutes',str(DEFAULT_MIN_POST_GAP_MINUTES)))}</b>\n"
            f"🌐 فاصله بررسی منابع: <b>{await get_setting(db,'default_source_interval',str(DEFAULT_SOURCE_INTERVAL_MINUTES))} دقیقه</b>\n"
            f"🕐 نوبت تقریبی بعدی: <b>{nxt}</b>")
    return text, schedule_menu_kb()

async def automation_report(db: D1Database) -> str:
    # FIX: یک کوئری ترکیبی (سرعت) + کلیدهای درست گزارش + روز تهران
    day_start = datetime.now(pytz.timezone("Asia/Tehran")).replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc).isoformat()
    day_ago = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    agg = await db.execute("SELECT (SELECT COUNT(*) FROM sources WHERE enabled=1) src,(SELECT COUNT(*) FROM publication_queue WHERE status='queued') queued,(SELECT COUNT(*) FROM articles WHERE status='ready') ready,(SELECT COUNT(*) FROM articles WHERE status='published' AND COALESCE(published_at,created_at)>=?) pub,(SELECT COUNT(*) FROM articles WHERE created_at>=?) disc,(SELECT COUNT(*) FROM publication_queue WHERE status='failed' AND created_at>=?) fail", [day_start, day_ago, day_ago])
    a = agg[0] if agg else {}
    enabled = await get_setting(db, 'automation_enabled', '0')
    channel = await get_channel_id(db)
    channel_label = await get_setting(db, 'channel_username', '') or ('کانال خصوصی تنظیم شده' if channel else 'تنظیم نشده')
    hb = await get_setting(db, 'worker_heartbeat_at', '')
    hb_seconds = None
    if hb:
        try: hb_seconds = int((datetime.now(timezone.utc) - datetime.fromisoformat(hb.replace('Z', '+00:00'))).total_seconds())
        except Exception: hb_seconds = None
    hb_label = 'نامشخص'
    if hb_seconds is not None:
        hb_label = f'{hb_seconds} ثانیه قبل' + (' 🟢' if hb_seconds < 300 else ' 🟡' if hb_seconds < 900 else ' 🔴')
    result_raw = await get_setting(db, 'last_cycle_result', '')
    result_line = 'هنوز گزارشی ثبت نشده'
    if result_raw:
        try:
            o = json.loads(result_raw)
            result_line = (f"منابع: {o.get('sources_checked',0)} · پردازش: {o.get('processed',0)} · قبول: {o.get('accepted',0)} · صف: {o.get('queued',0)} · انتشار: {'بله ✅' if o.get('published') else 'خیر ⏸'}")
            if o.get('error'): result_line = f"خطا: {o.get('error')}"
        except Exception: result_line = 'آخرین نتیجه قابل نمایش نیست'
    return ("📊 <b>گزارش اتوماسیون</b>\n"
            f"{'🟢' if enabled=='1' else '🔴'} وضعیت: <b>{'فعال' if enabled=='1' else 'خاموش'}</b>\n"
            f"📢 کانال: <b>{html.escape(channel_label)}</b>\n🌐 منابع فعال: <b>{a.get('src',0)}</b>\n"
            f"📰 کشف ۲۴ ساعت: <b>{a.get('disc',0)}</b>\n📥 صف فعلی: <b>{a.get('queued',0)}</b>\n"
            f"📝 آماده در آرشیو: <b>{a.get('ready',0)}</b>\n📢 منتشرشده امروز: <b>{a.get('pub',0)}/{await get_setting(db,'max_daily_posts',str(DEFAULT_MAX_DAILY_POSTS))}</b>\n"
            f"❌ انتشار ناموفق ۲۴ ساعت: <b>{a.get('fail',0)}</b>\n⭐ حداقل امتیاز: <b>{await get_setting(db,'min_content_score',str(DEFAULT_MIN_CONTENT_SCORE))}</b>\n"
            f"⏱ فاصله بررسی منابع: <b>{await get_setting(db,'default_source_interval',str(DEFAULT_SOURCE_INTERVAL_MINUTES))} دقیقه</b>\n"
            f"📢 فاصله انتشار: <b>{format_duration_minutes(await get_setting(db,'min_post_gap_minutes',str(DEFAULT_MIN_POST_GAP_MINUTES)))}</b>\n"
            f"💓 Heartbeat: <b>{hb_label}</b>\n🕐 آخرین شروع چرخه: <b>{html.escape(await get_setting(db,'last_cycle_started_at','') or 'هنوز اجرا نشده')}</b>\n"
            f"✅ آخرین پایان چرخه: <b>{html.escape(await get_setting(db,'last_cycle_finished_at','') or 'هنوز اجرا نشده')}</b>\n"
            f"📋 آخرین نتیجه: <b>{html.escape(result_line)}</b>")

async def automation_overview(db: D1Database) -> str:
    enabled = (await get_setting(db, "automation_enabled", "0")) == "1"
    agg = await db.execute("SELECT (SELECT COUNT(*) FROM sources WHERE enabled=1) src,(SELECT COUNT(*) FROM publication_queue WHERE status='queued') queued")
    a = agg[0] if agg else {}
    return ("📰 <b>اتوماسیون محتوا</b>\n"
            f"🤖 وضعیت: <b>{'🟢 فعال' if enabled else '🔴 خاموش'}</b>\n🌐 منابع فعال: <b>{a.get('src',0)}</b>\n"
            f"📥 صف فعلی: <b>{a.get('queued',0)}</b>\n🔢 سقف روزانه: <b>{await get_setting(db,'max_daily_posts',str(DEFAULT_MAX_DAILY_POSTS))}</b>\n"
            f"⏱ فاصله انتشار: <b>{format_duration_minutes(await get_setting(db,'min_post_gap_minutes',str(DEFAULT_MIN_POST_GAP_MINUTES)))}</b>\n"
            f"🌐 فاصله بررسی منابع: <b>{await get_setting(db,'default_source_interval',str(DEFAULT_SOURCE_INTERVAL_MINUTES))} دقیقه</b>\n"
            "ℹ️ گزارش کامل فقط از دکمه «📊 گزارش» نمایش داده می‌شود.")

async def reset_database(db: D1Database):
    for t in ["users", "posts", "saves", "votes", "article_saves", "article_votes", "user_content_saves", "user_content_votes", "user_states", "processed_updates", "sources", "source_items", "articles", "publication_queue", "ai_providers", "automation_settings", "automation_logs", "manual_channel_events", "test_history"]:
        await db.execute(f"DROP TABLE IF EXISTS {t}")
    SETTINGS_CACHE.clear(); invalidate_sources(); invalidate_providers()
    await initialize_database(db)
    await migrate_unified_user_interactions(db)
    await initialize_automation_database(db)

class BotStates(StatesGroup):
    idle = State(); ai_chat = State(); user_chat_admin = State()
    waiting_post_content = State(); waiting_post_confirm = State()
    waiting_broadcast_content = State(); waiting_broadcast_confirm = State()
    admin_search_word = State(); admin_view_all = State(); admin_post_edit = State()
    user_search_folder = State(); admin_add_source = State()
    admin_add_provider = State(); admin_provider_token = State(); admin_provider_model = State()
    admin_channel_input = State(); admin_automation_setting = State(); automation_article_edit = State()

def get_tehran_date() -> str:
    return datetime.now(pytz.timezone("Asia/Tehran")).strftime("%Y-%m-%d")

async def download_telegram_file_text(bot: Bot, file_id: str) -> str:
    file_info = await bot.get_file(file_id)
    dest = io.BytesIO()
    await bot.download_file(file_info.file_path, destination=dest)
    dest.seek(0)
    text = dest.read().decode('utf-8', errors='ignore')
    if len(text) > 15000: text = text[:15000] + "\n[حجم فایل زیاد بود، بخشی بررسی شد]"
    return text

async def call_ai_with_history(url, api_key, model, messages):
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(url, headers=headers, json={"model": model, "messages": messages}) as resp:
                if resp.status != 200:
                    return {"content": f"❌ خطا در پاسخ هوش مصنوعی (کد {resp.status})", "tokens": 0}
                data = await resp.json()
                if "choices" in data and data["choices"]:
                    content = data["choices"][0]["message"]["content"]
                    return {"content": content, "tokens": data.get("usage", {}).get("total_tokens", math.ceil(len(content) / 4))}
                return {"content": f"⚠️ پاسخی دریافت نشد", "tokens": 0}
        except Exception as e:
            return {"content": f"❌ خطای ارتباط: {str(e)}", "tokens": 0}

FUNNY_MESSAGES = ["آروم‌تر قهرمان! 🏎️", "دکمه‌ها گناه دارن، یواش‌تر! 🥺", "اسپم نکن مشتی، یکم استراحت کن ☕", "سرعتت زیاده! یواش‌تر بران 🛑", "آروم‌تر بکوب رو دکمه‌ها دوست من! 🛠️"]
class RateLimitMiddleware(BaseMiddleware):
    def __init__(self, admin_id: int):
        super().__init__(); self.admin_id = admin_id; self.rate_limit_map = {}
    async def __call__(self, handler, event: TelegramObject, data: dict):
        user_id = event.from_user.id if getattr(event, "from_user", None) else None
        if user_id and user_id != self.admin_id:
            now = time.time()
            if now - self.rate_limit_map.get(user_id, 0.0) < 1.0:
                msg = random.choice(FUNNY_MESSAGES)
                if isinstance(event, Message): await event.answer(msg)
                elif isinstance(event, CallbackQuery): await event.answer(msg, show_alert=True)
                return
            self.rate_limit_map[user_id] = now
        return await handler(event, data)

router = Router()

async def register_user_if_not_exists(db: D1Database, user_id: int):
    await db.execute("INSERT OR IGNORE INTO users(id, joined_at) VALUES(?, ?)", [user_id, datetime.now(timezone.utc).isoformat()])

async def send_post_content(bot: Bot, chat_id: int, post: dict, reply_markup=None):
    text = post.get("text") or ""; file_id = post.get("file_id"); media_type = post.get("media_type")
    caption = text if len(text) <= 1024 else text[:1020] + "..."
    try:
        if media_type == "photo" and file_id: return await bot.send_photo(chat_id=chat_id, photo=file_id, caption=caption, reply_markup=reply_markup)
        elif media_type == "document" and file_id: return await bot.send_document(chat_id=chat_id, document=file_id, caption=caption, reply_markup=reply_markup)
        elif media_type == "video" and file_id: return await bot.send_video(chat_id=chat_id, video=file_id, caption=caption, reply_markup=reply_markup)
        elif media_type == "audio" and file_id: return await bot.send_audio(chat_id=chat_id, audio=file_id, caption=caption, reply_markup=reply_markup)
        else: return await bot.send_message(chat_id=chat_id, text=(text if len(text) <= 4096 else text[:4090] + "...") or "محتوای ارسالی", reply_markup=reply_markup)
    except Exception as e:
        logger.error("send post failed: %s", e); return None

async def send_article_content(bot: Bot, chat_id: int, article: dict, reply_markup=None):
    # FIX: ارسال چندبخشی با برش امن HTML
    title = html.escape(str(article.get('title') or 'مطلب'))
    body = sanitize_telegram_html(article.get('body') or '')
    body = remove_article_metadata_blocks(_remove_duplicate_title_from_body(article.get('title') or '', body))
    source_url = normalize_url(article.get('source_url') or '')
    if source_url and 'منبع اصلی' not in strip_html_text(body):
        body = f"{body.rstrip()}\n<a href=\"{html.escape(source_url, quote=True)}\">منبع اصلی</a>"
    relative = relative_time_label(article.get('source_published_at') or article.get('published_at') or '')
    if relative != 'زمان نامشخص': body = body.rstrip() + f"\n<i>⏱ {relative}</i>"
    chunks = split_html_safe(f"<b>📖 {title}</b>\n{body}", 3800) or [f"<b>📖 {title}</b>"]
    for i, ch in enumerate(chunks):
        kb = reply_markup if i == len(chunks) - 1 else None
        try: await bot.send_message(chat_id, ch, parse_mode='HTML', disable_web_page_preview=True, reply_markup=kb)
        except Exception: await bot.send_message(chat_id, strip_html_text(ch)[:3800], reply_markup=kb)

async def deliver_article_by_token(message: Message, bot: Bot, db: D1Database, token: str) -> bool:
    token = (token or '').strip()
    if token.startswith(('auto_', 'article_')): token = token.split('_', 1)[1]
    if not re.fullmatch(r'[A-Za-z0-9_-]{6,64}', token): return False
    rows = await db.execute("SELECT * FROM articles WHERE deep_token=? AND status IN ('ready','published','test')", [token])
    if not rows: return False
    article = rows[0]; article_id = int(article.get('id') or 0)
    try: await db.execute("UPDATE articles SET deep_views=COALESCE(deep_views,0)+1 WHERE id=?", [article_id])
    except Exception: pass
    like_rows = await db.execute("SELECT COUNT(*) c FROM user_content_votes WHERE content_type='article' AND content_id=? AND vote_type='like'", [article_id])
    dislike_rows = await db.execute("SELECT COUNT(*) c FROM user_content_votes WHERE content_type='article' AND content_id=? AND vote_type='dislike'", [article_id])
    save_rows = await db.execute("SELECT folder FROM user_content_saves WHERE user_id=? AND content_type='article' AND content_id=?", [message.from_user.id, article_id])
    kb = get_article_inline_kb(article_id, like_rows[0].get('c', 0) if like_rows else 0, dislike_rows[0].get('c', 0) if dislike_rows else 0, bool(save_rows))
    image = await resolve_article_image(db, article)
    if image:
        try:
            cap = f"<b>📖 {html.escape(str(article.get('title') or 'مطلب'))}</b>"
            rel = relative_time_label(article.get('source_published_at') or article.get('published_at') or '')
            if rel != 'زمان نامشخص': cap += f"\n<i>⏱ {rel}</i>"
            await bot.send_photo(message.chat.id, photo=image, caption=cap, parse_mode='HTML')
        except Exception: pass
    await send_article_content(bot, message.chat.id, article, kb)
    return True

@router.message(Command("help"))
async def cmd_help(message: Message):
    await message.answer("🌐 /help • راهنما\n📞 /man • تماس با مدیر\n🚀 /start • شروع", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 منوی اصلی", callback_data="user_home")]]))

@router.message(Command("man"))
async def cmd_man(message: Message, state: FSMContext):
    await state.set_state(BotStates.user_chat_admin)
    await message.answer("📞 پیام خود را برای مدیریت ارسال کن.", reply_markup=get_exit_menu())

@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext, db: D1Database, bot: Bot):
    user_id = message.from_user.id
    await register_user_if_not_exists(db, user_id)
    await state.set_state(BotStates.idle)
    state_data = await state.get_data()
    args = message.text.split()
    if len(args) > 1:
        deep_arg = args[1]
        if deep_arg.startswith(("auto_", "article_")):
            if not await deliver_article_by_token(message, bot, db, deep_arg):
                await message.answer("❌ این لینک ادامه مطلب معتبر نیست یا مقاله دیگر در دسترس نیست.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 منوی اصلی", callback_data="user_home")]]))
            return
        if deep_arg.isdigit():
            post_id = int(deep_arg)
            post_rows = await db.execute("SELECT text, file_id, media_type, likes, dislikes FROM posts WHERE id = ? AND deleted = 0", [post_id])
            if post_rows:
                post = post_rows[0]
                await db.execute("UPDATE posts SET views = views + 1 WHERE id = ?", [post_id])
                save_rows = await db.execute("SELECT folder FROM user_content_saves WHERE user_id = ? AND content_type='post' AND content_id = ?", [user_id, post_id])
                await send_post_content(bot, message.chat.id, post, get_post_inline_kb(post_id, post.get("likes", 0), post.get("dislikes", 0), len(save_rows) > 0))
            else:
                await message.answer("❌ این پست یافت نشد یا حذف شده است.")
            return
    first_name = message.from_user.first_name or "دوست عزیز"
    welcomes = [f"سلام {first_name} عزیز! 👋 خیلی خوش اومدی. وقت کاوش تو دنیای تکنولوژیه! 🚀", f"درود {first_name}! 🌟 خوشحالیم که اینجایی. آماده‌ای برای مطالب جذاب؟ 📚", f"سلام {first_name} جان! 🤖 به پایگاه دانش ما خوش اومدی. بزن بریم که کلی مطلب خفن داریم! 🔥"]
    admin_mode = state_data.get("admin_mode", "user")
    menu = get_admin_menu() if (user_id == ADMIN_ID and admin_mode != "user") else get_main_menu()
    await message.answer(random.choice(welcomes) + "\nاز دکمه های پایین استفاده کنید👇🏻", reply_markup=menu)

@router.message(Command("article"))
async def cmd_article(message: Message, db: D1Database, bot: Bot):
    parts = (message.text or '').split(maxsplit=1)
    if len(parts) < 2: await message.answer("فرمت: /article TOKEN"); return
    if not await deliver_article_by_token(message, bot, db, parts[1]):
        await message.answer("❌ مقاله پیدا نشد یا لینک منقضی شده است.")

@router.message(Command("setup_db"))
async def cmd_setup_db(message: Message, db: D1Database):
    if message.from_user.id == ADMIN_ID:
        try:
            await initialize_database(db); await migrate_unified_user_interactions(db); await initialize_automation_database(db)
            await message.answer("✅ Database setup completed successfully.")
        except Exception as e: await message.answer(f"❌ Error: {str(e)}")

@router.message(Command("reset_db"))
async def cmd_reset_db(message: Message, db: D1Database):
    if message.from_user.id == ADMIN_ID:
        try:
            await reset_database(db); await message.answer("✅ Database reset successfully!")
        except Exception as e: await message.answer(f"❌ Error: {str(e)}")

@router.message(F.text == "❌ خروج از نشست")
async def cmd_exit_session(message: Message, state: FSMContext):
    data = await state.get_data()
    admin_mode = data.get("admin_mode", "user")
    await state.set_state(BotStates.idle)
    await state.set_data({"admin_mode": admin_mode, "search_count": data.get("search_count", 0), "search_window_start": data.get("search_window_start", 0)})
    menu = get_admin_menu() if (message.from_user.id == ADMIN_ID and admin_mode != "user") else get_main_menu()
    await message.answer("🚪 خروج از نشست با موفقیت انجام شد!\n", reply_markup=menu)
    
    # ============================================================
# PART 4/4 — Keyboards + All Handlers + main()
# ============================================================
async def admin_ok(call) -> bool:
    if call.from_user.id != ADMIN_ID:
        try: await call.answer("دسترسی ندارید", show_alert=True)
        except Exception: pass
        return False
    return True

FOLDER_NAMES = {"cyber": "🔒 امنیت سایبری", "tech": "💻 تکنولوژی و فناوری", "ai": "🧠 هوش مصنوعی", "edu": "📚 آموزش"}

def get_main_menu(): return InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="💾 ذخیره‌های من", callback_data="user_saves"), InlineKeyboardButton(text="👤 پروفایل", callback_data="user_profile")],
    [InlineKeyboardButton(text="🤖 چت هوش مصنوعی", callback_data="ai_chat_start"), InlineKeyboardButton(text="❓ راهنما", callback_data="user_help")]])
def get_admin_menu(): return InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="📰 اتوماسیون محتوا", callback_data="admin_automation")],
    [InlineKeyboardButton(text="📁 مدیریت محتوای هسته", callback_data="admin_content")],
    [InlineKeyboardButton(text="📢 ارسال همگانی", callback_data="admin_broadcast"), InlineKeyboardButton(text="➕ افزودن پست", callback_data="admin_add_post")],
    [InlineKeyboardButton(text="📊 آمار کلی", callback_data="admin_stats")],
    [InlineKeyboardButton(text="👤 حالت کاربری", callback_data="admin_user_mode")]])
def get_admin_back_kb(t="admin_home"): return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 بازگشت", callback_data=t)]])
def get_exit_menu(): return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ لغو و بازگشت", callback_data="cancel_state")]])
def get_folder_selection_kb(): return InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text=FOLDER_NAMES["cyber"], callback_data="f_view_cyber"), InlineKeyboardButton(text=FOLDER_NAMES["tech"], callback_data="f_view_tech")],
    [InlineKeyboardButton(text=FOLDER_NAMES["ai"], callback_data="f_view_ai"), InlineKeyboardButton(text=FOLDER_NAMES["edu"], callback_data="f_view_edu")]])
def get_save_to_folder_kb(ctype, cid, back="user_saves"): return InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text=FOLDER_NAMES["cyber"], callback_data=f"usave_{ctype}_{cid}_cyber"), InlineKeyboardButton(text=FOLDER_NAMES["tech"], callback_data=f"usave_{ctype}_{cid}_tech")],
    [InlineKeyboardButton(text=FOLDER_NAMES["ai"], callback_data=f"usave_{ctype}_{cid}_ai"), InlineKeyboardButton(text=FOLDER_NAMES["edu"], callback_data=f"usave_{ctype}_{cid}_edu")],
    [InlineKeyboardButton(text="🔙 برگشت", callback_data=back)]])
def unified_saved_kb(folder="all"): return InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text=FOLDER_NAMES["tech"], callback_data="saved_folder_tech"), InlineKeyboardButton(text=FOLDER_NAMES["ai"], callback_data="saved_folder_ai")],
    [InlineKeyboardButton(text=FOLDER_NAMES["cyber"], callback_data="saved_folder_cyber"), InlineKeyboardButton(text=FOLDER_NAMES["edu"], callback_data="saved_folder_edu")],
    [InlineKeyboardButton(text="🗂 همه", callback_data="saved_folder_all")],
    [InlineKeyboardButton(text="🏠 منوی اصلی", callback_data="user_home")]])
def get_post_inline_kb(pid, likes, dislikes, saved): return InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text=f"👍 {likes}", callback_data=f"like_{pid}"), InlineKeyboardButton(text=f"👎 {dislikes}", callback_data=f"dis_{pid}")],
    [InlineKeyboardButton(text="❌ حذف از ذخیره‌ها" if saved else "💾 ذخیره", callback_data=f"unsave_{pid}" if saved else f"save_{pid}")],
    [InlineKeyboardButton(text="❓ راهنما", callback_data="user_help"), InlineKeyboardButton(text="🏠 منوی اصلی", callback_data="user_home")]])
def get_article_inline_kb(aid, likes, dislikes, saved): return InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text=f"👍 {likes}", callback_data=f"alike_{aid}"), InlineKeyboardButton(text=f"👎 {dislikes}", callback_data=f"adis_{aid}")],
    [InlineKeyboardButton(text="❌ حذف از ذخیره‌ها" if saved else "💾 ذخیره", callback_data=f"aunsave_{aid}" if saved else f"asave_{aid}")],
    [InlineKeyboardButton(text="🏠 منوی اصلی", callback_data="user_home")]])
def get_saved_folder_pagination_kb(pid, folder, index): return InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="🗑️ حذف", callback_data=f"ask_del_{pid}_{folder}")],
    [InlineKeyboardButton(text="⏮ قبلی", callback_data=f"fpg_prev_{folder}_{index}"), InlineKeyboardButton(text="⏭ بعدی", callback_data=f"fpg_next_{folder}_{index}")],
    [InlineKeyboardButton(text="🔍 جستجو", callback_data=f"f_srch_{folder}")]])
def get_saved_folder_search_pagination_kb(pid, folder, index): return InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="🗑️ حذف", callback_data=f"ask_del_{pid}_{folder}")],
    [InlineKeyboardButton(text="⏮ قبلی", callback_data=f"fspg_prev_{folder}_{index}"), InlineKeyboardButton(text="⏭ بعدی", callback_data=f"fspg_next_{folder}_{index}")],
    [InlineKeyboardButton(text="🔍 جستجوی مجدد", callback_data=f"f_srch_{folder}")]])
def get_confirm_delete_kb(pid, folder): return InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="🗑️ بله، حذفش کن", callback_data=f"f_del_save_{pid}_{folder}"), InlineKeyboardButton(text="🔙 نه", callback_data=f"cancel_delete_{folder}")]])
def get_confirm_add_post_kb(): return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ بله، ثبتش کن!", callback_data="conf_add_yes"), InlineKeyboardButton(text="❌ خیر", callback_data="conf_add_no")]])
def get_confirm_broadcast_kb(): return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🚀 ارسال همگانی", callback_data="conf_broad_yes"), InlineKeyboardButton(text="❌ لغو", callback_data="conf_broad_no")]])
def get_content_management_kb(): return InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="🔍 جستجو", callback_data="adm_search_text"), InlineKeyboardButton(text="📋 همه محتواها", callback_data="adm_view_all")],
    [InlineKeyboardButton(text="🔙 بازگشت", callback_data="admin_home")]])
def get_admin_search_pagination_kb(pid, index): return InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="⏮", callback_data=f"asearch_prev_{index}"), InlineKeyboardButton(text="⏭", callback_data=f"asearch_next_{index}")],
    [InlineKeyboardButton(text="✏️ ویرایش", callback_data=f"aedit_{pid}"), InlineKeyboardButton(text="📊 آمار", callback_data=f"astats_{pid}")],
    [InlineKeyboardButton(text="🗑 حذف", callback_data=f"adelete_{pid}"), InlineKeyboardButton(text="🔙", callback_data="admin_content")]])
def get_admin_view_all_confirm_kb(): return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ نمایش", callback_data="adm_view_all_confirm"), InlineKeyboardButton(text="❌ لغو", callback_data="adm_view_all_cancel")]])
def get_help_more_kb(): return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="💡 بیشتر توضیح بده", callback_data="help_more")]])
def get_help_got_it_kb(): return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🤓 متوجه شدم!", callback_data="help_got_it")]])
def automation_menu_kb(enabled): return InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="⏸ خاموش کردن" if enabled else "▶️ روشن کردن", callback_data="auto_off" if enabled else "auto_on")],
    [InlineKeyboardButton(text="🌐 منابع خبری", callback_data="auto_sources"), InlineKeyboardButton(text="🤖 مدل‌های AI", callback_data="auto_providers")],
    [InlineKeyboardButton(text="📢 انتشار و زمان‌بندی", callback_data="auto_channel"), InlineKeyboardButton(text="🧠 کیفیت محتوا", callback_data="auto_quality")],
    [InlineKeyboardButton(text="🗃 محتوا و داده‌ها", callback_data="auto_content_db")],
    [InlineKeyboardButton(text="🧪 تست و سلامت", callback_data="auto_health"), InlineKeyboardButton(text="📊 گزارش", callback_data="auto_report")],
    [InlineKeyboardButton(text="🔙 پنل اصلی", callback_data="admin_home")]])
def source_list_kb(sources):
    rows = [[InlineKeyboardButton(text="➕ افزودن منبع", callback_data="auto_add_source")]]
    for s in sources[:20]: rows.append([InlineKeyboardButton(text=f"{'🟢' if s.get('enabled') else '🔴'} {str(s.get('name'))[:35]}", callback_data=f"source_view_{s['id']}")])
    rows.append([InlineKeyboardButton(text="🔙", callback_data="auto_back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)
def provider_list_kb(providers):
    rows = [[InlineKeyboardButton(text="➕ افزودن مدل", callback_data="auto_add_provider")], [InlineKeyboardButton(text="ℹ️ راهنما", callback_data="provider_help")]]
    for p in providers[:20]:
        mark = {"healthy": "🟢", "invalid": "🔴", "cooldown": "🟡"}.get(p.get("status") or "", "⚪")
        rows.append([InlineKeyboardButton(text=f"{mark} #{p['id']} {'🌐' if p.get('web_enabled') else ''} {str(p.get('model_name'))[:30]}", callback_data=f"provider_view_{p['id']}")])
    rows.append([InlineKeyboardButton(text="🔙", callback_data="auto_back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)
def quality_menu_kb(): return InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="⭐ حداقل امتیاز", callback_data="set_min_score")],
    [InlineKeyboardButton(text="🎯 وزن معیارها", callback_data="quality_weights")],
    [InlineKeyboardButton(text="✍️ دستورهای تولید", callback_data="editorial_prompts")],
    [InlineKeyboardButton(text="🔙", callback_data="auto_back")]])
def schedule_menu_kb(): return InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="🔢 سقف روزانه", callback_data="set_max_daily"), InlineKeyboardButton(text="⏱ فاصله انتشار", callback_data="set_min_gap")],
    [InlineKeyboardButton(text="🌐 فاصله منابع", callback_data="set_default_interval"), InlineKeyboardButton(text="🧭 فاصله WebScout", callback_data="set_webscout_interval")],
    [InlineKeyboardButton(text="🚀 انتشار فوری", callback_data="publish_now"), InlineKeyboardButton(text="🧪 تست کانال", callback_data="channel_test")],
    [InlineKeyboardButton(text="🔙", callback_data="auto_back")]])
def automation_content_db_kb(): return InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="📥 صف انتشار", callback_data="auto_queue")],
    [InlineKeyboardButton(text="📰 محتوای تولیدشده", callback_data="auto_articles")],
    [InlineKeyboardButton(text="🗄 پاکسازی داده‌ها", callback_data="auto_db")],
    [InlineKeyboardButton(text="🔙", callback_data="auto_back")]])

# ---------- FSM message handlers ----------
@router.message(StateFilter(BotStates.ai_chat))
async def process_ai_chat(message, state, db, bot):
    uid = message.from_user.id
    if message.text and message.text.strip() in {"/exit", "❌ خروج", "خروج"}:
        await state.set_state(BotStates.idle); await message.answer("🚪 از چت AI خارج شدی.", reply_markup=get_main_menu()); return
    if not await db.execute("SELECT id FROM ai_providers WHERE enabled=1 AND status!='invalid' LIMIT 1"):
        await message.answer("⚠️ هیچ مدل فعالی تنظیم نشده است."); return
    today = get_tehran_date()
    ur = await db.execute("SELECT tokens_used,last_reset_date FROM users WHERE id=?", [uid])
    tokens = (ur[0].get("tokens_used") or 0) if ur else 0
    if (ur[0].get("last_reset_date") if ur else "") != today: tokens = 0
    if tokens >= 10000: await message.answer("⛔ سهمیه امروز تمام شد."); return
    prompt = message.text or ""
    if not prompt and message.document:
        await message.answer("⏳ در حال خواندن فایل...")
        try: prompt = (await download_telegram_file_text(bot, message.document.file_id))[:12000]
        except Exception as e: await message.answer(f"⚠️ {e}"); return
    if not prompt: await message.answer("⚠️ متن یا فایل متنی بفرست."); return
    sd = await state.get_data()
    history = sd.get("ai_history", [{"role": "system", "content": "You are a helpful assistant. Reply in Persian."}])
    history.append({"role": "user", "content": prompt})
    if len(history) > 11: history = [history[0]] + history[-10:]
    mgr = AIProviderManager(db, bot)
    try: res = await mgr.call(history, 0.25, 3000, "user_chat")
    finally: await mgr.close()
    if res.get("error") and not res.get("content"): await message.answer("⚠️ مدلی پاسخ نداد.\n" + html.escape(res["error"])[:1500]); return
    history.append({"role": "assistant", "content": res["content"]})
    await state.update_data(ai_history=history)
    for i in range(0, len(res["content"]), 3900): await message.answer(res["content"][i:i+3900])
    await db.execute("UPDATE users SET tokens_used=?, last_reset_date=? WHERE id=?", [tokens + int(res.get("tokens") or 0), today, uid])

@router.message(StateFilter(BotStates.user_chat_admin))
async def process_user_chat_admin(message, bot):
    uid = message.from_user.id
    if uid == ADMIN_ID or not ADMIN_ID: return
    tag = f"#User_{uid}"
    try:
        if message.photo: await bot.send_photo(ADMIN_ID, message.photo[-1].file_id, caption=f"پیام جدید:\n{tag}")
        elif message.document: await bot.send_document(ADMIN_ID, message.document.file_id, caption=f"فایل:\n{tag}")
        elif message.video: await bot.send_video(ADMIN_ID, message.video.file_id, caption=f"ویدیو:\n{tag}")
        elif message.text: await bot.send_message(ADMIN_ID, f"پیام جدید:\n{tag}\n{message.text}")
        await message.answer("✅ پیام شما برای مدیر ارسال شد.")
    except Exception: await message.answer("⚠️ ارسال ناموفق بود.")

@router.message(StateFilter(BotStates.waiting_post_content))
async def process_add_post_content(message, state, bot):
    if message.from_user.id != ADMIN_ID: return
    fid, mt = None, None; cap = message.text or message.caption or ""
    if message.photo: fid, mt = message.photo[-1].file_id, "photo"
    elif message.document: fid, mt = message.document.file_id, "document"
    elif message.video: fid, mt = message.video.file_id, "video"
    elif message.audio: fid, mt = message.audio.file_id, "audio"
    if not fid and not cap.strip(): await message.answer("❌ متن یا فایل معتبر بفرست."); return
    await state.update_data(temp_text=cap, temp_file_id=fid, temp_media_type=mt); await state.set_state(BotStates.waiting_post_confirm)
    await send_post_content(bot, message.chat.id, {"text": cap, "file_id": fid, "media_type": mt})
    await message.answer("آیا این محتوا ذخیره گردد؟", reply_markup=get_confirm_add_post_kb())

@router.message(StateFilter(BotStates.waiting_broadcast_content))
async def process_broadcast_content(message, state, bot):
    if message.from_user.id != ADMIN_ID: return
    fid, mt = None, None; cap = message.text or message.caption or ""
    if message.photo: fid, mt = message.photo[-1].file_id, "photo"
    elif message.document: fid, mt = message.document.file_id, "document"
    elif message.video: fid, mt = message.video.file_id, "video"
    elif message.audio: fid, mt = message.audio.file_id, "audio"
    if not fid and not cap.strip(): await message.answer("❌ متن یا فایل معتبر بفرست."); return
    await state.update_data(temp_text=cap + "\n#Broadcast", temp_file_id=fid, temp_media_type=mt); await state.set_state(BotStates.waiting_broadcast_confirm)
    await send_post_content(bot, message.chat.id, {"text": cap, "file_id": fid, "media_type": mt})
    await message.answer("ارسال همگانی انجام شود؟", reply_markup=get_confirm_broadcast_kb())

async def send_search_item(bot, chat_id, db, item, folder, index):
    kb = get_saved_folder_search_pagination_kb(item["id"], folder, index)
    if item.get("t") == "p":
        r = await db.execute("SELECT text,file_id,media_type FROM posts WHERE id=? AND deleted=0", [item["id"]])
        if r: await send_post_content(bot, chat_id, r[0], kb)
    else:
        r = await db.execute("SELECT * FROM articles WHERE id=? AND status IN ('ready','published','test')", [item["id"]])
        if r: await send_article_content(bot, chat_id, r[0], kb)

@router.message(StateFilter(BotStates.user_search_folder))
async def process_user_search_folder(message, state, db, bot):
    q = (message.text or "").strip()
    if not q: return
    sd = await state.get_data(); folder = sd.get("folder")
    if not folder: await state.set_state(BotStates.idle); return
    now = time.time() * 1000; W = 8 * 3600 * 1000
    cnt, ws = sd.get("search_count", 0), sd.get("search_window_start", 0)
    if now - ws > W: cnt, ws = 0, 0
    if cnt >= 5:
        await state.set_state(BotStates.idle); await message.answer("⏱️ سهمیه جستجوی این بازه تمام شده."); return
    if cnt == 0: ws = now
    await state.update_data(search_count=cnt + 1, search_window_start=ws)
    posts = await db.execute("SELECT p.id FROM user_content_saves s JOIN posts p ON p.id=s.content_id AND s.content_type='post' WHERE s.user_id=? AND s.folder=? AND p.text LIKE ? AND p.deleted=0 ORDER BY p.id DESC LIMIT 15", [message.from_user.id, folder, f"%{q}%"])
    arts = await db.execute("SELECT a.id FROM user_content_saves s JOIN articles a ON a.id=s.content_id AND s.content_type='article' WHERE s.user_id=? AND s.folder=? AND a.title LIKE ? AND a.status IN ('ready','published','test') ORDER BY a.id DESC LIMIT 15", [message.from_user.id, folder, f"%{q}%"])
    items = [{"t": "p", "id": r["id"]} for r in posts] + [{"t": "a", "id": r["id"]} for r in arts]
    if not items: await message.answer("❌ چیزی پیدا نکردم 🫠 کلمه دیگری بفرست:"); return
    await state.update_data(search_items=items, search_index=0)
    await message.answer(f"🎉 {len(items)} مطلب پیدا شد!")
    await send_search_item(bot, message.chat.id, db, items[0], folder, 0)

@router.message(F.chat.id == ADMIN_ID, F.reply_to_message, StateFilter(None, BotStates.idle))
async def process_admin_replies(message, bot):
    m = re.search(r"#User_(\d+)", message.reply_to_message.text or message.reply_to_message.caption or "")
    if m:
        t = int(m.group(1))
        try:
            if message.photo: await bot.send_photo(t, message.photo[-1].file_id, caption="پاسخ مدیریت:")
            elif message.text: await bot.send_message(t, "پاسخ مدیریت:\n" + message.text)
            await message.answer("✅ ارسال شد.")
        except Exception as e: await message.answer(f"❌ {e}")

COMMANDS_LIST = ["کاربر", "مدیریت", "💾 ذخیره‌های من", "❓ راهنما", "👤 پروفایل", "➕ افزودن پست", "📁 مدیریت محتوا", "📊 آمار", "📢 ارسال همگانی", "⚙️ اتوماسیون محتوا"]
@router.message(F.text.in_(COMMANDS_LIST), StateFilter(None, BotStates.idle))
async def intercept_global_commands(message, state, db):
    text, uid = message.text, message.from_user.id
    if text == "کاربر": await state.update_data(admin_mode="user"); await message.answer("✅ فاز کاربری.", reply_markup=get_main_menu())
    elif text == "مدیریت":
        if uid == ADMIN_ID: await state.update_data(admin_mode="admin"); await message.answer("✅ پنل مدیریت.", reply_markup=get_admin_menu())
        else: await message.answer("⛔ دسترسی ندارید.")
    elif text == "❓ راهنما": await message.answer("/help • راهنما\n/man • تماس\n/start • شروع", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙", callback_data="user_home")]]))
    elif text == "👤 پروفایل":
        saves = (await db.execute("SELECT COUNT(*) c FROM user_content_saves WHERE user_id=?", [uid]))[0].get("c", 0)
        likes = (await db.execute("SELECT COUNT(*) c FROM user_content_votes WHERE user_id=? AND vote_type='like'", [uid]))[0].get("c", 0)
        await message.answer(f"👤 <b>پروفایل</b>\n💾 ذخیره‌ها: <b>{saves}</b>\n👍 لایک‌ها: <b>{likes}</b>", parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙", callback_data="user_home")]]))
    elif text == "💾 ذخیره‌های من": await message.answer("💾 یک پوشه انتخاب کن:", reply_markup=unified_saved_kb("all"))
    elif text == "➕ افزودن پست" and uid == ADMIN_ID: await state.set_state(BotStates.waiting_post_content); await message.answer("📝 محتوا را بفرست:", reply_markup=get_exit_menu())
    elif text == "📁 مدیریت محتوا" and uid == ADMIN_ID: await message.answer("📂 انتخاب کنید:", reply_markup=get_content_management_kb())
    elif text == "📊 آمار" and uid == ADMIN_ID:
        a = (await db.execute("SELECT (SELECT COUNT(*) FROM users) u,(SELECT COUNT(*) FROM posts WHERE deleted=0) p,(SELECT COALESCE(SUM(views),0) FROM posts) v"))[0]
        await message.answer(f"📊 کاربران: {a.get('u',0)}\n📝 فعال: {a.get('p',0)}\n👁 بازدید: {a.get('v',0)}")
    elif text == "📢 ارسال همگانی" and uid == ADMIN_ID: await state.set_state(BotStates.waiting_broadcast_content); await message.answer("📢 پیام را بفرست:", reply_markup=get_exit_menu())
    elif text == "⚙️ اتوماسیون محتوا" and uid == ADMIN_ID:
        await message.answer(await automation_overview(db), parse_mode="HTML", reply_markup=automation_menu_kb((await get_setting(db, "automation_enabled", "0")) == "1"))

@router.message(StateFilter(None, BotStates.idle))
async def process_unknown(message, state):
    d = await state.get_data()
    await message.answer("❌ دستور نامعتبر.", reply_markup=get_admin_menu() if (message.from_user.id == ADMIN_ID and d.get("admin_mode") == "admin") else get_main_menu())

# ---------- Admin inputs ----------
@router.message(F.chat.id == ADMIN_ID, StateFilter(BotStates.admin_add_source))
async def admin_add_source_input(message, state, db, bot):
    url = (message.text or "").strip()
    if not re.match(r"^https?://", url, re.I): await message.answer("❌ URL معتبر نیست.", reply_markup=get_exit_menu()); return
    try:
        sid = await add_source(db, url)
        await state.set_state(BotStates.idle)
        await message.answer(f"✅ منبع اضافه شد. شناسه: {sid}", reply_markup=source_list_kb(await db.execute("SELECT * FROM sources ORDER BY priority ASC,id ASC")))
    except Exception as e: await message.answer(f"❌ {html.escape(str(e))}", reply_markup=get_exit_menu())

@router.message(F.chat.id == ADMIN_ID, StateFilter(BotStates.admin_automation_setting))
async def admin_automation_setting_input(message, state, db, bot):
    data = await state.get_data(); key = data.get("automation_setting_key"); value = (message.text or "").strip(); parent = data.get("parent_callback", "admin_home")
    try:
        if key == "__source_interval__":
            sid = int(data["source_interval_id"]); await db.execute("UPDATE sources SET interval_minutes=?,next_check_at=? WHERE id=?", [max(1, int(value)), datetime.now(timezone.utc).isoformat(), sid]); invalidate_sources(); parent = f"source_view_{sid}"
        elif key == "__source_priority__":
            sid = int(data["source_priority_id"]); await db.execute("UPDATE sources SET priority=? WHERE id=?", [max(1, int(value)), sid]); invalidate_sources(); parent = f"source_view_{sid}"
        elif key == "__provider_priority__":
            pid = int(data["provider_priority_id"]); await db.execute("UPDATE ai_providers SET priority=?,updated_at=? WHERE id=?", [max(1, int(value)), datetime.now(timezone.utc).isoformat(), pid]); invalidate_providers(); parent = f"provider_view_{pid}"
        elif key.startswith("weight_"): await set_setting(db, key, str(max(0, min(100, float(value))))); parent = "quality_weights"
        elif key in {"max_daily_posts", "default_source_interval", "webscout_success_interval_minutes", "webscout_empty_retry_minutes"}:
            await set_setting(db, key, str(max(1, int(value))))
            if key == "default_source_interval": await db.execute("UPDATE sources SET interval_minutes=?,next_check_at=? WHERE enabled=1", [int(value), datetime.now(timezone.utc).isoformat()]); invalidate_sources()
        elif key in {"max_workers", "max_ai_workers"}: await set_setting(db, key, str(max(1, min(4, int(value))))); parent = "auto_channel"
        elif key == "min_content_score": await set_setting(db, key, str(max(0, min(100, float(value)))))
        elif key in {"editorial_prompt_channel", "editorial_prompt_article"}:
            if not value: raise ValueError("خالی")
            await set_setting(db, key, value[:5000])
        elif key in {"min_hours_between_posts", "min_post_gap_minutes"}:
            await set_setting(db, "min_post_gap_minutes", str(max(1, int(float(value)))))
        elif key == "ai_verify_mode":
            if value not in {"auto", "always", "off"}: raise ValueError("auto/always/off")
            await set_setting(db, key, value)
        elif key == "__publish_delay__":
            row = await db.execute("SELECT q.article_id FROM publication_queue q JOIN articles a ON a.id=q.article_id WHERE q.status='queued' ORDER BY a.score DESC,q.created_at ASC LIMIT 1")
            if not row: raise ValueError("صف خالی است")
            await db.execute("UPDATE publication_queue SET scheduled_at=? WHERE article_id=? AND status='queued'", [(datetime.now(timezone.utc) + timedelta(minutes=max(0, min(10080, int(value))))).isoformat(), row[0]["article_id"]])
        else: raise ValueError("unsupported")
        await state.set_state(BotStates.idle)
        await message.answer("✅ ذخیره شد.", reply_markup=get_admin_back_kb(parent))
    except Exception as e:
        await message.answer(f"❌ مقدار نامعتبر: {html.escape(str(e))}", reply_markup=get_admin_back_kb(parent))

@router.message(F.chat.id == ADMIN_ID, StateFilter(BotStates.admin_channel_input))
async def admin_channel_input(message, state, db, bot):
    raw = (message.text or "").strip()
    if not raw: return
    if "t.me/" in raw: raw = "@" + urllib.parse.urlsplit(raw).path.strip("/").split("/")[-1]
    try:
        chat = await bot.get_chat(raw); me = await bot.get_me()
        member = await bot.get_chat_member(chat.id, me.id)
        if str(getattr(member, "status", "")) not in {"administrator", "creator"}:
            await message.answer("❌ ربات در کانال ادمین نیست.", reply_markup=get_exit_menu()); return
        await set_setting(db, "channel_id", str(chat.id)); await set_setting(db, "channel_username", "@" + chat.username if getattr(chat, "username", None) else "")
        await state.set_state(BotStates.idle)
        await message.answer(f"✅ کانال تنظیم شد: {html.escape('@' + chat.username if getattr(chat, 'username', None) else 'خصوصی')}", parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🧪 تست کانال", callback_data="channel_test"), InlineKeyboardButton(text="🔙", callback_data="auto_channel")]]))
    except Exception as e: await message.answer(f"❌ {html.escape(str(e)[:700])}", reply_markup=get_exit_menu())

# ---------- Core admin callbacks ----------
async def render_admin_home(call, db):
    await call.message.edit_text("🛠 <b>پنل مدیریت</b>\n<code>" + BUILD_VERSION + "</code>", parse_mode="HTML", reply_markup=get_admin_menu()); await call.answer()
@router.callback_query(F.data == "admin_home")
async def admin_home(call, db):
    if not await admin_ok(call): return
    await render_admin_home(call, db)
@router.callback_query(F.data == "admin_automation")
async def admin_automation(call, db):
    if not await admin_ok(call): return
    await call.answer(); await call.message.edit_text(await automation_overview(db), parse_mode="HTML", reply_markup=automation_menu_kb((await get_setting(db, "automation_enabled", "0")) == "1"))
@router.callback_query(F.data == "admin_stats")
async def admin_stats(call, db):
    if not await admin_ok(call): return
    a = (await db.execute("SELECT (SELECT COUNT(*) FROM users) u,(SELECT COUNT(*) FROM posts WHERE deleted=0) p,(SELECT COALESCE(SUM(views),0) FROM posts) v"))[0]
    await call.message.edit_text(f"📊 کاربران: {a.get('u',0)}\n📝 فعال: {a.get('p',0)}\n👁 بازدید: {a.get('v',0)}", reply_markup=get_admin_back_kb("admin_home")); await call.answer()
@router.callback_query(F.data == "admin_content")
async def admin_content(call, db):
    if not await admin_ok(call): return
    await call.message.edit_text("📁 <b>مدیریت محتوای هسته</b>", parse_mode="HTML", reply_markup=get_content_management_kb()); await call.answer()
@router.callback_query(F.data == "admin_add_post")
async def admin_add_post(call, state):
    if not await admin_ok(call): return
    await state.set_state(BotStates.waiting_post_content); await call.message.edit_text("📝 محتوا را بفرست:", reply_markup=get_exit_menu()); await call.answer()
@router.callback_query(F.data == "admin_broadcast")
async def admin_broadcast(call, state):
    if not await admin_ok(call): return
    await state.set_state(BotStates.waiting_broadcast_content); await call.message.edit_text("📢 پیام همگانی را بفرست:", reply_markup=get_exit_menu()); await call.answer()
@router.callback_query(F.data == "admin_user_mode")
async def admin_user_mode(call, state):
    await state.update_data(admin_mode="user"); await call.message.edit_text("👤 حالت کاربری.", reply_markup=get_main_menu()); await call.answer()

async def render_channel_panel(call, db):
    ch = await get_channel_id(db); chu = await get_setting(db, "channel_username", "")
    shown = html.escape(chu) if chu else ("✅ خصوصی" if ch else "⛔ تنظیم نشده")
    est = await next_publication_estimate(db)
    nxt = "آماده" if est["minutes"] <= 0 else (f"~{est['minutes']} دقیقه" if est["minutes"] < 60 else f"~{est['minutes']//60}h{est['minutes']%60}m")
    await call.message.edit_text(f"📢 <b>انتشار و زمان‌بندی</b>\n📢 کانال: <b>{shown}</b>\n🤖 اتوماسیون: <b>{'🟢' if await get_setting(db,'automation_enabled','0')=='1' else '🔴'}</b>\n🔢 سقف: <b>{await get_setting(db,'max_daily_posts',str(DEFAULT_MAX_DAILY_POSTS))}</b>\n⏱ فاصله: <b>{format_duration_minutes(await get_setting(db,'min_post_gap_minutes',str(DEFAULT_MIN_POST_GAP_MINUTES)))}</b>\n🕐 نوبت بعدی: <b>{nxt}</b>", parse_mode="HTML", reply_markup=schedule_menu_kb())
@router.callback_query(F.data == "auto_channel")
async def auto_channel(call, db):
    if not await admin_ok(call): return
    await call.answer(); await render_channel_panel(call, db)
@router.callback_query(F.data == "auto_channel_set")
async def auto_channel_set(call, state):
    if not await admin_ok(call): return
    await state.set_state(BotStates.admin_channel_input); await call.message.edit_text("📢 آیدی یا @username کانال را بفرست:", parse_mode="HTML", reply_markup=get_exit_menu()); await call.answer()
@router.callback_query(F.data == "publish_now")
async def publish_now(call, db, bot):
    if not await admin_ok(call): return
    await call.answer("🚀 انتشار…")
    try:
        ok = await publish_next_article(db, bot, force=True)
        msg = "✅ منتشر شد." if ok else "⏸ چیزی برای انتشار نیست یا سقف پر شده."
    except Exception as e: msg = "❌ " + html.escape(str(e)[:900])
    await call.message.edit_text(msg, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙", callback_data="auto_channel")]]))
@router.callback_query(F.data == "channel_test")
async def channel_test(call, db, bot):
    if not await admin_ok(call): return
    ch = await get_channel_id(db)
    if not ch: await call.answer("کانال تنظیم نشده", show_alert=True); return
    try:
        chat = await bot.get_chat(ch); me = await bot.get_me()
        member = await bot.get_chat_member(chat.id, me.id)
        if str(getattr(member, "status", "")) not in {"administrator", "creator"}: raise RuntimeError("ربات ادمین نیست")
        await call.message.edit_text(f"✅ کانال سالم: {html.escape('@' + chat.username if getattr(chat, 'username', None) else 'خصوصی')}", parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙", callback_data="auto_channel")]]))
    except Exception as e: await call.message.edit_text("❌ " + html.escape(str(e)[:800]), parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙", callback_data="auto_channel")]]))
    await call.answer()

@router.callback_query(F.data == "cancel_state")
async def cancel_state(call, state, db):
    data = await state.get_data(); parent = data.get("parent_callback") or "admin_home"
    await state.set_state(BotStates.idle)
    await state.update_data(panel_message_id=None, provider_base_url=None, provider_token=None, provider_edit_id=None)
    try:
        if parent == "auto_channel": await render_channel_panel(call, db); return
        if parent == "auto_quality": await auto_quality(call, db); return
        if parent == "quality_weights": await quality_weights(call, db); return
        if parent == "auto_providers" or parent.startswith("provider_view_"): await auto_providers(call, db); return
        if parent == "auto_sources" or parent.startswith("source_view_"): await auto_sources(call, db); return
        if parent == "editorial_prompts": await editorial_prompts_panel(call, db); return
    except Exception: pass
    if call.from_user.id == ADMIN_ID: await render_admin_home(call, db)
    else: await call.message.edit_text("لغو شد.", reply_markup=get_main_menu())
    await call.answer("لغو شد")

# ---------- User callbacks ----------
@router.callback_query(F.data == "user_home")
async def user_home(call): await call.message.edit_text("🏠 منوی اصلی", reply_markup=get_main_menu()); await call.answer()
@router.callback_query(F.data == "ai_chat_start")
async def ai_chat_start(call, state):
    await state.set_state(BotStates.ai_chat); await call.message.edit_text("🤖 پیام خود را بفرست. خروج: /exit", reply_markup=get_exit_menu()); await call.answer()
@router.callback_query(F.data == "user_saves")
async def user_saves(call): await call.message.edit_text("💾 یک پوشه انتخاب کن:", parse_mode="HTML", reply_markup=unified_saved_kb("all")); await call.answer()
async def _render_unified_saves(call, db, folder="all"):
    uid = call.from_user.id; fc, base = (" AND s.folder=?", [uid, folder]) if folder != "all" else ("", [uid])
    posts = await db.execute(f"SELECT p.id,p.text FROM user_content_saves s JOIN posts p ON p.id=s.content_id AND s.content_type='post' WHERE s.user_id=? AND p.deleted=0{fc} ORDER BY s.rowid DESC LIMIT 30", base)
    arts = await db.execute(f"SELECT a.id,a.title,a.deep_token FROM user_content_saves s JOIN articles a ON a.id=s.content_id AND s.content_type='article' WHERE s.user_id=? AND a.status IN ('ready','published','test'){fc} ORDER BY s.rowid DESC LIMIT 30", base)
    bn = BOT_USERNAME_RUNTIME or BOT_USERNAME.lstrip("@")
    items = [(r["id"], "post", strip_html_text(r.get("text") or "")[:80], f"https://t.me/{bn}?start={r['id']}") for r in posts] + [(r["id"], "article", strip_html_text(r.get("title") or "")[:90], f"https://t.me/{bn}?start=article_{r.get('deep_token','')}") for r in arts]
    items.sort(key=lambda x: x[0], reverse=True); items = items[:30]
    if not items: text = "💾 چیزی ذخیره نشده."
    else:
        lines = ["💾 <b>ذخیره‌های من</b>"]
        for i, (_, ct, t, u) in enumerate(items, 1): lines.append(f"{i}. {'📰' if ct=='article' else '📌'} <a href=\"{html.escape(u, quote=True)}\">{html.escape(t or 'بدون عنوان')}</a>")
        text = "\n".join(lines)
    await call.message.edit_text(text, parse_mode="HTML", disable_web_page_preview=True, reply_markup=unified_saved_kb(folder)); await call.answer()
@router.callback_query(F.data.startswith("saved_folder_"))
async def saved_folder(call, db): await _render_unified_saves(call, db, call.data.split("saved_folder_", 1)[1] or "all")
@router.callback_query(F.data == "user_profile")
async def user_profile(call, db):
    uid = call.from_user.id
    rows = await db.execute("SELECT joined_at,role FROM users WHERE id=?", [uid])
    days = 0
    if rows and rows[0].get("joined_at"):
        try: days = max(0, (datetime.now(timezone.utc) - datetime.fromisoformat(rows[0]["joined_at"].replace("Z", "+00:00"))).days)
        except Exception: pass
    saves = (await db.execute("SELECT COUNT(*) c FROM user_content_saves WHERE user_id=?", [uid]))[0].get("c", 0)
    likes = (await db.execute("SELECT COUNT(*) c FROM user_content_votes WHERE user_id=? AND vote_type='like'", [uid]))[0].get("c", 0)
    await call.message.edit_text(f"👤 <b>پروفایل</b>\n🗓 عضویت: {days} روز پیش\n💾 ذخیره‌ها: {saves}\n👍 لایک‌ها: {likes}", parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="💾 ذخیره‌ها", callback_data="user_saves"), InlineKeyboardButton(text="🔙", callback_data="user_home")]])); await call.answer()
@router.callback_query(F.data == "user_help")
async def user_help(call): await call.message.edit_text("❓ /start • شروع\n/help • راهنما\n/man • تماس", parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙", callback_data="user_home")]])); await call.answer()

# ---------- Automation callbacks ----------
@router.callback_query(F.data == "auto_on")
async def auto_on(call, db):
    if not await admin_ok(call): return
    await set_setting(db, "automation_enabled", "1"); await call.message.edit_text(await automation_overview(db), parse_mode="HTML", reply_markup=automation_menu_kb(True)); await call.answer("فعال شد")
@router.callback_query(F.data == "auto_off")
async def auto_off(call, db):
    if not await admin_ok(call): return
    await set_setting(db, "automation_enabled", "0"); await call.message.edit_text(await automation_overview(db), parse_mode="HTML", reply_markup=automation_menu_kb(False)); await call.answer("خاموش شد")
@router.callback_query(F.data == "auto_back")
async def auto_back(call, db):
    if not await admin_ok(call): return
    await call.message.edit_text(await automation_overview(db), parse_mode="HTML", reply_markup=automation_menu_kb((await get_setting(db, "automation_enabled", "0")) == "1")); await call.answer()
@router.callback_query(F.data == "auto_report")
async def auto_report(call, db):
    if not await admin_ok(call): return
    await call.message.edit_text(await automation_report(db), parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔄", callback_data="auto_report"), InlineKeyboardButton(text="🔙", callback_data="auto_back")]])); await call.answer()
@router.callback_query(F.data == "auto_sources")
async def auto_sources(call, db):
    if not await admin_ok(call): return
    rows = await db.execute("SELECT * FROM sources ORDER BY priority ASC,id ASC")
    await call.message.edit_text("🌐 منابع محتوا\n" + "".join(f"{'🟢' if s.get('enabled') else '🔴'} #{s['id']} {s.get('name')} | {s.get('interval_minutes')}m\n" for s in rows[:20]) or "🌐 منابع\nهنوز منبعی نیست.", reply_markup=source_list_kb(rows)); await call.answer()
@router.callback_query(F.data == "auto_add_source")
async def auto_add_source(call, state):
    if not await admin_ok(call): return
    await state.set_state(BotStates.admin_add_source); await call.message.edit_text("🌐 URL سایت را بفرست:", reply_markup=get_exit_menu()); await call.answer()
@router.callback_query(F.data.startswith("source_view_"))
async def source_view(call, db):
    if not await admin_ok(call): return
    sid = int(call.data.split("_")[-1]); rows = await db.execute("SELECT * FROM sources WHERE id=?", [sid])
    if not rows: await call.answer("یافت نشد", show_alert=True); return
    s = rows[0]
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 تست اکنون", callback_data=f"source_test_{sid}")],
        [InlineKeyboardButton(text="🔢 اولویت", callback_data=f"source_priority_{sid}"), InlineKeyboardButton(text="⏱ فاصله", callback_data=f"source_interval_{sid}")],
        [InlineKeyboardButton(text="⏸/▶️", callback_data=f"source_toggle_{sid}"), InlineKeyboardButton(text="🗑 حذف", callback_data=f"source_delete_{sid}")],
        [InlineKeyboardButton(text="🔙", callback_data="auto_sources")]])
    await call.message.edit_text(f"🌐 #{sid} {s.get('name')}\nURL: {s.get('url')}\nفاصله: {s.get('interval_minutes')}m\nاولویت: {s.get('priority')}\nآخرین بررسی: {s.get('last_checked_at') or '-'}\nخطا: {s.get('last_error') or '-'}", reply_markup=kb); await call.answer()
@router.callback_query(F.data.startswith("source_toggle_"))
async def source_toggle(call, db):
    if not await admin_ok(call): return
    sid = int(call.data.split("_")[-1]); r = await db.execute("SELECT enabled FROM sources WHERE id=?", [sid])
    if r: await db.execute("UPDATE sources SET enabled=? WHERE id=?", [0 if r[0].get("enabled") else 1, sid]); invalidate_sources()
    await source_view(call, db)
@router.callback_query(F.data.startswith("source_delete_"))
async def source_delete(call, db):
    if not await admin_ok(call): return
    sid = int(call.data.split("_")[-1])
    await call.message.edit_text("⚠️ حذف منبع؟", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🗑 بله", callback_data=f"source_delete_confirm_{sid}"), InlineKeyboardButton(text="↩️", callback_data=f"source_view_{sid}")]])); await call.answer()
@router.callback_query(F.data.startswith("source_delete_confirm_"))
async def source_delete_confirm(call, db):
    if not await admin_ok(call): return
    sid = int(call.data.split("_")[-1]); await db.execute("DELETE FROM sources WHERE id=?", [sid]); invalidate_sources()
    await call.message.edit_text("✅ حذف شد.", reply_markup=source_list_kb(await db.execute("SELECT * FROM sources ORDER BY priority ASC,id ASC"))); await call.answer("حذف شد")
@router.callback_query(F.data.startswith("source_priority_"))
async def source_priority(call, state):
    if not await admin_ok(call): return
    sid = int(call.data.split("_")[-1])
    await state.set_state(BotStates.admin_automation_setting); await state.update_data(automation_setting_key="__source_priority__", source_priority_id=sid, parent_callback=f"source_view_{sid}")
    await call.message.edit_text("🔢 اولویت:", reply_markup=get_exit_menu()); await call.answer()
@router.callback_query(F.data.startswith("source_interval_"))
async def source_interval(call, state):
    if not await admin_ok(call): return
    sid = int(call.data.split("_")[-1])
    await state.set_state(BotStates.admin_automation_setting); await state.update_data(automation_setting_key="__source_interval__", source_interval_id=sid, parent_callback=f"source_view_{sid}")
    await call.message.edit_text("⏱ فاصله به دقیقه:", reply_markup=get_exit_menu()); await call.answer()
@router.callback_query(F.data.startswith("source_test_"))
async def source_test(call, db, bot):
    if not await admin_ok(call): return
    sid = int(call.data.split("_")[-1]); rows = await db.execute("SELECT * FROM sources WHERE id=?", [sid])
    if not rows: await call.answer("یافت نشد", show_alert=True); return
    await call.answer("WebScout در حال بررسی…", show_alert=True)
    ai = AIProviderManager(db, bot)
    try:
        r = await ai.webscout_call(rows[0].get("url") or "", "Inspect TARGET URL; newest item in last 6h. FALSE or JSON(title,article_url,published_at,score,research_text).")
        raw = str(r.get("content") or "").strip()
        if not r.get("ok"): text = "❌ " + html.escape(str(r.get("error"))[:1800])
        elif raw.upper().startswith("FALSE"): text = "🟡 موردی پیدا نشد."
        else:
            o = parse_json_object(raw)
            text = f"🟢 {html.escape(str((o or {}).get('title') or 'مورد یافت شد'))}" if o else "⚠️ پاسخ نامعتبر"
        await call.message.edit_text(text, parse_mode="HTML", reply_markup=get_admin_back_kb(f"source_view_{sid}"))
    finally: await ai.close()

@router.callback_query(F.data == "auto_providers")
async def auto_providers(call, db):
    if not await admin_ok(call): return
    rows = await db.execute("SELECT * FROM ai_providers ORDER BY priority ASC,id ASC")
    await call.message.edit_text("🤖 <b>مدل‌های AI</b>", parse_mode="HTML", reply_markup=provider_list_kb(rows)); await call.answer()
@router.callback_query(F.data == "auto_add_provider")
async def auto_add_provider(call, state):
    if not await admin_ok(call): return
    await state.set_state(BotStates.admin_add_provider); await state.update_data(provider_edit_id=None)
    await call.message.edit_text("🔗 مرحله : Base URL را بفرست:", reply_markup=get_exit_menu()); await call.answer()
@router.message(F.chat.id == ADMIN_ID, StateFilter(BotStates.admin_add_provider))
async def provider_base_input(message, state):
    base = (message.text or "").strip()
    if not re.match(r"^https?://", base, re.I): await message.answer("❌ Base URL معتبر نیست.", reply_markup=get_exit_menu()); return
    await state.update_data(provider_base_url=base); await state.set_state(BotStates.admin_provider_token)
    await message.answer("🔐 مرحله ۲: توکن/API Key را بفرست:", reply_markup=get_exit_menu())
@router.message(F.chat.id == ADMIN_ID, StateFilter(BotStates.admin_provider_token))
async def provider_token_input(message, state):
    token = (message.text or "").strip()
    if len(token) < 4: await message.answer("❌ توکن کوتاه است.", reply_markup=get_exit_menu()); return
    await state.update_data(provider_token=token); await state.set_state(BotStates.admin_provider_model)
    try: await message.delete()   # FIX امنیت: حذف پیام توکن
    except Exception: pass
    await message.answer("🧩 مرحله ۳: نام دقیق Model را بفرست:", reply_markup=get_exit_menu())
@router.message(F.chat.id == ADMIN_ID, StateFilter(BotStates.admin_provider_model))
async def provider_model_input(message, state, db, bot):
    model = (message.text or "").strip(); data = await state.get_data(); base, token = data.get("provider_base_url", ""), data.get("provider_token", "")
    if not model: await message.answer("❌ مدل خالی است.", reply_markup=get_exit_menu()); return
    await message.answer("🧪 در حال تست مدل...")
    tester = AIProviderManager(db)
    try: result = await tester.test_provider_values(base, token, model)
    finally: await tester.close()
    await state.set_state(BotStates.idle); await state.update_data(provider_base_url=None, provider_token=None, provider_edit_id=None)
    if not result.get("ok"):
        await message.answer("❌ تست ناموفق:\n" + html.escape(str(result.get("error", ""))[:1000]), parse_mode="HTML", reply_markup=provider_list_kb(await db.execute("SELECT * FROM ai_providers ORDER BY priority ASC,id ASC"))); return
    now = datetime.now(timezone.utc).isoformat(); host = urllib.parse.urlsplit(base).netloc or "provider"; name = f"{model[:80]} | {host[:30]}"[:120]
    edit_id = data.get("provider_edit_id")
    if edit_id:
        await db.execute("UPDATE ai_providers SET name=?,base_url=?,encrypted_api_key=?,model_name=?,updated_at=?,status='healthy',last_error=NULL,cooldown_until=NULL,last_checked_at=?,last_latency_ms=? WHERE id=?", [name, base, encrypt_secret(token), model, now, now, result.get("latency_ms", 0), int(edit_id)])
    else:
        c = await db.execute("SELECT COALESCE(MAX(priority),0) p FROM ai_providers")
        await db.execute("INSERT INTO ai_providers(name,base_url,encrypted_api_key,model_name,priority,enabled,web_enabled,created_at,updated_at,status,last_checked_at,last_latency_ms) VALUES(?,?,?,{},?,1,0,?,?, 'healthy',?,?)".replace("{}", str((c[0].get("p") or 0) + 10)), [name, base, encrypt_secret(token), model, now, now, now, result.get("latency_ms", 0)])
    invalidate_providers()
    ws = result.get("webscout_ok")
    await message.answer(f"✅ مدل ذخیره شد.\n🤖 {html.escape(model)}\n⚡ {result.get('latency_ms',0)}ms\n🌐 WebScout: {'✅' if ws else '❌/نامشخص'}", parse_mode="HTML", reply_markup=provider_list_kb(await db.execute("SELECT * FROM ai_providers ORDER BY priority ASC,id ASC")))
@router.callback_query(F.data.startswith("provider_view_"))
async def provider_view(call, db):
    if not await admin_ok(call): return
    pid = int(call.data.split("_")[-1]); rows = await db.execute("SELECT * FROM ai_providers WHERE id=?", [pid])
    if not rows: await call.answer("یافت نشد", show_alert=True); return
    p = rows[0]
    st = {"healthy": "🟢 سالم", "invalid": "🔴 نامعتبر", "cooldown": "🟡 cooldown"}.get(p.get("status") or "", "⚪")
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ ویرایش", callback_data=f"provider_edit_{pid}"), InlineKeyboardButton(text="🧪 تست", callback_data=f"provider_test_{pid}")],
        [InlineKeyboardButton(text="🔢 اولویت", callback_data=f"provider_priority_{pid}"), InlineKeyboardButton(text="⏸/▶️", callback_data=f"provider_toggle_{pid}")],
        [InlineKeyboardButton(text="🌐 WebScout: " + ("روشن" if p.get("web_enabled") else "خاموش"), callback_data=f"provider_web_toggle_{pid}")],
        [InlineKeyboardButton(text="🗑 حذف", callback_data=f"provider_delete_{pid}")],
        [InlineKeyboardButton(text="🔙", callback_data="auto_providers")]])
    await call.message.edit_text(f"🤖 #{pid}\nModel: <code>{html.escape(str(p.get('model_name')))}</code>\nBase: <code>{html.escape(str(p.get('base_url')))}</code>\nوضعیت: {st}\nLatency: {p.get('last_latency_ms') or 0}ms\nخطا: {html.escape(str(p.get('last_error') or '-')[:400])}", parse_mode="HTML", reply_markup=kb); await call.answer()
@router.callback_query(F.data.startswith("provider_edit_"))
async def provider_edit(call, state, db):
    if not await admin_ok(call): return
    pid = int(call.data.split("_")[-1])
    await state.set_state(BotStates.admin_add_provider); await state.update_data(provider_edit_id=pid, provider_base_url=None, provider_token=None, parent_callback=f"provider_view_{pid}")
    await call.message.edit_text(f"✏️ ویرایش #{pid}\nمرحله ۱: Base URL جدید:", parse_mode="HTML", reply_markup=get_exit_menu()); await call.answer()
@router.callback_query(F.data.startswith("provider_test_"))
async def provider_test(call, db):
    if not await admin_ok(call): return
    pid = int(call.data.split("_")[-1]); await call.answer("🧪 تست…")
    m = AIProviderManager(db)
    try: r = await m.test_provider(pid)
    finally: await m.close()
    ws = r.get("webscout_ok")
    await call.message.edit_text(("✅ تست موفق · " + str(r.get("latency_ms", 0)) + "ms" + (f"\n🌐 WebScout: {'✅' if ws else '❌'}" if ws is not None else "")) if r.get("ok") else "❌ " + html.escape(str(r.get("error", ""))[:1000]), parse_mode="HTML", reply_markup=get_admin_back_kb(f"provider_view_{pid}"))
@router.callback_query(F.data.startswith("provider_priority_"))
async def provider_priority(call, state):
    if not await admin_ok(call): return
    pid = int(call.data.split("_")[-1])
    await state.set_state(BotStates.admin_automation_setting); await state.update_data(automation_setting_key="__provider_priority__", provider_priority_id=pid, parent_callback=f"provider_view_{pid}")
    await call.message.edit_text("🔢 اولویت:", reply_markup=get_exit_menu()); await call.answer()
@router.callback_query(F.data.startswith("provider_toggle_"))
async def provider_toggle(call, db):
    if not await admin_ok(call): return
    pid = int(call.data.split("_")[-1]); r = await db.execute("SELECT enabled,web_enabled FROM ai_providers WHERE id=?", [pid])
    if r:
        new = 0 if r[0].get("enabled") else 1
        if new == 0 and r[0].get("web_enabled"):
            a = await db.execute("SELECT COUNT(*) c FROM ai_providers WHERE enabled=1 AND web_enabled=1 AND id!=?", [pid])
            if not a or a[0].get("c", 0) <= 0: await call.answer("حداقل یک WebScout فعال لازم است.", show_alert=True); return
        await db.execute("UPDATE ai_providers SET enabled=?,updated_at=? WHERE id=?", [new, datetime.now(timezone.utc).isoformat(), pid]); invalidate_providers()
    await provider_view(call, db)
@router.callback_query(F.data.startswith("provider_web_toggle_"))
async def provider_web_toggle(call, db):
    if not await admin_ok(call): return
    pid = int(call.data.rsplit("_", 1)[-1]); r = await db.execute("SELECT web_enabled,base_url FROM ai_providers WHERE id=?", [pid])
    if not r: await call.answer("یافت نشد", show_alert=True); return
    new = 0 if int(r[0].get("web_enabled") or 0) else 1
    base = str(r[0].get("base_url") or "").lower()
    if new == 1 and not ("generativelanguage.googleapis.com" in base or "openrouter.ai" in base):
        await call.answer("این provider برای WebScout پشتیبانی نمی‌شود (فقط Gemini/OpenRouter).", show_alert=True); return
    if new == 0:
        a = await db.execute("SELECT COUNT(*) c FROM ai_providers WHERE enabled=1 AND web_enabled=1 AND id!=?", [pid])
        if not a or a[0].get("c", 0) <= 0: await call.answer("حداقل یک WebScout روشن لازم است.", show_alert=True); return
    await db.execute("UPDATE ai_providers SET web_enabled=?,updated_at=? WHERE id=?", [new, datetime.now(timezone.utc).isoformat(), pid]); invalidate_providers()
    await call.answer("🌐 WebScout " + ("روشن" if new else "خاموش")); await provider_view(call, db)
@router.callback_query(F.data.startswith("provider_delete_"))
async def provider_delete(call, db):
    if not await admin_ok(call): return
    pid = int(call.data.split("_")[-1])
    await call.message.edit_text("⚠️ حذف مدل؟", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🗑 بله", callback_data=f"provider_delete_confirm_{pid}"), InlineKeyboardButton(text="↩️", callback_data=f"provider_view_{pid}")]])); await call.answer()
@router.callback_query(F.data.startswith("provider_delete_confirm_"))
async def provider_delete_confirm(call, db):
    if not await admin_ok(call): return
    pid = int(call.data.split("_")[-1]); await db.execute("DELETE FROM ai_providers WHERE id=?", [pid]); invalidate_providers()
    await call.message.edit_text("🗑️ حذف شد.", parse_mode="HTML", reply_markup=provider_list_kb(await db.execute("SELECT * FROM ai_providers ORDER BY priority ASC,id ASC"))); await call.answer("حذف شد")
@router.callback_query(F.data == "provider_help")
async def provider_help(call):
    if not await admin_ok(call): return
    await call.message.edit_text("🤖 <b>راهنما</b>\nبرای هر مدل: Base URL + Token + نام دقیق مدل.\n🌐 WebScout فقط Gemini Native یا OpenRouter.\n🟡 429/quota → استراحت خودکار ۲ ساعته.\n🔴 401/403/404 → علامت نامعتبر.", parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙", callback_data="auto_providers")]])); await call.answer()

@router.callback_query(F.data == "auto_content_db")
async def auto_content_db(call, db):
    if not await admin_ok(call): return
    r = (await db.execute("SELECT (SELECT COUNT(*) FROM articles) a,(SELECT COUNT(*) FROM publication_queue WHERE status='queued') q,(SELECT COUNT(*) FROM test_history) t"))[0]
    await call.message.edit_text(f"🗃 <b>داده‌ها</b>\n📥 صف: {r.get('q',0)}\n📰 مقالات: {r.get('a',0)}\n🧪 تست‌ها: {r.get('t',0)}", parse_mode="HTML", reply_markup=automation_content_db_kb()); await call.answer()
@router.callback_query(F.data == "auto_db")
async def auto_db(call, db):
    if not await admin_ok(call): return
    await call.message.edit_text("🗄 <b>پاکسازی</b>\n⚠️ همه مقالات/صف/لاگ‌ها حذف می‌شوند؛ منابع و مدل‌ها می‌مانند.", parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🗑 تأیید", callback_data="auto_db_delete_confirm"), InlineKeyboardButton(text="🔙", callback_data="auto_content_db")]])); await call.answer()
@router.callback_query(F.data == "auto_db_delete_confirm")
async def auto_db_delete_confirm(call):
    if not await admin_ok(call): return
    await call.message.edit_text("⚠️ مطمئنی؟", parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⚠️ بله", callback_data="auto_db_delete_yes"), InlineKeyboardButton(text="↩️", callback_data="auto_db")]])); await call.answer()
@router.callback_query(F.data == "auto_db_delete_yes")
async def auto_db_delete_yes(call, db):
    if not await admin_ok(call): return
    for q in ["DELETE FROM publication_queue", "DELETE FROM articles", "DELETE FROM test_history", "DELETE FROM manual_channel_events", "DELETE FROM automation_logs"]: await db.execute(q)
    await db.execute("UPDATE sources SET last_checked_at=NULL,next_check_at=?", [datetime.now(timezone.utc).isoformat()]); invalidate_sources()
    await call.message.edit_text("✅ پاکسازی شد.", parse_mode="HTML", reply_markup=automation_content_db_kb()); await call.answer("پاک شد")

@router.callback_query(F.data == "auto_quality")
async def auto_quality(call, db):
    if not await admin_ok(call): return
    await call.message.edit_text(f"🧠 <b>کیفیت</b>\nحداقل امتیاز: <b>{html.escape(await get_setting(db,'min_content_score',str(DEFAULT_MIN_CONTENT_SCORE)))}</b>", parse_mode="HTML", reply_markup=quality_menu_kb()); await call.answer()
@router.callback_query(F.data == "editorial_prompts")
async def editorial_prompts_panel(call, db):
    if not await admin_ok(call): return
    ch = await get_setting(db, "editorial_prompt_channel", ""); ar = await get_setting(db, "editorial_prompt_article", "")
    await call.message.edit_text(f"✍️ <b>دستورها</b>\n📌 کوتاه: <code>{html.escape(ch[:220])}</code>\n📌 کامل: <code>{html.escape(ar[:220])}</code>", parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✍️ کوتاه", callback_data="set_editorial_prompt_channel"), InlineKeyboardButton(text="📝 کامل", callback_data="set_editorial_prompt_article")], [InlineKeyboardButton(text="♻️ پیش‌فرض", callback_data="editorial_prompts_reset")], [InlineKeyboardButton(text="🔙", callback_data="auto_quality")]])); await call.answer()
@router.callback_query(F.data == "editorial_prompts_reset")
async def editorial_prompts_reset(call, db):
    if not await admin_ok(call): return
    await set_setting(db, "editorial_prompt_channel", "فقط محتوای فنی و واقعاً ارزشمند برای مخاطب فناوری و هوش مصنوعی را پوشش بده.")
    await set_setting(db, "editorial_prompt_article", "نسخه کامل فنی، غنی و مبتنی بر واقعیت‌های منبع باشد.")
    await editorial_prompts_panel(call, db)
@router.callback_query(F.data == "set_editorial_prompt_channel")
async def set_editorial_prompt_channel(call, state, db):
    if not await admin_ok(call): return
    await prompt_for_setting(call, state, "editorial_prompt_channel", "✍️ پرامپت کوتاه جدید:\nفعلی:\n<code>" + html.escape((await get_setting(db, "editorial_prompt_channel", ""))[:1500]) + "</code>", "editorial_prompts")
@router.callback_query(F.data == "set_editorial_prompt_article")
async def set_editorial_prompt_article(call, state, db):
    if not await admin_ok(call): return
    await prompt_for_setting(call, state, "editorial_prompt_article", "📝 پرامپت کامل جدید:\nفعلی:\n<code>" + html.escape((await get_setting(db, "editorial_prompt_article", ""))[:1500]) + "</code>", "editorial_prompts")
@router.callback_query(F.data == "quality_weights")
async def quality_weights(call, db):
    if not await admin_ok(call): return
    items = [("global", "🌍 جهانی"), ("technology", "💻 فناوری"), ("ai", "🤖 AI"), ("cyber", "🔐 سایبری"), ("education", "📚 آموزش"), ("iran", "🇮 ایران"), ("freshness", "🆕 تازگی"), ("source", "✅ منبع"), ("novelty", "♻️ عدم تکرار")]
    text = "🎯 <b>وزن‌ها</b>\n" + "\n".join(f"{lab}: <b>{await get_setting(db, 'weight_' + k, '10')}</b>" for k, lab in items)
    rows = [[InlineKeyboardButton(text=lab, callback_data="weight_" + k)] for k, lab in items] + [[InlineKeyboardButton(text="🔙", callback_data="auto_quality")]]
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows)); await call.answer()
@router.callback_query(F.data.startswith("weight_"))
async def set_weight(call, state):
    if not await admin_ok(call): return
    await prompt_for_setting(call, state, call.data, "🎯 وزن (0-100):", "quality_weights")

async def prompt_for_setting(call, state, key, label, parent):
    await state.set_state(BotStates.admin_automation_setting)
    await state.update_data(automation_setting_key=key, parent_callback=parent)
    await call.message.edit_text(label, parse_mode="HTML", reply_markup=get_exit_menu()); await call.answer()
@router.callback_query(F.data == "set_max_daily")
async def set_max_daily(call, state, db):
    if not await admin_ok(call): return
    await prompt_for_setting(call, state, "max_daily_posts", "🔢 سقف روزانه:", "auto_channel")
@router.callback_query(F.data == "set_min_score")
async def set_min_score(call, state):
    if not await admin_ok(call): return
    await prompt_for_setting(call, state, "min_content_score", "⭐ حداقل امتیاز (0-100):", "auto_quality")
@router.callback_query(F.data == "set_min_gap")
async def set_min_gap(call, state, db):
    if not await admin_ok(call): return
    await prompt_for_setting(call, state, "min_post_gap_minutes", "⏱ فاصله انتشار به دقیقه:", "auto_channel")
@router.callback_query(F.data == "set_default_interval")
async def set_default_interval(call, state, db):
    if not await admin_ok(call): return
    await prompt_for_setting(call, state, "default_source_interval", "🌐 فاصله بررسی منابع به دقیقه:", "auto_channel")
@router.callback_query(F.data == "set_webscout_interval")
async def set_webscout_interval(call, state, db):
    if not await admin_ok(call): return
    await prompt_for_setting(call, state, "webscout_success_interval_minutes", "🧭 فاصله WebScout بعد از موفقیت به دقیقه:", "auto_channel")

# ---------- Health ----------
def health_progress_block(stage, total, title, detail=""):
    total = max(1, total); filled = max(0, min(total, stage)); bar = "█" * filled + "░" * (total - filled)
    return f"🧪 <b>{html.escape(title)}</b>\n<code>{bar}</code> {int(filled/total*100)}%\n{html.escape(detail)}"
async def edit_health_progress(message, text, reply_markup=None):
    try: await message.edit_text(text, parse_mode="HTML", reply_markup=reply_markup)
    except Exception: pass
def make_health_png(width=1280, height=720): return b""
async def choose_test_candidate(db, ai):
    for src in await db.execute("SELECT * FROM sources WHERE enabled=1 ORDER BY priority ASC LIMIT 5"):
        r = await ai.webscout_call(src.get("url") or "", "Inspect TARGET URL; newest substantive item in last 6h. FALSE or JSON(title,article_url,published_at,image_url,research_text,resource_links).")
        if not r.get("ok"): continue
        raw = str(r.get("content") or "").strip()
        if raw.upper().startswith("FALSE"): continue
        o = parse_json_object(raw)
        if o and o.get("research_text"):
            return src, {"title": strip_html_text(str(o.get("title") or ""))[:500], "url": normalize_url(str(o.get("article_url") or "")), "body": str(o.get("research_text")), "webscout_research": str(o.get("research_text")), "image_url": normalize_url(str(o.get("image_url") or "")), "published_at": str(o.get("published_at") or ""), "links": o.get("resource_links") or []}
    return None, None
@router.callback_query(F.data == "auto_health")
async def auto_health(call, db, bot):
    if not await admin_ok(call): return
    provs = await db.execute("SELECT status,enabled FROM ai_providers")
    healthy = sum(1 for p in provs if p.get("enabled") and p.get("status") == "healthy")
    src = (await db.execute("SELECT COUNT(*) c FROM sources WHERE enabled=1"))[0].get("c", 0)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🤖 تست مدل‌ها", callback_data="health_test_ai"), InlineKeyboardButton(text="🌐 تست منابع", callback_data="health_test_source")],
        [InlineKeyboardButton(text="🧪 تولید بدون انتشار", callback_data="health_dry_run")],
        [InlineKeyboardButton(text="▶️ چرخه واقعی", callback_data="health_run_cycle"), InlineKeyboardButton(text="📢 تست انتشار", callback_data="health_test_publish")],
        [InlineKeyboardButton(text="🚦 وضعیت", callback_data="health_deployment"), InlineKeyboardButton(text="📜 لاگ", callback_data="health_logs")],
        [InlineKeyboardButton(text="🔄", callback_data="auto_health"), InlineKeyboardButton(text="🔙", callback_data="auto_back")]])
    await call.message.edit_text(f"🧪 <b>سلامت</b>\nD1: {'✅' if db.session and not db.session.closed else '❌'}\nکانال: {'✅' if await get_channel_id(db) else '❌'}\nمدل سالم: {healthy}/{len(provs)}\nمنبع فعال: {src}", parse_mode="HTML", reply_markup=kb); await call.answer()
@router.callback_query(F.data == "health_test_ai")
async def health_test_ai(call, db):
    if not await admin_ok(call): return
    rows = await db.execute("SELECT id,model_name FROM ai_providers WHERE enabled=1 AND status!='invalid' ORDER BY priority ASC LIMIT 5")
    if not rows: await call.message.edit_text("❌ مدل فعالی نیست.", reply_markup=get_admin_back_kb("auto_health")); return
    await call.answer("تست شروع شد…")
    m = AIProviderManager(db); res = []
    try:
        for p in rows:
            r = await m.test_provider(int(p["id"]))
            res.append(f"{'✅' if r.get('ok') else '❌'} {html.escape(str(p.get('model_name')))}" + (f" · {r.get('latency_ms',0)}ms" if r.get("ok") else f"\n<code>{html.escape(str(r.get('error',''))[:400])}</code>"))
    finally: await m.close()
    await call.message.edit_text("🧪 <b>تست مدل‌ها</b>\n" + "\n".join(res), parse_mode="HTML", reply_markup=get_admin_back_kb("auto_health"))
@router.callback_query(F.data == "health_test_source")
async def health_test_source(call, db, bot):
    if not await admin_ok(call): return
    rows = await db.execute("SELECT * FROM sources WHERE enabled=1 ORDER BY priority ASC LIMIT 5")
    if not rows: await call.message.edit_text("❌ منبعی نیست.", reply_markup=get_admin_back_kb("auto_health")); return
    await call.answer("تست WebScout…")
    ai = AIProviderManager(db, bot); res = []
    try:
        for s in rows:
            r = await ai.webscout_call(s.get("url") or "", "Inspect TARGET URL; newest item in last 6h. FALSE or JSON(title,article_url,published_at,research_text).")
            raw = str(r.get("content") or "").strip()
            if not r.get("ok"): res.append(f"❌ {s.get('name')}: {html.escape(str(r.get('error'))[:250])}")
            elif raw.upper().startswith("FALSE"): res.append(f"🟡 {s.get('name')}: FALSE")
            else:
                o = parse_json_object(raw); res.append(f"🟢 {s.get('name')}: {html.escape(str((o or {}).get('title') or 'مورد یافت شد')[:100])}")
    finally: await ai.close()
    await call.message.edit_text("🌐 <b>تست WebScout</b>\n" + "\n".join(res), parse_mode="HTML", reply_markup=get_admin_back_kb("auto_health"))
@router.callback_query(F.data == "health_dry_run")
async def health_dry_run(call, db, bot):
    if not await admin_ok(call): return
    await call.answer("تست تولید…")
    ai = AIProviderManager(db, bot)
    try:
        src, item = await choose_test_candidate(db, ai)
        if not item: await call.message.edit_text("❌ گزینه تازه‌ای پیدا نشد.", reply_markup=get_admin_back_kb("auto_health")); return
        weights = {k: float(await get_setting(db, "weight_" + k, "10")) for k in ["global", "technology", "ai", "cyber", "education", "iran", "freshness", "source", "novelty"]}
        out = await ai_editorial_process(ai, item, src, [], weights, await get_manager_editorial_prompts(db))
        if out.get("error"): await call.message.edit_text("❌ " + html.escape(str(out["error"])[:1500]), parse_mode="HTML", reply_markup=get_admin_back_kb("auto_health")); return
        await call.message.edit_text(f"✅ <b>تست تولید موفق</b>\n📰 {html.escape(str(out.get('title')))}\n⭐ {out.get('score')}\n<b>کانال:</b>\n{out.get('channel_html','')[:1200]}\n<b>مقاله:</b>\n{out.get('article_html','')[:3000]}\n🚫 منتشر نشد.", parse_mode="HTML", reply_markup=get_admin_back_kb("auto_health"))
    except Exception as e: await call.message.edit_text("❌ " + html.escape(str(e)[:2000]), parse_mode="HTML", reply_markup=get_admin_back_kb("auto_health"))
    finally: await ai.close()
@router.callback_query(F.data == "health_run_cycle")
async def health_run_cycle(call, db, bot):
    if not await admin_ok(call): return
    await call.answer("چرخه واقعی…")
    ai = AIProviderManager(db, bot); results = []
    try:
        rows = (await get_enabled_sources(db, force=True))[:MAX_SOURCE_ITEMS_PER_CYCLE]
        for s in rows:
            try: results.append(await fetch_source_cycle(db, s, ai))
            except Exception as e: results.append({"errors": 1, "diagnostics": [str(e)[:200]]})
        published = await publish_next_article(db, bot)
        await call.message.edit_text(f"✅ <b>چرخه واقعی کامل شد</b>\nمنابع: {len(results)} · قبول: {sum(r.get('accepted',0) for r in results)} · رد: {sum(r.get('rejected',0) for r in results)} · خطا: {sum(r.get('errors',0) for r in results)} · انتشار: {'✅' if published else '⏸'}", parse_mode="HTML", reply_markup=get_admin_back_kb("auto_health"))
    finally: await ai.close()
@router.callback_query(F.data == "health_test_publish")
async def health_test_publish(call, db, bot):
    if not await admin_ok(call): return
    channel = await get_channel_id(db)
    if not channel: await call.message.edit_text("❌ کانال تنظیم نشده.", reply_markup=get_admin_back_kb("auto_health")); return
    await call.answer("تست انتشار…")
    ai = AIProviderManager(db, bot)
    try:
        src, item = await choose_test_candidate(db, ai)
        if not item: raise RuntimeError("گزینه‌ای پیدا نشد.")
        weights = {k: float(await get_setting(db, "weight_" + k, "10")) for k in ["global", "technology", "ai", "cyber", "education", "iran", "freshness", "source", "novelty"]}
        out = await ai_editorial_process(ai, item, src, [], weights, await get_manager_editorial_prompts(db))
        if out.get("error"): raise RuntimeError(out["error"])
        if not manager_accepts_score(float(out.get("score", 0) or 0), float(await get_setting(db, "min_content_score", str(DEFAULT_MIN_CONTENT_SCORE)))): raise RuntimeError(f"امتیاز {out.get('score','-')} زیر حد مدیر")
        now = datetime.now(timezone.utc).isoformat()
        ins = await db.execute("INSERT INTO articles(title,channel_text,body,source_url,image_url,category,score,status,created_at,source_published_at) VALUES(?,?,?,?,?,?,?,'test',?,?) RETURNING id", [out.get("title") or item.get("title"), out.get("channel_html") or "", out.get("article_html") or "", item.get("url") or "", item.get("image_url") or "", out.get("category") or "tech", float(out.get("score") or 0), now, item.get("published_at", "")[:100]])
        aid = int(ins[0]["id"]) if ins else 0
        await db.execute("UPDATE articles SET deep_token=? WHERE id=?", [make_deep_token(aid), aid])
        username = await get_runtime_bot_username(bot); deep = f"https://t.me/{username}?start=article_{make_deep_token(aid)}"
        cap = publication_caption(str(out.get("title") or "مطلب"), sanitize_telegram_html(out.get("channel_html") or ""), deep)
        sent = None
        if item.get("image_url"):
            try: sent = await bot.send_photo(channel, photo=item["image_url"], caption=cap, parse_mode="HTML")
            except Exception: sent = None
        if sent is None: sent = await bot.send_message(channel, text=cap, parse_mode="HTML", disable_web_page_preview=True)
        await db.execute("UPDATE articles SET published_message_id=?,published_at=? WHERE id=?", [getattr(sent, "message_id", 0), now, aid])
        await call.message.edit_text(f"✅ <b>تست انتشار موفق</b>\n📰 {html.escape(str(out.get('title')))}\n📢 ID: <code>{getattr(sent,'message_id',0)}</code>", parse_mode="HTML", reply_markup=get_admin_back_kb("auto_health"))
    except Exception as e: await call.message.edit_text("❌ " + html.escape(str(e)[:2000]), parse_mode="HTML", reply_markup=get_admin_back_kb("auto_health"))
    finally: await ai.close()
@router.callback_query(F.data == "health_deployment")
async def health_deployment(call, db):
    if not await admin_ok(call): return
    hb = await get_setting(db, "worker_heartbeat_at", "")
    age = None
    if hb:
        try: age = int((datetime.now(timezone.utc) - datetime.fromisoformat(hb.replace("Z", "+00:00"))).total_seconds())
        except Exception: pass
    await call.message.edit_text(f"🚦 Worker: {'🟢' if age is not None and age < 600 else '🔴'}\n💓 {age if age is not None else '-'}s\n🔄 آخرین چرخه: {html.escape(await get_setting(db,'last_cycle_finished_at','') or '-')}", parse_mode="HTML", reply_markup=get_admin_back_kb("auto_health")); await call.answer()
@router.callback_query(F.data == "health_logs")
async def health_logs(call, db):
    if not await admin_ok(call): return
    rows = await db.execute("SELECT level,event,details,created_at FROM automation_logs ORDER BY id DESC LIMIT 15")
    await call.message.edit_text("📜 <b>لاگ</b>\n" + "".join(f"<b>{html.escape(str(r.get('created_at') or ''))[11:19]} · {html.escape(str(r.get('event')))}</b>\n{html.escape(str(r.get('details') or '')[:300])}\n\n" for r in rows) or "📜 خالی.", parse_mode="HTML", reply_markup=get_admin_back_kb("auto_health")); await call.answer()

# ---------- Queue / articles ----------
@router.callback_query(F.data == "auto_publish_now")
async def auto_publish_now(call, db, bot):
    if not await admin_ok(call): return
    try:
        ok = await publish_next_article(db, bot, force=True)
        msg = "✅ منتشر شد." if ok else "⚠️ چیزی منتشر نشد."
    except Exception as e: msg = "❌ " + html.escape(str(e)[:900])
    await call.message.edit_text(msg, parse_mode="HTML", reply_markup=get_admin_back_kb("auto_queue")); await call.answer()
@router.callback_query(F.data == "auto_queue")
async def auto_queue(call, db):
    if not await admin_ok(call): return
    est = await next_publication_estimate(db)
    rows = await db.execute("SELECT q.article_id,a.title,a.score FROM publication_queue q JOIN articles a ON a.id=q.article_id WHERE q.status='queued' ORDER BY q.created_at ASC LIMIT 20")
    text = f"📥 <b>صف</b> · {est['queued']} آیتم\n🕐 نوبت بعدی: {'آماده' if est['minutes']<=0 else '~'+str(est['minutes'])+' دقیقه'}\n"
    kb = []
    for r in rows:
        text += f"#{r['article_id']} ⭐{float(r['score'] or 0):.0f} {str(r['title'])[:60]}\n"
        kb.append([InlineKeyboardButton(text=f"📄 #{r['article_id']}", callback_data=f"auto_art_{r['article_id']}")])
    kb += [[InlineKeyboardButton(text="🚀 انتشار فوری", callback_data="auto_publish_now"), InlineKeyboardButton(text="🔄", callback_data="auto_queue")], [InlineKeyboardButton(text="🔙", callback_data="auto_content_db")]]
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)); await call.answer()
@router.callback_query(F.data == "auto_articles")
async def auto_articles(call, db):
    if not await admin_ok(call): return
    rows = await db.execute("SELECT id,title,score,status FROM articles ORDER BY id DESC LIMIT 20")
    text = "📰 <b>محتوا</b>\n" + "".join(f"#{r['id']} {'✅' if r.get('status')=='published' else '📝'} {float(r['score'] or 0):.0f} {str(r['title'] or '')[:60]}\n" for r in rows) or "📰 خالی."
    kb = [[InlineKeyboardButton(text=f"📄 #{r['id']}", callback_data=f"auto_art_{r['id']}")] for r in rows] + [[InlineKeyboardButton(text="🔙", callback_data="auto_content_db")]]
    await call.message.edit_text(text[:4000], parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)); await call.answer()
async def render_automation_article(call, db, aid):
    rows = await db.execute("SELECT a.*,q.status qs FROM articles a LEFT JOIN publication_queue q ON q.article_id=a.id WHERE a.id=?", [aid])
    if not rows: await call.answer("یافت نشد", show_alert=True); return
    a = rows[0]
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👁 متن", callback_data=f"auto_art_view_{aid}")],
        [InlineKeyboardButton(text="✏️ عنوان", callback_data=f"auto_art_edit_title_{aid}"), InlineKeyboardButton(text="✏️ کانال", callback_data=f"auto_art_edit_channel_{aid}")],
        [InlineKeyboardButton(text="✏️ کامل", callback_data=f"auto_art_edit_body_{aid}"), InlineKeyboardButton(text="🗑", callback_data=f"auto_art_delete_{aid}")],
        [InlineKeyboardButton(text="🔙", callback_data="auto_queue" if a.get("qs") == "queued" else "auto_articles")]])
    await call.message.edit_text(f"📰 #{aid}\n<b>{html.escape(str(a.get('title') or ''))}</b>\n⭐ {float(a.get('score') or 0):.1f} · {a.get('status')} · صف: {a.get('qs') or '-'}\n👁 {int(a.get('deep_views') or 0)}", parse_mode="HTML", reply_markup=kb); await call.answer()
@router.callback_query(F.data.regexp(r"^auto_art_(\d+)$"))
async def auto_art_view_callback(call, db):
    if not await admin_ok(call): return
    await render_automation_article(call, db, int(call.data.split("_")[-1]))
@router.callback_query(F.data.regexp(r"^auto_art_view_(\d+)$"))
async def auto_art_view_text(call, db, bot):
    if not await admin_ok(call): return
    aid = int(call.data.split("_")[-1]); rows = await db.execute("SELECT title,channel_text,body FROM articles WHERE id=?", [aid])
    if not rows: await call.answer("یافت نشد", show_alert=True); return
    chunks = split_html_safe(f"📢 <b>کانال:</b>\n{sanitize_telegram_html(rows[0].get('channel_text') or '')}\n📖 <b>کامل:</b>\n{sanitize_telegram_html(rows[0].get('body') or '')}", 3800) or ["خالی"]
    await call.message.edit_text(chunks[0][:4000], parse_mode="HTML", reply_markup=get_admin_back_kb(f"auto_art_{aid}"))
    for c in chunks[1:]:
        try: await bot.send_message(call.message.chat.id, c[:4000], parse_mode="HTML")
        except Exception: pass
    await call.answer()
@router.callback_query(F.data.regexp(r"^auto_art_edit_(title|channel|body)_(\d+)$"))
async def auto_art_edit_start(call, state, db):
    if not await admin_ok(call): return
    parts = call.data.split("_"); field, aid = parts[2], int(parts[3])
    await state.set_state(BotStates.automation_article_edit); await state.update_data(article_edit_id=aid, article_edit_field=field)
    await call.message.edit_text("✏️ مقدار جدید را بفرست:", reply_markup=get_exit_menu()); await call.answer()
@router.message(F.chat.id == ADMIN_ID, StateFilter(BotStates.automation_article_edit))
async def auto_art_edit_input(message, state, db, bot):
    data = await state.get_data(); aid = int(data["article_edit_id"]); field = data["article_edit_field"]; value = (message.text or message.caption or "").strip()
    if not value: await message.answer("❌ خالی.", reply_markup=get_exit_menu()); return
    col = {"title": "title", "channel": "channel_text", "body": "body"}[field]
    if field == "title": value = strip_html_text(value)[:500]
    else: value = sanitize_telegram_html(value)[:18000]
    await db.execute(f"UPDATE articles SET {col}=? WHERE id=?", [value, aid])
    rows = await db.execute("SELECT published_message_id,status,deep_token,title,channel_text FROM articles WHERE id=?", [aid])
    if rows and rows[0].get("status") == "published" and rows[0].get("published_message_id") and field in {"title", "channel"}:
        try:
            username = await get_runtime_bot_username(bot); deep = f"https://t.me/{username}?start=auto_{rows[0].get('deep_token')}"
            cap = publication_caption(rows[0].get("title") or "", rows[0].get("channel_text") or "", deep)
            await bot.edit_message_caption(chat_id=await get_channel_id(db), message_id=int(rows[0]["published_message_id"]), caption=cap, parse_mode="HTML")
        except Exception: pass
    await state.set_state(BotStates.idle)
    await message.answer(f"✅ {field} محتوا #{aid} ویرایش شد.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📄 مدیریت محتوا", callback_data=f"auto_art_{aid}"), InlineKeyboardButton(text="🔙", callback_data="auto_content_db")]]))
@router.callback_query(F.data.regexp(r"^auto_art_delete_(\d+)$"))
async def auto_art_delete(call):
    if not await admin_ok(call): return
    aid = int(call.data.split("_")[-1])
    await call.message.edit_text("⚠️ حذف محتوا؟", parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🗑 بله", callback_data=f"auto_art_delete_yes_{aid}"), InlineKeyboardButton(text="↩️", callback_data=f"auto_art_{aid}")]])); await call.answer()
@router.callback_query(F.data.regexp(r"^auto_art_delete_yes_(\d+)$"))
async def auto_art_delete_yes(call, db):
    if not await admin_ok(call): return
    aid = int(call.data.split("_")[-1])
    await db.execute("DELETE FROM publication_queue WHERE article_id=?", [aid])
    await db.execute("DELETE FROM articles WHERE id=?", [aid])
    await db.execute("DELETE FROM user_content_saves WHERE content_type='article' AND content_id=?", [aid])
    await db.execute("DELETE FROM user_content_votes WHERE content_type='article' AND content_id=?", [aid])
    await call.message.edit_text("🗑️ حذف شد.", parse_mode="HTML", reply_markup=automation_content_db_kb()); await call.answer("حذف شد")

@router.channel_post()
async def on_channel_post(message, db):
    ch = await get_channel_id(db)
    if not ch: return
    match = (bool(message.chat.username) and ("@" + message.chat.username.lower()) == ch.lower()) if ch.startswith("@") else str(message.chat.id) == str(ch)
    if not match: return
    if await db.execute("SELECT id FROM articles WHERE published_message_id=?", [message.message_id]): return
    await set_setting(db, "last_manual_channel_post_at", datetime.now(timezone.utc).isoformat())

# ---------- Voting / saves / legacy folders ----------
@router.callback_query(F.data.startswith("alike_") | F.data.startswith("adis_"))
async def process_article_voting(call, db):
    parts = call.data.split("_"); vote = "like" if parts[0] == "alike" else "dislike"; aid = int(parts[1]); uid = call.from_user.id
    ex = await db.execute("SELECT vote_type FROM user_content_votes WHERE user_id=? AND content_type='article' AND content_id=?", [uid, aid])
    if ex and ex[0].get("vote_type") == vote: await db.execute("DELETE FROM user_content_votes WHERE user_id=? AND content_type='article' AND content_id=?", [uid, aid])
    elif ex: await db.execute("UPDATE user_content_votes SET vote_type=? WHERE user_id=? AND content_type='article' AND content_id=?", [vote, uid, aid])
    else: await db.execute("INSERT INTO user_content_votes(user_id,content_type,content_id,vote_type,created_at) VALUES(?,?,?,?,?)", [uid, "article", aid, vote, datetime.now(timezone.utc).isoformat()])
    l = (await db.execute("SELECT COUNT(*) c FROM user_content_votes WHERE content_type='article' AND content_id=? AND vote_type='like'", [aid]))[0].get("c", 0)
    d = (await db.execute("SELECT COUNT(*) c FROM user_content_votes WHERE content_type='article' AND content_id=? AND vote_type='dislike'", [aid]))[0].get("c", 0)
    s = bool(await db.execute("SELECT 1 FROM user_content_saves WHERE user_id=? AND content_type='article' AND content_id=?", [uid, aid]))
    await call.message.edit_reply_markup(reply_markup=get_article_inline_kb(aid, l, d, s)); await call.answer("✅")
@router.callback_query(F.data.startswith("asave_"))
async def process_article_save(call):
    aid = int(call.data.split("_")[1]); await call.answer(); await call.message.edit_reply_markup(reply_markup=get_save_to_folder_kb("article", aid, f"article_actions_{aid}"))
@router.callback_query(F.data.startswith("aunsave_"))
async def process_article_unsave(call, db):
    aid = int(call.data.split("_")[1])
    await db.execute("DELETE FROM user_content_saves WHERE user_id=? AND content_type='article' AND content_id=?", [call.from_user.id, aid])
    l = (await db.execute("SELECT COUNT(*) c FROM user_content_votes WHERE content_type='article' AND content_id=? AND vote_type='like'", [aid]))[0].get("c", 0)
    d = (await db.execute("SELECT COUNT(*) c FROM user_content_votes WHERE content_type='article' AND content_id=? AND vote_type='dislike'", [aid]))[0].get("c", 0)
    await call.message.edit_reply_markup(reply_markup=get_article_inline_kb(aid, l, d, False)); await call.answer("🗑️ حذف شد")
@router.callback_query(F.data.startswith("usave_"))
async def process_unified_save(call, db):
    parts = call.data.split("_")
    if len(parts) != 4: await call.answer("❌", show_alert=True); return
    _, ct, cid, folder = parts; cid = int(cid)
    await db.execute("INSERT OR REPLACE INTO user_content_saves(user_id,content_type,content_id,folder,created_at) VALUES(?,?,?,?,?)", [call.from_user.id, ct, cid, folder, datetime.now(timezone.utc).isoformat()])
    if ct == "article":
        l = (await db.execute("SELECT COUNT(*) c FROM user_content_votes WHERE content_type='article' AND content_id=? AND vote_type='like'", [cid]))[0].get("c", 0)
        d = (await db.execute("SELECT COUNT(*) c FROM user_content_votes WHERE content_type='article' AND content_id=? AND vote_type='dislike'", [cid]))[0].get("c", 0)
        await call.message.edit_reply_markup(reply_markup=get_article_inline_kb(cid, l, d, True))
    else:
        r = await db.execute("SELECT likes,dislikes FROM posts WHERE id=?", [cid])
        await call.message.edit_reply_markup(reply_markup=get_post_inline_kb(cid, r[0].get("likes", 0) if r else 0, r[0].get("dislikes", 0) if r else 0, True))
    await call.answer(f"✅ در {FOLDER_NAMES.get(folder, folder)} ذخیره شد")
@router.callback_query(F.data.startswith("article_actions_"))
async def article_actions(call, db):
    aid = int(call.data.split("_")[2])
    l = (await db.execute("SELECT COUNT(*) c FROM user_content_votes WHERE content_type='article' AND content_id=? AND vote_type='like'", [aid]))[0].get("c", 0)
    d = (await db.execute("SELECT COUNT(*) c FROM user_content_votes WHERE content_type='article' AND content_id=? AND vote_type='dislike'", [aid]))[0].get("c", 0)
    s = bool(await db.execute("SELECT 1 FROM user_content_saves WHERE user_id=? AND content_type='article' AND content_id=?", [call.from_user.id, aid]))
    await call.message.edit_reply_markup(reply_markup=get_article_inline_kb(aid, l, d, s)); await call.answer()
@router.callback_query(F.data.startswith("like_") | F.data.startswith("dis_"))
async def process_post_voting(call, db):
    parts = call.data.split("_"); vote = "like" if parts[0] == "like" else "dislike"; pid = int(parts[1]); uid = call.from_user.id
    ex = await db.execute("SELECT vote_type FROM user_content_votes WHERE user_id=? AND content_type='post' AND content_id=?", [uid, pid])
    if not ex:
        await db.execute("INSERT INTO user_content_votes(user_id,content_type,content_id,vote_type,created_at) VALUES(?,?,?,?,?)", [uid, "post", pid, vote, datetime.now(timezone.utc).isoformat()])
        await db.execute(f"UPDATE posts SET {vote}s={vote}s+1 WHERE id=?", [pid])
    elif ex[0].get("vote_type") == vote:
        await db.execute("DELETE FROM user_content_votes WHERE user_id=? AND content_type='post' AND content_id=?", [uid, pid])
        await db.execute(f"UPDATE posts SET {vote}s={vote}s-1 WHERE id=?", [pid])
    else:
        old = ex[0].get("vote_type")
        await db.execute("UPDATE user_content_votes SET vote_type=? WHERE user_id=? AND content_type='post' AND content_id=?", [vote, uid, pid])
        await db.execute(f"UPDATE posts SET {vote}s={vote}s+1, {old}s={old}s-1 WHERE id=?", [pid])
    p = (await db.execute("SELECT likes,dislikes FROM posts WHERE id=?", [pid]))[0]
    s = bool(await db.execute("SELECT 1 FROM user_content_saves WHERE user_id=? AND content_type='post' AND content_id=?", [uid, pid]))
    await call.message.edit_reply_markup(reply_markup=get_post_inline_kb(pid, p.get("likes", 0), p.get("dislikes", 0), s)); await call.answer("✅")
@router.callback_query(F.data.startswith("save_"))
async def process_save(call):
    pid = int(call.data.split("_")[1]); await call.answer(); await call.message.edit_reply_markup(reply_markup=get_save_to_folder_kb("post", pid, f"post_actions_{pid}"))
@router.callback_query(F.data.startswith("unsave_"))
async def process_unsave(call, db):
    pid = int(call.data.split("_")[1])
    await db.execute("DELETE FROM user_content_saves WHERE user_id=? AND content_type='post' AND content_id=?", [call.from_user.id, pid])
    p = (await db.execute("SELECT likes,dislikes FROM posts WHERE id=?", [pid]))[0]
    await call.message.edit_reply_markup(reply_markup=get_post_inline_kb(pid, p.get("likes", 0), p.get("dislikes", 0), False)); await call.answer("🗑️ حذف شد")
@router.callback_query(F.data.startswith("f_view_"))
async def process_view_saved_folder(call, db): await _render_unified_saves(call, db, call.data.split("_")[2])
@router.callback_query(F.data.startswith("srchpg_"))
async def search_pagination(call, state, db, bot):
    parts = call.data.split("_"); direction, folder, idx = parts[1], parts[2], int(parts[3])
    items = (await state.get_data()).get("search_items", [])
    if not items: await call.answer("نتیجه‌ای نیست", show_alert=True); return
    ni = max(0, min(idx + (1 if direction == "next" else -1), len(items) - 1))
    if ni == idx: await call.answer("🚧 انتهای نتایج"); return
    await state.update_data(search_index=ni); await call.answer()
    await send_search_item(bot, call.message.chat.id, db, items[ni], folder, ni)

# ---------- Admin content mgmt ----------
async def send_admin_all_posts_page(bot, chat_id, rows, page, pages, total, edit_id=None):
    text = f"📋 <b>همه محتواها</b> · {page+1}/{pages}\n"
    btns = []
    for p in rows:
        text += f"\n<b>#{p['id']}</b> {html.escape(strip_html_text(p.get('text') or '')[:50])}"
        btns.append([InlineKeyboardButton(text=f"✏️ #{p['id']}", callback_data=f"aedit_{p['id']}"), InlineKeyboardButton(text=f"🗑 #{p['id']}", callback_data=f"adelete_{p['id']}")])
    btns += [[InlineKeyboardButton(text="⏮", callback_data=f"adm_all_page_prev_{page}"), InlineKeyboardButton(text="⏭", callback_data=f"adm_all_page_next_{page}")], [InlineKeyboardButton(text="🔙", callback_data="admin_content")]]
    kb = InlineKeyboardMarkup(inline_keyboard=btns)
    if edit_id:
        try: await bot.edit_message_text(chat_id=chat_id, message_id=edit_id, text=text, parse_mode="HTML", reply_markup=kb); return
        except Exception: pass
    await bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=kb)
@router.callback_query(F.data == "adm_view_all")
async def adm_view_all(call):
    if not await admin_ok(call): return
    await call.message.edit_text("📋 نمایش همه؟", parse_mode="HTML", reply_markup=get_admin_view_all_confirm_kb()); await call.answer()
@router.callback_query(F.data == "adm_view_all_cancel")
async def adm_view_all_cancel(call):
    if not await admin_ok(call): return
    await call.message.edit_text("📁 مدیریت محتوا", parse_mode="HTML", reply_markup=get_content_management_kb()); await call.answer()
@router.callback_query(F.data == "adm_view_all_confirm")
async def adm_view_all_confirm(call, state, db, bot):
    if not await admin_ok(call): return
    total = (await db.execute("SELECT COUNT(*) c FROM posts WHERE deleted=0"))[0].get("c", 0)
    pages = max(1, math.ceil(total / 10))
    rows = await db.execute("SELECT id,text FROM posts WHERE deleted=0 ORDER BY id DESC LIMIT 10 OFFSET 0")
    await state.update_data(all_pages=pages)
    await send_admin_all_posts_page(bot, call.message.chat.id, rows, 0, pages, total, call.message.message_id); await call.answer()
@router.callback_query(F.data.startswith("adm_all_page_"))
async def adm_all_page(call, state, db, bot):
    if not await admin_ok(call): return
    parts = call.data.split("_"); d, cur = parts[3], int(parts[4])
    pages = int((await state.get_data()).get("all_pages", 1))
    new = max(0, min(cur + (1 if d == "next" else -1), pages - 1))
    rows = await db.execute("SELECT id,text FROM posts WHERE deleted=0 ORDER BY id DESC LIMIT 10 OFFSET ?", [new * 10])
    await send_admin_all_posts_page(bot, call.message.chat.id, rows, new, pages, 0, call.message.message_id); await call.answer()
@router.callback_query(F.data == "adm_search_text")
async def adm_search_text(call, state):
    if not await admin_ok(call): return
    await state.set_state(BotStates.admin_search_word); await call.message.edit_text("🔍 کلمه یا شماره پست:", parse_mode="HTML", reply_markup=get_exit_menu()); await call.answer()
@router.message(F.chat.id == ADMIN_ID, StateFilter(BotStates.admin_search_word))
async def process_admin_search_word(message, state, db, bot):
    q = (message.text or "").strip()
    if not q: return
    rows = await db.execute("SELECT id FROM posts WHERE id=? AND deleted=0" if q.isdigit() else "SELECT id FROM posts WHERE text LIKE ? AND deleted=0 ORDER BY id DESC LIMIT 50", [int(q)] if q.isdigit() else [f"%{q}%"])
    ids = [r["id"] for r in rows]
    if not ids: await message.answer("❌ پیدا نشد.", reply_markup=get_content_management_kb()); return
    await state.update_data(search_ids=ids, search_index=0)
    p = await db.execute("SELECT id,text,file_id,media_type FROM posts WHERE id=?", [ids[0]])
    if p: await send_post_content(bot, message.chat.id, p[0], get_admin_search_pagination_kb(ids[0], 0))
@router.callback_query(F.data.startswith("asearch_"))
async def asearch(call, state, db, bot):
    if not await admin_ok(call): return
    parts = call.data.split("_"); d, cur = parts[1], int(parts[2])
    ids = (await state.get_data()).get("search_ids", [])
    if not ids: await call.answer("خالی", show_alert=True); return
    ni = max(0, min(cur + (1 if d == "next" else -1), len(ids) - 1))
    p = await db.execute("SELECT id,text,file_id,media_type FROM posts WHERE id=?", [ids[ni]])
    if p: await send_post_content(bot, call.message.chat.id, p[0], get_admin_search_pagination_kb(ids[ni], ni))
    await call.answer()
@router.callback_query(F.data.startswith("aedit_"))
async def aedit(call, state, db):
    if not await admin_ok(call): return
    pid = int(call.data.split("_")[1])
    if not await db.execute("SELECT id FROM posts WHERE id=? AND deleted=0", [pid]): await call.answer("یافت نشد", show_alert=True); return
    await state.set_state(BotStates.admin_post_edit); await state.update_data(edit_post_id=pid)
    await call.message.edit_text(f"✏️ متن جدید #{pid}:", parse_mode="HTML", reply_markup=get_exit_menu()); await call.answer()
@router.message(F.chat.id == ADMIN_ID, StateFilter(BotStates.admin_post_edit))
async def admin_edit_post_input(message, state, db):
    data = await state.get_data(); pid = int(data["edit_post_id"]); t = message.text or message.caption or ""
    if not t: await message.answer("❌ خالی."); return
    await db.execute("UPDATE posts SET text=? WHERE id=?", [t, pid]); await state.set_state(BotStates.idle)
    await message.answer(f"✅ پست #{pid} ویرایش شد.", reply_markup=get_content_management_kb())
@router.callback_query(F.data.startswith("astats_"))
async def astats(call, db):
    if not await admin_ok(call): return
    p = await db.execute("SELECT likes,dislikes,views FROM posts WHERE id=?", [int(call.data.split("_")[1])])
    await call.answer(f"👁 {p[0].get('views',0)} 👍 {p[0].get('likes',0)} 👎 {p[0].get('dislikes',0)}" if p else "یافت نشد", show_alert=True)
@router.callback_query(F.data.startswith("adelete_"))
async def adelete(call):
    if not await admin_ok(call): return
    pid = int(call.data.split("_")[1])
    await call.message.edit_text(f"⚠️ حذف #{pid}؟", parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🗑", callback_data=f"adelete_confirm_{pid}"), InlineKeyboardButton(text="↩️", callback_data="admin_content")]])); await call.answer()
@router.callback_query(F.data.startswith("adelete_confirm_"))
async def adelete_confirm(call, db):
    if not await admin_ok(call): return
    pid = int(call.data.split("_")[-1])
    await db.execute("UPDATE posts SET deleted=1 WHERE id=?", [pid])
    await db.execute("DELETE FROM user_content_saves WHERE content_type='post' AND content_id=?", [pid])
    await db.execute("DELETE FROM user_content_votes WHERE content_type='post' AND content_id=?", [pid])
    await call.message.edit_text("🗑️ حذف شد.", reply_markup=get_content_management_kb()); await call.answer("حذف شد")

@router.callback_query(F.data == "help_more")
async def callback_help_more(call):
    await call.message.edit_text("🤖 این ربات پایگاه دانش فناوری/هوش مصنوعی است؛ خلاصه‌ها در کانال و متن کامل داخل ربات با لینک ادامه مطلب.", reply_markup=get_help_got_it_kb()); await call.answer()
@router.callback_query(F.data == "help_got_it")
async def callback_help_got_it(call):
    try: await call.message.delete()
    except Exception: pass
    await call.answer("🚀 بزن بریم!")
@router.callback_query(F.data == "conf_add_yes")
async def conf_add_yes(call, state, db):
    if not await admin_ok(call): return
    sd = await state.get_data(); t, f, m = sd.get("temp_text"), sd.get("temp_file_id"), sd.get("temp_media_type")
    if not t and not f: await call.answer("ناقص", show_alert=True); return
    res = await db.execute("INSERT INTO posts(text,file_id,media_type) VALUES(?,?,?) RETURNING id", [t, f, m])
    pid = res[0].get("id") if res else 0
    await state.update_data(temp_text=None, temp_file_id=None, temp_media_type=None); await state.set_state(BotStates.idle)
    await call.message.answer(f"✅ آرشیو شد:\nhttps://t.me/{BOT_USERNAME_RUNTIME or BOT_USERNAME.lstrip('@')}?start={pid}")
    await call.answer("✅")
@router.callback_query(F.data == "conf_add_no")
async def conf_add_no(call, state):
    if not await admin_ok(call): return
    await state.update_data(temp_text=None, temp_file_id=None, temp_media_type=None); await state.set_state(BotStates.idle)
    await call.message.answer("❌ لغو شد."); await call.answer()
@router.callback_query(F.data == "conf_broad_yes")
async def conf_broad_yes(call, state, db, bot):
    if not await admin_ok(call): return
    sd = await state.get_data(); t, f, m = sd.get("temp_text"), sd.get("temp_file_id"), sd.get("temp_media_type")
    users = await db.execute("SELECT id FROM users")
    if not users: await call.message.answer("⚠️ کاربری نیست."); await call.answer(); return
    await call.answer("🚀 شروع…")
    ok = fail = 0
    async def send(uid):
        try:
            if m == "photo" and f: await bot.send_photo(uid, f, caption=t[:1020])
            elif m == "document" and f: await bot.send_document(uid, f, caption=t[:1020])
            elif m == "video" and f: await bot.send_video(uid, f, caption=t[:1020])
            else: await bot.send_message(uid, t[:4090] or "پیام همگانی")
            return True
        except Exception: return False
    for i in range(0, len(users), 20):
        res = await asyncio.gather(*[send(u["id"]) for u in users[i:i+20]])
        ok += sum(res); fail += len(res) - sum(res)
        await asyncio.sleep(0.2)
    await state.update_data(temp_text=None, temp_file_id=None, temp_media_type=None); await state.set_state(BotStates.idle)
    await call.message.answer(f"✅ همگانی: موفق {ok} / ناموفق {fail}")
@router.callback_query(F.data == "conf_broad_no")
async def conf_broad_no(call, state):
    if not await admin_ok(call): return
    await state.update_data(temp_text=None, temp_file_id=None, temp_media_type=None); await state.set_state(BotStates.idle)
    await call.message.answer("❌ لغو شد."); await call.answer()
@router.callback_query(F.data == "noop")
async def noop(call): await call.answer()

# ============================================================
# MAIN (FIX: middleware ثبت شد + cleanup جلسه‌ها)
# ============================================================
async def main():
    global BOT_USERNAME, BOT_USERNAME_RUNTIME
    if not API_TOKEN: raise RuntimeError("TELEGRAM_BOT_TOKEN is not configured")
    if not (CF_ACCOUNT_ID and CF_DATABASE_ID and CF_API_TOKEN): raise RuntimeError("Cloudflare D1 env not configured")
    bot = Bot(token=API_TOKEN)
    try:
        me = await bot.get_me()
        if me.username: BOT_USERNAME = me.username; BOT_USERNAME_RUNTIME = me.username
    except Exception: pass
    dp = Dispatcher(storage=MemoryStorage())
    db = D1Database(CF_ACCOUNT_ID, CF_DATABASE_ID, CF_API_TOKEN)
    await db.start()
    automation_task = None
    try:
        dp["db"] = db
        await initialize_database(db)
        await migrate_unified_user_interactions(db)
        await initialize_automation_database(db)
        dp.message.middleware(RateLimitMiddleware(ADMIN_ID))
        dp.callback_query.middleware(RateLimitMiddleware(ADMIN_ID))
        dp.include_router(router)
        automation_task = asyncio.create_task(automation_loop(db, bot))
        logger.info("Bot started (full fixed).")
        await dp.start_polling(bot)
    finally:
        if automation_task:
            automation_task.cancel()
            try: await automation_task
            except asyncio.CancelledError: pass
        await db.close()
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())