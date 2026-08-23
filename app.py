# VERSION 10.26.0 — FIXED / OPERATIONAL / D1-FREE-PLAN OPTIMIZED
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
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)

load_dotenv()

# ============================================================
# Config
# ============================================================
API_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
BOT_USERNAME = os.getenv("BOT_USERNAME", "TechNowAibot")
BOT_USERNAME_RUNTIME = ""
BUILD_VERSION = "10.26.0-fixed-d1lite"

CF_ACCOUNT_ID = os.getenv("CF_ACCOUNT_ID")
CF_DATABASE_ID = os.getenv("CF_DATABASE_ID")
CF_API_TOKEN = os.getenv("CF_API_TOKEN")

CHANNEL_ID = os.getenv("CHANNEL_ID", "")
AI_PROVIDER_ENCRYPTION_KEY = os.getenv("AI_PROVIDER_ENCRYPTION_KEY", "")
HTTP_USER_AGENT = os.getenv("HTTP_USER_AGENT", "TechNowAI/2.0 (+content automation)")

# Free-plan friendly defaults
AUTOMATION_ENABLED_DEFAULT = os.getenv("AUTOMATION_ENABLED", "true").lower() in {"1", "true", "yes", "on"}
DEFAULT_SOURCE_INTERVAL_MINUTES = max(15, int(os.getenv("DEFAULT_SOURCE_INTERVAL_MINUTES", "60")))
WEBSCOUT_FRESHNESS_HOURS = float(os.getenv("WEBSCOUT_FRESHNESS_HOURS", "6"))
WEBSCOUT_SUCCESS_INTERVAL_MINUTES = max(20, int(os.getenv("WEBSCOUT_SUCCESS_INTERVAL_MINUTES", "60")))
WEBSCOUT_EMPTY_RETRY_MINUTES = max(15, int(os.getenv("WEBSCOUT_EMPTY_RETRY_MINUTES", "20")))
WEBSCOUT_HEARTBEAT_SECONDS = max(180, int(os.getenv("WEBSCOUT_HEARTBEAT_SECONDS", "240")))
WEBSCOUT_LOOP_SLEEP_SECONDS = max(15, int(os.getenv("WEBSCOUT_LOOP_SLEEP_SECONDS", "20")))
AUTOMATION_CLEANUP_INTERVAL_SECONDS = max(3600, int(os.getenv("AUTOMATION_CLEANUP_INTERVAL_SECONDS", "21600")))
DEFAULT_MAX_DAILY_POSTS = int(os.getenv("MAX_DAILY_POSTS", "6"))
DEFAULT_MIN_CONTENT_SCORE = float(os.getenv("MIN_CONTENT_SCORE", "65"))
MANAGER_SCORE_TOLERANCE = float(os.getenv("MANAGER_SCORE_TOLERANCE", "8"))
DEFAULT_MIN_HOURS_BETWEEN_POSTS = float(os.getenv("MIN_HOURS_BETWEEN_POSTS", "2"))
DEFAULT_MIN_POST_GAP_MINUTES = max(1, int(round(DEFAULT_MIN_HOURS_BETWEEN_POSTS * 60)))
DEFAULT_PUBLISH_START_HOUR = int(os.getenv("PUBLISH_START_HOUR", "8"))
DEFAULT_PUBLISH_END_HOUR = int(os.getenv("PUBLISH_END_HOUR", "23"))
CONTENT_RETENTION_DAYS = int(os.getenv("CONTENT_RETENTION_DAYS", "7"))
LOG_RETENTION_DAYS = int(os.getenv("LOG_RETENTION_DAYS", "7"))
AI_PROVIDER_RECHECK_MINUTES = int(os.getenv("AI_PROVIDER_RECHECK_MINUTES", "15"))
HTTP_TIMEOUT_SECONDS = int(os.getenv("HTTP_TIMEOUT_SECONDS", "45"))

SETTINGS_CACHE_TTL = 120.0
SOURCES_CACHE_TTL = 60.0
PROVIDERS_CACHE_TTL = 30.0
PUBLISH_ATTEMPT_INTERVAL = 90.0

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SETTINGS_CACHE: Dict[str, Tuple[str, float]] = {}
SOURCES_CACHE: Dict[str, Any] = {"ts": 0.0, "rows": []}
PROVIDERS_CACHE: Dict[str, Any] = {"ts": 0.0, "rows": []}

PUBLISH_LOCK = asyncio.Lock()
LAST_RECOVER = 0.0
LAST_AI_FINAL_NOTICE = 0.0
LAST_UNSUPPORTED_NOTICE = 0.0
LAST_SOURCE_ERROR_NOTICE = 0.0

# ============================================================
# D1 Database
# ============================================================
class D1Database:
    def __init__(self, account_id: str, database_id: str, api_token: str):
        self.account_id = account_id
        self.database_id = database_id
        self.api_token = api_token
        self.url = f"https://api.cloudflare.com/client/v4/accounts/{account_id}/d1/database/{database_id}/query"
        self.headers = {
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json",
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
                    logger.error("D1 API Error status=%s sql=%.120s", resp.status, sql.replace("\n", " "))
                    raise Exception(f"Cloudflare D1 API returned status {resp.status}")

                data = await resp.json()
                if not data.get("success"):
                    logger.error("D1 Query failed sql=%.120s", sql.replace("\n", " "))
                    raise Exception("D1 Query failed")

                result = data.get("result", [])
                if isinstance(result, list) and len(result) > 0:
                    return result[0].get("results", [])
                if isinstance(result, dict):
                    return result.get("results", [])
                return []
        except Exception as e:
            # Do NOT log params; they may contain secrets.
            logger.error("SQL error: %.160s | %s", sql.replace("\n", " "), e)
            raise
        finally:
            if temporary_session:
                await session.close()

    async def execute_batch(self, queries: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
        out = []
        for q in queries:
            out.append(await self.execute(q["sql"], q.get("params")))
        return out

# ============================================================
# Core utilities
# ============================================================
def normalize_url(url: str) -> str:
    url = (url or "").strip()
    if not url:
        return ""
    parsed = urllib.parse.urlsplit(url if "://" in url else "https://" + url)
    query = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    query = [(k, v) for k, v in query if not k.lower().startswith(("utm_", "fbclid", "gclid"))]
    return urllib.parse.urlunsplit(
        (parsed.scheme.lower(), parsed.netloc.lower(), parsed.path.rstrip("/") or "/", urllib.parse.urlencode(query), "")
    )

def text_hash(text: str) -> str:
    return hashlib.sha256(re.sub(r"\s+", " ", text or "").strip().lower().encode("utf-8", errors="ignore")).hexdigest()

def normalize_model_text(value: str) -> str:
    if value is None:
        return ""
    text = str(value)
    text = text.replace("\\r\\n", "\n").replace("\\n", "\n").replace("\\r", "\n").replace("\\t", "\t")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = text.replace("\u00a0", " ")
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()

def strip_html_text(value: str) -> str:
    if not value:
        return ""
    value = normalize_model_text(value)
    value = re.sub(r"<script.*?</script>|<style.*?</style>|<noscript.*?</noscript>", " ", value, flags=re.I | re.S)
    value = re.sub(r"<[^>]+>", " ", value)
    return re.sub(r"\s+", " ", html.unescape(value)).strip()

def parse_publication_datetime(raw: str) -> Optional[datetime]:
    raw = normalize_model_text(raw or "").strip()
    if not raw:
        return None

    candidates = [raw, raw.replace("Z", "+00:00")]
    for value in candidates:
        try:
            dt = datetime.fromisoformat(value)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            pass

    try:
        from email.utils import parsedate_to_datetime
        dt = parsedate_to_datetime(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

PAYWALL_KEYWORDS = [
    "ادامه مطلب", "برای مشاهده", "اشتراک", "محدود", "ثبت نام", "عضویت",
    "خرید اشتراک", "دسترسی کامل", "متن کامل", "نمایش کامل", "بیشتر بخوانید",
    "continue reading", "subscribe", "sign up", "register", "full access",
    "premium", "paywall", "limited access", "you have reached",
    "already a member", "log in", "login"
]

def is_insufficient_content(title: str, body: str, description: str) -> Tuple[bool, str]:
    title_plain = strip_html_text(title or "").strip()
    desc_plain = strip_html_text(description or "").strip()
    body_plain = strip_html_text(body or "").strip()
    combined = (title_plain + " " + desc_plain + " " + body_plain).lower()

    if len(body_plain) < 20 and len(title_plain) < 30 and len(desc_plain) < 50:
        return True, "محتوا بسیار کوتاه و فاقد اطلاعات کافی است"

    if any(kw in combined for kw in PAYWALL_KEYWORDS):
        if len(body_plain) < 500:
            return True, "محتوای پشت‌دیوار/اشتراکی است و اطلاعات کافی ندارد"

    if len(body_plain) < 100:
        word_count = len(re.findall(r"\w+", title_plain + " " + desc_plain))
        if word_count < 20:
            return True, "محتوا بسیار کوتاه و فاقد اطلاعات کافی است"

    if len(body_plain) < 300:
        if re.search(r"\d{4}[-/]\d{1,2}[-/]\d{1,2}", body_plain) or re.search(r"\d+%", body_plain):
            return False, ""
        if len(body_plain) < 180:
            return True, "محتوا برای تولید مقاله غنی کافی نیست"

    return False, ""

def parse_json_object(text: str) -> Dict[str, Any]:
    text = (text or "").strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text, flags=re.I | re.S).strip()

    def _loads(candidate: str):
        try:
            obj = json.loads(candidate)
            return obj if isinstance(obj, dict) else {}
        except Exception:
            repaired = re.sub(r"\\(?![\"/bfnrt]|u[0-9a-fA-F]{4})", lambda m: "\\\\", candidate)
            try:
                obj = json.loads(repaired)
                return obj if isinstance(obj, dict) else {}
            except Exception:
                return {}

    obj = _loads(text)
    if obj:
        return obj
    m = re.search(r"\{.*\}", text, flags=re.S)
    return _loads(m.group(0)) if m else {}

def manager_accepts_score(score: float, min_score: float) -> bool:
    try:
        score = float(score or 0)
        minimum = float(min_score or 0)
    except Exception:
        return False
    if minimum <= 1:
        return True
    return score >= max(0.0, minimum - MANAGER_SCORE_TOLERANCE)

def get_tehran_date() -> str:
    return datetime.now(pytz.timezone("Asia/Tehran")).strftime("%Y-%m-%d")

# ============================================================
# Telegram-safe HTML + safe splitting
# ============================================================
class TelegramHTMLSanitizer:
    ALLOWED = {"b", "strong", "i", "em", "u", "s", "del", "code", "pre", "blockquote", "a", "tg-spoiler"}
    BLOCK = {"p", "div", "section", "article", "header", "footer", "h1", "h2", "h3", "h4", "h5", "h6", "ul", "ol", "li"}

    def __init__(self):
        self.out = []
        self.stack = []

    def _newline(self, count=1):
        if not self.out:
            return
        current = "".join(self.out)
        target = "\n" * count
        if not current.endswith(target):
            if count == 2 and current.endswith("\n"):
                self.out.append("\n")
            else:
                self.out.append(target)

    def handle_data(self, data):
        data = str(data or "")
        data = data.replace("\u00a0", " ")
        data = re.sub(r"\n{3,}", "\n\n", data)
        if data:
            self.out.append(html.escape(data, quote=False))

    def handle_starttag(self, tag, attrs):
        tag = tag.lower()
        if tag in self.BLOCK:
            self._newline(2 if tag in {"p", "div", "section", "article", "header", "footer", "h1", "h2", "h3", "h4", "h5", "h6"} else 1)
            if tag == "li":
                self.out.append("• ")
            return

        if tag not in self.ALLOWED:
            return

        if tag == "a":
            href = dict(attrs).get("href", "")
            if href.startswith(("https://", "http://", "tg://")):
                self.out.append(f'<a href="{html.escape(href, quote=True)}">')
                self.stack.append("a")
            return

        self.out.append(f"<{tag}>")
        self.stack.append(tag)

    def handle_startendtag(self, tag, attrs):
        if tag.lower() == "br":
            self._newline(1)

    def handle_endtag(self, tag):
        tag = tag.lower()
        if tag in self.BLOCK:
            self._newline(1)
            return
        if tag not in self.ALLOWED:
            return
        if tag in self.stack:
            while self.stack:
                t = self.stack.pop()
                self.out.append(f"</{t}>")
                if t == tag:
                    break

    def get_result(self):
        while self.stack:
            self.out.append(f"</{self.stack.pop()}>")
        return "".join(self.out)

def sanitize_telegram_html(value: str) -> str:
    value = normalize_model_text(value)
    value = re.sub(r"&lt;\s*(/?\s*(?:blockquote|b|strong|i|em|u|s|del|code|pre|tg-spoiler))\s*&gt;", r"<\1>", value, flags=re.I | re.S)
    if not value:
        return ""
    try:
        from html.parser import HTMLParser

        class Parser(HTMLParser, TelegramHTMLSanitizer):
            def __init__(self):
                HTMLParser.__init__(self, convert_charrefs=True)
                TelegramHTMLSanitizer.__init__(self)

        p = Parser()
        p.feed(value)
        p.close()
        result = p.get_result()
        result = re.sub(r"[ \t]+\n", "\n", result)
        result = re.sub(r"\n[ \t]+", "\n", result)
        result = re.sub(r"\n{3,}", "\n\n", result)
        return result.strip()
    except Exception:
        return html.escape(strip_html_text(value), quote=False)

def plain_len(value: str) -> int:
    return len(strip_html_text(value or ""))

def split_html_safe(value: str, limit: int = 3800) -> List[str]:
    value = sanitize_telegram_html(value or "")
    if not value:
        return []
    if plain_len(value) <= limit:
        return [value]

    blocks = [b.strip() for b in re.split(r"\n\s*\n+", value) if strip_html_text(b).strip()]
    chunks = []
    cur = ""
    for block in blocks:
        candidate = (cur + "\n\n" + block).strip() if cur else block
        if plain_len(candidate) <= limit:
            cur = candidate
        else:
            if cur:
                chunks.append(cur)
            if plain_len(block) <= limit:
                cur = block
            else:
                plain = strip_html_text(block)
                for i in range(0, len(plain), limit):
                    chunks.append(html.escape(plain[i:i + limit], quote=False))
                cur = ""
    if cur:
        chunks.append(cur)
    return chunks

def _normalize_text_blocks(value: str) -> str:
    value = (value or "").replace("\r\n", "\n").replace("\r", "\n")
    value = value.replace("\\n", "\n")
    value = re.sub(r"<br\s*/?>", "\n", value, flags=re.I)
    value = re.sub(r"[ \t]+", " ", value)
    value = re.sub(r"[ \t]*\n[ \t]*", "\n", value)
    value = re.sub(r"\n{3,}", "\n\n", value)
    return value.strip()

def _remove_duplicate_title_from_body(title: str, value: str) -> str:
    text = _normalize_text_blocks(value or "")
    title_plain = strip_html_text(title or "").strip()
    if not text or not title_plain:
        return text

    blocks = [x.strip() for x in re.split(r"\n\s*\n+", text) if strip_html_text(x).strip()]
    if not blocks:
        return text

    kept = []
    skipping = True
    from difflib import SequenceMatcher
    for block in blocks:
        plain = strip_html_text(block).strip()
        sim = SequenceMatcher(None, plain.lower(), title_plain.lower()).ratio() if plain else 0
        looks_like_title = (sim >= 0.72 or (title_plain.lower() in plain.lower() and len(plain) <= max(40, len(title_plain) * 1.8)))
        if skipping and looks_like_title:
            continue
        skipping = False
        kept.append(block)
    return "\n\n".join(kept)

def _split_readable_paragraphs(value: str, max_chars: int = 520) -> List[str]:
    raw = _normalize_text_blocks(value or "")
    blocks = [x.strip() for x in re.split(r"\n\s*\n+", raw) if strip_html_text(x).strip()]
    if not blocks:
        return []

    out = []
    for block in blocks:
        plain = strip_html_text(block).strip()
        if len(plain) <= max_chars:
            out.append(block)
            continue

        sentences = re.split(r"(?<=[.!?؟:])\s+", block)
        current = ""
        for sent in sentences:
            sent = sent.strip()
            if not sent:
                continue
            candidate = (current + " " + sent).strip()
            if current and len(strip_html_text(candidate)) > max_chars:
                out.append(current.strip())
                current = sent
            else:
                current = candidate
        if current:
            out.append(current.strip())
    return out

def _mandatory_quote_block(paragraphs: List[str], start_index: int = 1) -> Tuple[str, int]:
    if not paragraphs:
        return "", -1
    order = list(range(start_index, len(paragraphs))) + list(range(0, start_index))
    for idx in order:
        plain = strip_html_text(paragraphs[idx]).strip()
        if len(plain) < 20:
            continue
        sentences = [x.strip() for x in re.split(r"(?<=[.!?؟])\s+", plain) if x.strip()]
        excerpt = next((x for x in sentences if 20 <= len(x) <= 220), "")
        if not excerpt:
            excerpt = plain[:180].rsplit(" ", 1)[0] + ("…" if len(plain) > 180 else "")
        return f"<blockquote>🔎 {html.escape(excerpt, quote=False)}</blockquote>", idx
    return "", -1

def dedupe_adjacent_emojis(text: str) -> str:
    emojis = ["💻", "⚙️", "🚀", "🔎", "🤖", "🧠", "⚡", "🔬", "🛡️", "🔐", "🚨", "🧩", "📚", "💡", "🧭", "📝", "🌐", "✨", "📌", "🔭", "📱", "🔍", "🛰️", "🧪", "🛠️", "🎯", "📢", "📰", "🔗"]
    for e in emojis:
        while f"{e} {e}" in text:
            text = text.replace(f"{e} {e}", e)
        while f"{e}{e}" in text:
            text = text.replace(f"{e}{e}", e)
    return text

def clean_channel_copy(value: str) -> str:
    text = normalize_model_text(value or "")
    for pat in [
        r"(?:📖\s*)?(?:بیشتر بخوانید|ادامه مطلب|برای ادامه(?: متن| مطلب)?(?: روی| از) لینک(?: زیر)? کلیک کنید)\s*",
        r"(?:روی لینک|از طریق لینک) (?:زیر|بالا) کلیک کنید",
        r"لینک ادامه مطلب\s*",
        r"<a\s+href=[^>]+>\s*(?:منبع اصلی|منبع)\s*</a>"
    ]:
        text = re.sub(pat, "", text, flags=re.I | re.S)
    return re.sub(r"\n{3,}", "\n\n", text).strip()

def _visualize_plain_paragraphs(title: str, value: str, category: str, article: bool = False) -> str:
    value = _remove_duplicate_title_from_body(title, value or "")
    value = re.sub(r"</?blockquote[^>]*>", "", value, flags=re.I)
    clean = sanitize_telegram_html(_normalize_text_blocks(value))
    plain = strip_html_text(clean)
    if not plain:
        return ""

    emoji_map = {
        "ai": ["🤖", "🧠", "🔬", "⚡", "🧩"],
        "cyber": ["🛡️", "🔐", "🚨", "⚠️", "🔎"],
        "tech": ["💻", "⚙️", "🚀", "🔎", "🧪"],
        "edu": ["📚", "💡", "🧭", "📝", "🎓"],
        "general": ["🌐", "✨", "📌", "🔭", "🧭"],
    }
    icons = emoji_map.get(category, emoji_map["tech"])
    paragraphs = _split_readable_paragraphs(clean, max_chars=430 if not article else 560) or [clean]
    out = [f"<b>{icons[0]} {html.escape(strip_html_text(title)[:220])}</b>"]
    quote, quote_index = _mandatory_quote_block(paragraphs, start_index=1)

    from difflib import SequenceMatcher
    last_icon = None
    for i, para in enumerate(paragraphs[:12]):
        pplain = strip_html_text(para).strip()
        title_similarity = SequenceMatcher(None, pplain.lower(), strip_html_text(title).lower()).ratio()
        if not pplain or title_similarity > 0.82:
            continue

        if i == quote_index:
            out.append(quote)
            continue

        icon = icons[i % len(icons)]
        if icon == last_icon:
            icon = icons[(i + 1) % len(icons)]
        last_icon = icon

        has_rich = any(tag in para.lower() for tag in ("<b>", "<strong>", "<i>", "<em>", "<u>", "<s>", "<a ", "<pre>", "<code>", "<blockquote>"))
        if has_rich:
            formatted = sanitize_telegram_html(para)
        else:
            formatted = html.escape(pplain, quote=False)

        if i == 1:
            formatted = f"{icon} <b>{formatted}</b>"
        elif i == 3 and len(pplain) <= 140:
            formatted = f"{icon} <i>{formatted}</i>"
        else:
            formatted = f"{icon} {formatted}"

        out.append(formatted)

    return dedupe_adjacent_emojis("\n\n".join(out))

def ensure_rich_channel_format(title: str, value: str, category: str = "tech") -> str:
    return _visualize_plain_paragraphs(title, clean_channel_copy(value or ""), category, article=False)

def ensure_rich_article_format(title: str, value: str, source_url: str, category: str = "tech") -> str:
    clean = _normalize_text_blocks(value or "")
    if not strip_html_text(sanitize_telegram_html(clean)):
        return ""
    return _visualize_plain_paragraphs(title, clean, category, article=True)

def remove_article_metadata_blocks(value: str) -> str:
    text = _normalize_text_blocks(value or "")
    text = re.sub(r"(?:<u>)?\s*🔗\s*لینک(?:‌| )های مرتبط.*$", "", text, flags=re.I | re.S)
    text = re.sub(r"\n+.*?تاریخ انتشار\s*:.+?(?=\n|$)", "", text, flags=re.I)
    text = re.sub(r"\n+<i>⏱.*?پیش</i>", "", text, flags=re.I | re.S)
    return _normalize_text_blocks(text)

def relative_time_label(value: str) -> str:
    if not value:
        return "زمان نامشخص"
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        seconds = max(0, int((now - dt.astimezone(timezone.utc)).total_seconds()))

        def fa(n: int) -> str:
            return str(n).translate(str.maketrans("0123456789", "۰۱۲۳۴۵۶۷۸۹"))

        if seconds < 60:
            return "همین الان"
        minutes = seconds // 60
        if minutes < 60:
            return f"{fa(minutes)} دقیقه پیش"
        hours = minutes // 60
        if hours < 24:
            return f"{fa(hours)} ساعت پیش"
        days = hours // 24
        if days < 7:
            return f"{fa(days)} روز پیش"
        weeks = days // 7
        if weeks < 5:
            return f"{fa(weeks)} هفته پیش"
        months = days // 30
        if months < 12:
            return f"{fa(months)} ماه پیش"
        years = days // 365
        return f"{fa(years)} سال پیش"
    except Exception:
        return "زمان نامشخص"

def sanitize_resource_links(raw_links):
    out = []
    seen = set()
    if not isinstance(raw_links, list):
        return out
    for item in raw_links:
        if not isinstance(item, dict):
            continue
        url = normalize_url(str(item.get("url") or ""))
        label = strip_html_text(str(item.get("label") or item.get("title") or "")).strip()
        if not url.startswith(("http://", "https://")) or not label or url in seen:
            continue
        seen.add(url)
        out.append({"label": label[:120], "url": url})
    return out[:5]

def append_resource_links(article_html: str, resource_links, source_url: str = "") -> str:
    clean = remove_article_metadata_blocks(article_html)
    main = normalize_url(source_url or "")
    if main:
        clean = re.sub(r'<a\s+href=["\']' + re.escape(main) + r'["\'][^>]*>.*?</a>', "", clean, flags=re.I | re.S)
        clean = re.sub(r'<a\s+href=["\'][^"\']+["\'][^>]*>\s*(?:منبع اصلی|منبع)\s*</a>', "", clean, flags=re.I | re.S)
        clean = re.sub(r"(?:<u>|<b>|<strong>|<i>|<em>)?\s*🔗?\s*(?:لینک(?:‌| )های مرتبط|منبع اصلی|منبع)\s*(?:</u>|</b>|</strong>|</i>|</em>)?", "", clean, flags=re.I)
        clean = re.sub(r"\n{3,}", "\n\n", clean).strip()

    rendered = []
    if main:
        rendered.append(f'<a href="{html.escape(main, quote=True)}">منبع اصلی</a>')

    for x in sanitize_resource_links(resource_links):
        label = x["label"]
        url = normalize_url(x["url"])
        if url == main:
            continue
        if not re.search(r"ثبت[-‌ ]?نام|عضویت|دانلود|دریافت|مستندات|docs|register|signup|خرید|قیمت|demo|دمو|مشاهده", label, re.I):
            continue
        rendered.append(f'<a href="{html.escape(url, quote=True)}">{html.escape(label)}</a>')
        break

    return clean.rstrip() + "\n" + " · ".join(rendered) if rendered else clean

def publication_caption(title: str, channel_html: str, deep_link: str) -> str:
    link = f'<a href="{html.escape(deep_link, quote=True)}">📖 بیشتر بخوانید</a>'
    clean = sanitize_telegram_html(clean_channel_copy(channel_html or title or ""))
    if not strip_html_text(clean):
        clean = html.escape(strip_html_text(title or "مطلب")[:500], quote=False)

    blocks = [b.strip() for b in re.split(r"\n\s*\n+", clean) if strip_html_text(b).strip()]
    if not blocks:
        blocks = [clean]

    out = []
    link_plain = strip_html_text(link)
    for b in blocks:
        candidate = "\n\n".join(out + [b])
        if plain_len(candidate + "\n" + link_plain) <= 980:
            out.append(b)
        else:
            break

    if not out:
        out = [blocks[0]]

    base = "\n\n".join(out)
    if plain_len(base + "\n" + link_plain) > 1000:
        plain = strip_html_text(base)
        plain = plain[:850].rsplit(" ", 1)[0] + "…"
        base = html.escape(plain, quote=False)

    return base.rstrip() + "\n" + link

# ============================================================
# Database initialization / settings / caches
# ============================================================
async def initialize_database(db: D1Database):
    queries = [
        {"sql": "CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY, joined_at TEXT, role TEXT DEFAULT 'user', tokens_used INTEGER DEFAULT 0, last_reset_date TEXT)"},
        {"sql": "CREATE TABLE IF NOT EXISTS posts(id INTEGER PRIMARY KEY AUTOINCREMENT, text TEXT, file_id TEXT, media_type TEXT, likes INTEGER DEFAULT 0, dislikes INTEGER DEFAULT 0, views INTEGER DEFAULT 0, deleted INTEGER DEFAULT 0)"},
        {"sql": "CREATE INDEX IF NOT EXISTS idx_posts_deleted ON posts(deleted)"},
        {"sql": "CREATE TABLE IF NOT EXISTS user_content_saves(user_id INTEGER NOT NULL, content_type TEXT NOT NULL, content_id INTEGER NOT NULL, folder TEXT NOT NULL, created_at TEXT, PRIMARY KEY(user_id, content_type, content_id))"},
        {"sql": "CREATE INDEX IF NOT EXISTS idx_user_content_saves_user_folder ON user_content_saves(user_id, folder)"},
        {"sql": "CREATE TABLE IF NOT EXISTS user_content_votes(user_id INTEGER NOT NULL, content_type TEXT NOT NULL, content_id INTEGER NOT NULL, vote_type TEXT NOT NULL, created_at TEXT, PRIMARY KEY(user_id, content_type, content_id))"},
        {"sql": "CREATE INDEX IF NOT EXISTS idx_user_content_votes_content ON user_content_votes(content_type, content_id)"},
    ]
    await db.execute_batch(queries)

async def migrate_unified_user_interactions(db: D1Database):
    try:
        rows = await db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('saves','votes','article_saves','article_votes')")
        existing = {str(r.get("name") or "") for r in rows}
        inserts = []
        if "saves" in existing:
            inserts.append("INSERT OR IGNORE INTO user_content_saves(user_id,content_type,content_id,folder,created_at) SELECT user, 'post', post, folder, NULL FROM saves")
        if "article_saves" in existing:
            inserts.append("INSERT OR IGNORE INTO user_content_saves(user_id,content_type,content_id,folder,created_at) SELECT user_id, 'article', article_id, folder, NULL FROM article_saves")
        if "votes" in existing:
            inserts.append("INSERT OR IGNORE INTO user_content_votes(user_id,content_type,content_id,vote_type,created_at) SELECT user_id, 'post', post_id, vote_type, NULL FROM votes")
        if "article_votes" in existing:
            inserts.append("INSERT OR IGNORE INTO user_content_votes(user_id,content_type,content_id,vote_type,created_at) SELECT user_id, 'article', article_id, vote_type, NULL FROM article_votes")

        for sql in inserts:
            await db.execute(sql)

        for legacy in ("saves", "votes", "article_saves", "article_votes"):
            if legacy in existing:
                await db.execute(f"DROP TABLE IF EXISTS {legacy}")
    except Exception:
        logger.exception("migrate_unified_user_interactions failed")

async def initialize_automation_database(db: D1Database):
    queries = [
        {"sql": "CREATE TABLE IF NOT EXISTS sources(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, url TEXT UNIQUE, feed_url TEXT, category TEXT DEFAULT 'tech', enabled INTEGER DEFAULT 1, interval_minutes INTEGER DEFAULT 60, priority INTEGER DEFAULT 5, last_checked_at TEXT, next_check_at TEXT, last_error TEXT, trust_score REAL DEFAULT 80, created_at TEXT, last_seen_published_at TEXT, last_seen_url TEXT)"},
        {"sql": "CREATE INDEX IF NOT EXISTS idx_sources_due ON sources(enabled, next_check_at)"},
        {"sql": "CREATE TABLE IF NOT EXISTS articles(id INTEGER PRIMARY KEY AUTOINCREMENT, source_item_id INTEGER UNIQUE, title TEXT, channel_text TEXT, body TEXT, source_url TEXT, image_url TEXT, category TEXT, score REAL, status TEXT DEFAULT 'ready', deep_token TEXT UNIQUE, created_at TEXT, verified_at TEXT, published_message_id INTEGER, source_published_at TEXT, deep_views INTEGER DEFAULT 0, content_hash TEXT, published_at TEXT)"},
        {"sql": "CREATE INDEX IF NOT EXISTS idx_articles_status ON articles(status)"},
        {"sql": "CREATE INDEX IF NOT EXISTS idx_articles_hash ON articles(content_hash)"},
        {"sql": "CREATE TABLE IF NOT EXISTS publication_queue(id INTEGER PRIMARY KEY AUTOINCREMENT, article_id INTEGER UNIQUE, scheduled_at TEXT, status TEXT DEFAULT 'queued', attempts INTEGER DEFAULT 0, last_error TEXT, created_at TEXT, published_at TEXT, last_attempt_at TEXT)"},
        {"sql": "CREATE INDEX IF NOT EXISTS idx_publication_queue_due ON publication_queue(status, scheduled_at)"},
        {"sql": "CREATE TABLE IF NOT EXISTS ai_providers(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE, base_url TEXT, encrypted_api_key TEXT, model_name TEXT, priority INTEGER DEFAULT 10, enabled INTEGER DEFAULT 1, web_enabled INTEGER DEFAULT 0, created_at TEXT, updated_at TEXT, status TEXT DEFAULT 'unknown', last_error TEXT, cooldown_until TEXT, last_checked_at TEXT, last_latency_ms INTEGER DEFAULT 0, consecutive_failures INTEGER DEFAULT 0)"},
        {"sql": "CREATE TABLE IF NOT EXISTS automation_settings(key TEXT PRIMARY KEY, value TEXT)"},
        {"sql": "CREATE TABLE IF NOT EXISTS automation_logs(id INTEGER PRIMARY KEY AUTOINCREMENT, level TEXT, event TEXT, details TEXT, created_at TEXT)"},
        {"sql": "CREATE INDEX IF NOT EXISTS idx_automation_logs_created ON automation_logs(created_at)"},
        {"sql": "CREATE TABLE IF NOT EXISTS test_history(id INTEGER PRIMARY KEY AUTOINCREMENT, source_url TEXT, content_hash TEXT, title TEXT, tested_at TEXT)"},
    ]
    await db.execute_batch(queries)

    for sql in [
        "ALTER TABLE ai_providers ADD COLUMN status TEXT DEFAULT 'unknown'",
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
        "min_post_gap_minutes": str(DEFAULT_MIN_POST_GAP_MINUTES),
        "publish_start_hour": str(DEFAULT_PUBLISH_START_HOUR),
        "publish_end_hour": str(DEFAULT_PUBLISH_END_HOUR),
        "default_source_interval": str(DEFAULT_SOURCE_INTERVAL_MINUTES),
        "webscout_freshness_hours": str(WEBSCOUT_FRESHNESS_HOURS),
        "webscout_success_interval_minutes": str(WEBSCOUT_SUCCESS_INTERVAL_MINUTES),
        "webscout_empty_retry_minutes": str(WEBSCOUT_EMPTY_RETRY_MINUTES),
        "webscout_next_run_at": "",
        "last_cleanup_at": "",
        "last_manual_channel_post_at": "",
        "channel_id": CHANNEL_ID,
        "channel_username": "",
        "worker_heartbeat_at": "",
        "worker_started_at": "",
        "last_cycle_started_at": "",
        "last_cycle_finished_at": "",
        "last_cycle_result": "",
        "ai_verify_mode": "off",
        "weight_global": "15",
        "weight_technology": "15",
        "weight_ai": "15",
        "weight_cyber": "15",
        "weight_education": "10",
        "weight_iran": "15",
        "weight_freshness": "10",
        "weight_source": "5",
        "weight_novelty": "10",
        "editorial_prompt_channel": "فقط محتوای فنی و واقعاً ارزشمند برای مخاطب فناوری و هوش مصنوعی را پوشش بده؛ خبرهای سطحی، عمومی، تبلیغاتی و تکراری را کنار بگذار.",
        "editorial_prompt_article": "نسخه کامل باید یک محتوای فنی و غنی باشد؛ جزئیات واقعی، نکات فنی مهم، زمینه و اثرات قابل فهم را پوشش بده.",
    }

    try:
        rows = await db.execute("SELECT key FROM automation_settings")
        have = {r.get("key") for r in rows}
        for k, v in defaults.items():
            if k not in have:
                await db.execute("INSERT OR IGNORE INTO automation_settings(key, value) VALUES(?, ?)", [k, v])
    except Exception:
        logger.exception("ensure defaults failed")

    # Privacy/storage: keep published image URLs from lingering.
    try:
        await db.execute("UPDATE articles SET image_url='' WHERE status='published' AND image_url IS NOT NULL AND image_url!=''")
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
    value = rows[0].get("value") if rows else None
    if value is None:
        value = default
    value = str(value)
    SETTINGS_CACHE[key] = (value, now)
    return value

async def set_setting(db: D1Database, key: str, value: str):
    await db.execute("INSERT OR REPLACE INTO automation_settings(key, value) VALUES(?, ?)", [key, str(value)])
    SETTINGS_CACHE[key] = (str(value), time.monotonic())

async def get_channel_id(db: D1Database) -> str:
    return (await get_setting(db, "channel_id", CHANNEL_ID)).strip()

def invalidate_sources():
    SOURCES_CACHE["ts"] = 0.0

def invalidate_providers():
    PROVIDERS_CACHE["ts"] = 0.0

async def get_enabled_sources(db: D1Database, force: bool = False) -> List[Dict[str, Any]]:
    now = time.monotonic()
    if not force and now - SOURCES_CACHE["ts"] < SOURCES_CACHE_TTL:
        return SOURCES_CACHE["rows"]
    rows = await db.execute("SELECT * FROM sources WHERE enabled=1 ORDER BY priority ASC, id ASC")
    SOURCES_CACHE["ts"] = now
    SOURCES_CACHE["rows"] = rows
    return rows

async def get_enabled_providers(db: D1Database, force: bool = False) -> List[Dict[str, Any]]:
    now = time.monotonic()
    if not force and now - PROVIDERS_CACHE["ts"] < PROVIDERS_CACHE_TTL:
        return PROVIDERS_CACHE["rows"]
    now_iso = datetime.now(timezone.utc).isoformat()
    rows = await db.execute(
        "SELECT * FROM ai_providers WHERE enabled=1 AND status != 'invalid' AND (cooldown_until IS NULL OR cooldown_until <= ?) ORDER BY priority ASC, id ASC",
        [now_iso]
    )
    PROVIDERS_CACHE["ts"] = now
    PROVIDERS_CACHE["rows"] = rows
    return rows

async def log_automation(db: D1Database, level: str, event: str, details: str = ""):
    try:
        if len(details) > 1500:
            details = details[:1500]
        await db.execute(
            "INSERT INTO automation_logs(level, event, details, created_at) VALUES(?, ?, ?, ?)",
            [level, event, details, datetime.now(timezone.utc).isoformat()]
        )
    except Exception:
        logger.exception("automation log failed")

async def cleanup_automation_data(db: D1Database):
    now = datetime.now(timezone.utc)
    cutoff_logs = (now - timedelta(days=LOG_RETENTION_DAYS)).isoformat()
    cutoff_queue = (now - timedelta(days=1)).isoformat()
    cutoff_test = (now - timedelta(days=1)).isoformat()
    cutoff_image = (now - timedelta(days=max(1, CONTENT_RETENTION_DAYS))).isoformat()

    await db.execute("DELETE FROM automation_logs WHERE created_at < ?", [cutoff_logs])
    await db.execute("DELETE FROM publication_queue WHERE status IN ('published','failed') AND created_at < ?", [cutoff_queue])
    await db.execute("DELETE FROM test_history WHERE tested_at < ?", [cutoff_test])
    await db.execute("UPDATE articles SET image_url='' WHERE status='published' AND COALESCE(published_at, created_at) < ?", [cutoff_image])
    await set_setting(db, "last_cleanup_at", now.isoformat())

# ============================================================
# AI Provider Manager
# ============================================================
class AIProviderManager:
    def __init__(self, db: D1Database, bot: Optional[Bot] = None):
        self.db = db
        self.bot = bot
        self._session: Optional[aiohttp.ClientSession] = None

    async def start(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=120))

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None

    async def providers(self):
        return await get_enabled_providers(self.db, force=True)

    @staticmethod
    def protocol(url: str) -> str:
        u = (url or "").lower().rstrip("/")
        if "generativelanguage.googleapis.com" in u:
            if "/openai" in u or "chat/completions" in u:
                return "openai"
            return "gemini"
        if "api.anthropic.com" in u and "/chat/completions" not in u:
            return "anthropic"
        return "openai"

    @staticmethod
    def endpoint(url: str, protocol: str, model: str = "") -> str:
        u = (url or "").strip().rstrip("/")
        if protocol == "gemini":
            if u.endswith(":generateContent"):
                return u
            if "/models/" in u:
                return u + ":generateContent"
            return u + f"/models/{urllib.parse.quote(model, safe='')}:generateContent"
        if protocol == "anthropic":
            if u.endswith("/messages"):
                return u
            if u.endswith("/v1"):
                return u + "/messages"
            return u + "/v1/messages"
        if u.endswith("/chat/completions"):
            return u
        if u.endswith("/v1"):
            return u + "/chat/completions"
        if u.endswith("/openai"):
            return u + "/chat/completions"
        return u + "/chat/completions"

    @staticmethod
    def google_openai_endpoint(base_url: str) -> str:
        u = (base_url or "").strip().rstrip("/")
        if "generativelanguage.googleapis.com" not in u:
            return ""
        if "/openai" in u:
            return u if u.endswith("/chat/completions") else u + "/chat/completions"
        return "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions"

    @staticmethod
    def _extract_content(protocol: str, data: dict) -> str:
        if not isinstance(data, dict):
            return ""
        if protocol == "anthropic":
            return "".join((b.get("text", "") for b in data.get("content", []) if isinstance(b, dict) and b.get("type") == "text"))
        if protocol == "gemini":
            parts = []
            for candidate in data.get("candidates") or []:
                content = candidate.get("content") or {}
                for part in content.get("parts") or []:
                    if isinstance(part, dict) and part.get("text"):
                        parts.append(str(part.get("text")))
            return "".join(parts).strip()

        choice = (data.get("choices") or [{}])[0] or {}
        message = choice.get("message") or {}
        content = message.get("content")
        if isinstance(content, str):
            return content.strip()
        if isinstance(content, list):
            return "".join(str(x.get("text", "")) for x in content if isinstance(x, dict)).strip()
        return str(content or "").strip()

    @staticmethod
    def _empty_response_reason(protocol: str, data: dict) -> str:
        if not isinstance(data, dict):
            return "بدنه پاسخ JSON قابل تشخیص نبود."
        if protocol == "gemini":
            reasons = []
            pf = data.get("promptFeedback") or {}
            if pf.get("blockReason"):
                reasons.append(f"promptFeedback.blockReason={pf.get('blockReason')}")
            for c in data.get("candidates") or []:
                if c.get("finishReason"):
                    reasons.append(f"finishReason={c.get('finishReason')}")
            return "؛ ".join(reasons) or "Gemini پاسخ HTTP موفق داد اما متن قابل استخراجی نداشت."
        if protocol == "anthropic":
            return str(data.get("stop_reason") or data.get("error") or "Anthropic متن قابل استخراجی نداشت.")
        choices = data.get("choices") or []
        if choices:
            c = choices[0] or {}
            return f"finish_reason={c.get('finish_reason') or '-'}؛ message.content خالی است."
        return "پاسخ API فاقد choices بود."

    @staticmethod
    def _usage_tokens(protocol: str, usage: dict) -> int:
        if not isinstance(usage, dict):
            return 0
        if protocol == "gemini":
            return int(usage.get("totalTokenCount") or usage.get("total_tokens") or 0)
        if protocol == "anthropic":
            return int((usage.get("input_tokens") or 0) + (usage.get("output_tokens") or 0) or usage.get("total_tokens") or 0)
        return int(usage.get("total_tokens") or 0)

    async def _request(self, provider, messages, temperature, max_tokens, forced_protocol: Optional[str] = None, forced_endpoint: Optional[str] = None, webscout: bool = False):
        await self.start()
        key = decrypt_secret(provider.get("encrypted_api_key") or "")
        model = (provider.get("model_name") or "").strip()
        base = provider.get("base_url") or ""
        protocol = forced_protocol or self.protocol(base)
        endpoint = forced_endpoint or self.endpoint(base, protocol, model)
        headers = {"Content-Type": "application/json", "User-Agent": HTTP_USER_AGENT}
        started = time.perf_counter()

        if protocol == "anthropic":
            headers["x-api-key"] = key
            headers["anthropic-version"] = "2023-06-01"
            system = "\n".join(m.get("content", "") for m in messages if m.get("role") == "system").strip()
            msgs = [{"role": "assistant" if m.get("role") == "assistant" else "user", "content": m.get("content", "")}
                    for m in messages if m.get("role") != "system"]
            while msgs and msgs[0]["role"] != "user":
                msgs.pop(0)
            payload = {"model": model, "messages": msgs, "max_tokens": max_tokens, "temperature": temperature}
            if system:
                payload["system"] = system

        elif protocol == "gemini":
            headers["x-goog-api-key"] = key
            contents = []
            for m in messages:
                role = m.get("role")
                if role == "system":
                    continue
                gem_role = "model" if role == "assistant" else "user"
                contents.append({"role": gem_role, "parts": [{"text": m.get("content", "")}]})
            if not contents:
                contents = [{"role": "user", "parts": [{"text": ""}]}]

            payload = {
                "contents": contents,
                "generationConfig": {"temperature": temperature, "maxOutputTokens": max_tokens}
            }
            system = "\n".join(m.get("content", "") for m in messages if m.get("role") == "system").strip()
            if system:
                payload["systemInstruction"] = {"parts": [{"text": system}]}
            if webscout:
                payload["tools"] = [{"url_context": {}}, {"google_search": {}}]

        else:
            headers["Authorization"] = f"Bearer {key}"
            payload = {"model": model, "messages": messages, "temperature": temperature, "max_tokens": max_tokens}
            if webscout and "openrouter.ai" in (base or "").lower():
                payload["tools"] = [{"type": "openrouter:web_search"}, {"type": "openrouter:web_fetch"}]
                payload["max_tool_calls"] = 6
                payload["web_search_options"] = {"search_context_size": "high"}

        async with self._session.post(endpoint, headers=headers, json=payload) as resp:
            raw = await resp.text()
            latency = int((time.perf_counter() - started) * 1000)
            if resp.status != 200:
                raise RuntimeError(f"HTTP {resp.status} | endpoint={endpoint} | body={raw[:1200]}")

            try:
                data = json.loads(raw)
            except Exception as e:
                raise RuntimeError(f"HTTP 200 ولی JSON نامعتبر بود: {e} | body={raw[:700]}")

            content = self._extract_content(protocol, data)
            usage = (data.get("usageMetadata") if protocol == "gemini" else data.get("usage")) or {}
            if not content:
                raise RuntimeError(
                    f"پاسخ مدل خالی بود | protocol={protocol} | model={model} | {self._empty_response_reason(protocol, data)}"
                )
            return content, data, latency, usage, protocol, endpoint

    async def _mark_health(self, provider, status: str, error: str = "", latency: int = 0, cooldown_minutes: int = 0):
        now = datetime.now(timezone.utc)
        cooldown = (now + timedelta(minutes=cooldown_minutes)).isoformat() if cooldown_minutes else None
        try:
            await self.db.execute(
                "UPDATE ai_providers SET status=?, last_error=?, cooldown_until=?, last_checked_at=?, last_latency_ms=?, updated_at=? WHERE id=?",
                [status, error[:1000], cooldown, now.isoformat(), latency, now.isoformat(), provider.get("id")]
            )
            invalidate_providers()
        except Exception:
            logger.exception("mark provider health failed")

    @staticmethod
    def classify_error(msg: str) -> str:
        m = msg.lower()
        if any(x in m for x in ("404", "model_not_found", "401", "403", "authentication", "invalid api")):
            return "invalid"
        return "temporary"

    async def call(self, messages, temperature=0.2, max_tokens=2500, purpose="generic", persist_health=True):
        global LAST_AI_FINAL_NOTICE
        providers = await self.providers()
        if not providers:
            return {"content": "", "provider": None, "model": None, "tokens": 0, "error": "هیچ مدل فعالی در پنل AI وجود ندارد."}

        errors = []
        tried = 0
        now = datetime.now(timezone.utc)

        for p in providers:
            cooldown = p.get("cooldown_until") or ""
            if cooldown:
                try:
                    if datetime.fromisoformat(cooldown.replace("Z", "+00:00")) > now:
                        continue
                except Exception:
                    pass

            tried += 1
            try:
                content, data, latency, usage, protocol, _ = await self._request(p, messages, temperature, max_tokens)
                if persist_health:
                    await self._mark_health(p, "healthy", "", latency, 0)
                return {
                    "content": content,
                    "provider": p.get("name"),
                    "model": p.get("model_name"),
                    "tokens": self._usage_tokens(protocol, usage),
                    "error": None,
                }
            except Exception as e:
                msg = str(e)
                errors.append(f"{p.get('name')}: {msg[:220]}")
                if persist_health:
                    kind = self.classify_error(msg)
                    if kind == "invalid":
                        await self._mark_health(p, "invalid", msg, 0, AI_PROVIDER_RECHECK_MINUTES)
                    else:
                        await self._mark_health(p, "cooldown", msg, 0, 5)

        final = ("همه مدل‌ها در cooldown یا نامعتبر هستند." if tried == 0 else "تمام مدل‌های قابل استفاده خطا دادند.") + " | " + " | ".join(errors)
        if purpose != "user_chat" and time.time() - LAST_AI_FINAL_NOTICE > 1800 and self.bot and ADMIN_ID:
            LAST_AI_FINAL_NOTICE = time.time()
            try:
                await self.bot.send_message(ADMIN_ID, "🚨 خطای نهایی AI\n" + html.escape(final[:1200]))
            except Exception:
                pass

        return {"content": "", "provider": None, "model": None, "tokens": 0, "error": final}

    async def webscout_call(self, url: str, scout_prompt: str, max_tokens: int = 7000):
        global LAST_UNSUPPORTED_NOTICE
        now_iso = datetime.now(timezone.utc).isoformat()
        providers = await self.db.execute(
            "SELECT * FROM ai_providers WHERE enabled=1 AND web_enabled=1 AND status != 'invalid' AND (cooldown_until IS NULL OR cooldown_until <= ?) ORDER BY priority ASC, id ASC",
            [now_iso]
        )
        if not providers:
            return {"ok": False, "error": "هیچ WebScout فعالی در پنل AI وجود ندارد."}

        errors = []
        for p in providers:
            model = str(p.get("model_name") or "")
            base = str(p.get("base_url") or "")
            low = base.lower()

            if "generativelanguage.googleapis.com" in low:
                protocol = "gemini"
                endpoint = self.endpoint("https://generativelanguage.googleapis.com/v1beta", "gemini", model)
            elif "openrouter.ai" in low:
                protocol = "openai"
                endpoint = self.endpoint(base, "openai", model)
            else:
                if time.time() - LAST_UNSUPPORTED_NOTICE > 3600 and self.bot and ADMIN_ID:
                    LAST_UNSUPPORTED_NOTICE = time.time()
                    try:
                        await self.bot.send_message(
                            ADMIN_ID,
                            "⚠️ یک provider برای WebScout فعال است اما پشتیبانی نمی‌شود. فقط Gemini Native یا OpenRouter."
                        )
                    except Exception:
                        pass
                continue

            messages = [
                {"role": "system", "content": "You are the WebScout research engine. Use web tools and inspect the supplied URL. Do not rely on memory."},
                {"role": "user", "content": scout_prompt + f"\nTARGET URL:\n{url}"}
            ]

            try:
                content, data, latency, usage, _, _ = await self._request(
                    p, messages, 0.1, max_tokens,
                    forced_protocol=protocol, forced_endpoint=endpoint, webscout=True
                )
                await self._mark_health(p, "healthy", "", latency, 0)
                return {"ok": True, "content": content, "provider": p.get("name"), "model": model, "latency_ms": latency, "usage": usage, "raw": data}
            except Exception as e:
                msg = str(e)
                errors.append(f"{p.get('name')}: {msg[:700]}")
                await self._mark_health(p, "cooldown", msg, 0, 5)
                continue

        return {"ok": False, "error": "\n".join(errors)[:6000]}

    async def test_provider_values(self, base_url, api_key, model):
        await self.start()
        base = (base_url or "").strip()
        key = (api_key or "").strip()
        mdl = (model or "").strip()

        if not base:
            return {"ok": False, "stage": "validation", "error": "Base URL خالی است."}
        if not key:
            return {"ok": False, "stage": "validation", "error": "API Key/Token خالی است."}
        if not mdl:
            return {"ok": False, "stage": "validation", "error": "نام مدل خالی است."}

        detected = self.protocol(base)
        candidates = [(detected, self.endpoint(base, detected, mdl))]
        if "generativelanguage.googleapis.com" in base:
            compat = self.google_openai_endpoint(base)
            if compat and all(ep != compat for _, ep in candidates):
                candidates.append(("openai", compat))
            native = self.endpoint("https://generativelanguage.googleapis.com/v1beta", "gemini", mdl)
            if all(ep != native for _, ep in candidates):
                candidates.append(("gemini", native))

        diagnostics = []
        for proto, endpoint in candidates:
            started = time.perf_counter()
            try:
                headers = {"Content-Type": "application/json", "User-Agent": HTTP_USER_AGENT}
                if proto == "anthropic":
                    headers["x-api-key"] = key
                    headers["anthropic-version"] = "2023-06-01"
                    payload = {
                        "model": mdl,
                        "messages": [{"role": "user", "content": "Reply with exactly: TEST_OK"}],
                        "max_tokens": 32,
                    }
                elif proto == "gemini":
                    headers["x-goog-api-key"] = key
                    payload = {
                        "contents": [{"role": "user", "parts": [{"text": "Reply with exactly: TEST_OK"}]}],
                        "generationConfig": {"maxOutputTokens": 32},
                    }
                else:
                    headers["Authorization"] = f"Bearer {key}"
                    payload = {
                        "model": mdl,
                        "messages": [{"role": "user", "content": "Reply with exactly: TEST_OK"}],
                    }

                async with self._session.post(endpoint, headers=headers, json=payload, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    raw = await resp.text()
                    latency = int((time.perf_counter() - started) * 1000)
                    try:
                        data = json.loads(raw)
                    except Exception:
                        data = {}

                    if resp.status == 200:
                        content = self._extract_content(proto, data)
                        if content:
                            return {
                                "ok": True,
                                "latency_ms": latency,
                                "preview": content.strip()[:120],
                                "protocol": proto,
                                "endpoint": endpoint,
                            }
                        diagnostics.append(f"{proto} HTTP 200 اما پاسخ قابل استخراج نبود.")
                        continue

                    if proto == "openai" and resp.status in {400, 422}:
                        retry_payload = {**payload, "temperature": 0, "max_tokens": 32}
                        async with self._session.post(endpoint, headers=headers, json=retry_payload, timeout=aiohttp.ClientTimeout(total=30)) as r2:
                            raw2 = await r2.text()
                            retry_latency = int((time.perf_counter() - started) * 1000)
                            try:
                                data2 = json.loads(raw2)
                            except Exception:
                                data2 = {}
                            if r2.status == 200:
                                content2 = self._extract_content("openai", data2)
                                if content2:
                                    return {
                                        "ok": True,
                                        "latency_ms": retry_latency,
                                        "preview": content2.strip()[:120],
                                        "protocol": "openai",
                                        "endpoint": endpoint,
                                    }
                    diagnostics.append(f"{proto} HTTP {resp.status}: {raw[:700]}")
            except Exception as e:
                diagnostics.append(f"{proto} {endpoint}: {type(e).__name__}: {str(e)[:700]}")

        return {
            "ok": False,
            "stage": "request",
            "protocol": detected,
            "endpoint": candidates[0][1] if candidates else "",
            "error": "\n".join(diagnostics)[:6000],
        }

    async def test_provider(self, provider_id: int):
        rows = await self.db.execute("SELECT * FROM ai_providers WHERE id=?", [provider_id])
        if not rows:
            return {"ok": False, "error": "Provider یافت نشد"}
        p = rows[0]
        result = await self.test_provider_values(p.get("base_url", ""), decrypt_secret(p.get("encrypted_api_key", "")), p.get("model_name", ""))
        now = datetime.now(timezone.utc).isoformat()

        if result["ok"]:
            await self.db.execute(
                "UPDATE ai_providers SET status='healthy', last_error=NULL, cooldown_until=NULL, last_checked_at=?, last_latency_ms=?, updated_at=? WHERE id=?",
                [now, result.get("latency_ms", 0), now, provider_id]
            )
            invalidate_providers()
        else:
            error_text = str(result.get("error", "") or "")
            kind = self.classify_error(error_text)
            status = "invalid" if kind == "invalid" else "cooldown"
            minutes = AI_PROVIDER_RECHECK_MINUTES
            cooldown = (datetime.now(timezone.utc) + timedelta(minutes=minutes)).isoformat()
            await self.db.execute(
                "UPDATE ai_providers SET status=?, last_error=?, cooldown_until=?, last_checked_at=?, updated_at=? WHERE id=?",
                [status, error_text[:1000], cooldown, now, now, provider_id]
            )
            invalidate_providers()
        return result

# ============================================================
# Editorial pipeline
# ============================================================
async def get_manager_editorial_prompts(db: D1Database) -> Dict[str, str]:
    return {
        "channel": await get_setting(db, "editorial_prompt_channel", "فقط محتوای فنی و واقعاً ارزشمند را پوشش بده."),
        "article": await get_setting(db, "editorial_prompt_article", "نسخه کامل باید فنی، غنی و مبتنی بر واقعیت‌های منبع باشد.")
    }

async def ai_editorial_process(ai: AIProviderManager, item: Dict[str, Any], source: Dict[str, Any], recent_titles: List[str], weights: Dict[str, float], manager_prompts: Optional[Dict[str, str]] = None):
    body = (item.get("webscout_research") or item.get("body") or item.get("description") or "")[:25000]
    manager_prompts = manager_prompts or {}
    channel_scope = manager_prompts.get("channel") or "تمرکز روی خبرهای فنی و ارزشمند؛ محتوای سطحی را کنار بگذار."
    article_scope = manager_prompts.get("article") or "نسخه کامل را فنی، غنی و مبتنی بر واقعیت‌های منبع بنویس."

    editorial_schema = {
        "accept": True, "score": 0, "global_relevance": 0, "technology_relevance": 0,
        "ai_relevance": 0, "cyber_relevance": 0, "education_relevance": 0, "iran_relevance": 0,
        "freshness": 0, "reliability": 0, "duplicate_risk": 0, "category": "ai|tech|cyber|edu|general",
        "why": "...", "title": "...", "channel_html": "...", "article_html": "...",
        "facts": ["..."], "resource_links": [{"label": "...", "url": "https://..."}]
    }

    prompt = f"""تو موتور تحریریه یک کانال فارسی فناوری/هوش مصنوعی هستی. فقط از اطلاعات داده‌شده استفاده کن و واقعیت نساز.
منبع: {source.get('name')}
عنوان: {item.get('title')}
URL: {item.get('url')}
تاریخ انتشار منبع: {item.get('published_at') or 'نامشخص'}
لینک‌های داخل صفحه:
{json.dumps(item.get('links') or [], ensure_ascii=False)[:4000]}
متن پژوهش WebScout:
{body}
وزن‌های مدیر:
{json.dumps(weights, ensure_ascii=False)}
دستور مدیر برای نسخه کوتاه کانال:
{channel_scope}
دستور مدیر برای نسخه کامل:
{article_scope}

خروجی فقط JSON معتبر با همین فیلدها:
{json.dumps(editorial_schema, ensure_ascii=False)}

قواعد:
- فارسی روان و طبیعی؛ پاراگراف کامل انگلیسی ممنوع.
- channel_html حدود 400 تا 600 کاراکتر، خود خبر، با HTML سازگار تلگرام.
- article_html حدود 2000 تا 3000 کاراکتر؛ اگر منبع کوتاه است، کوتاه و دقیق بمان.
- لینک ادامه مطلب را در متن کانال ننویس.
- اگر اطلاعات کافی نیست، باز هم بهترین متن کوتاه ممکن را بساز؛ اما جزئیات جعلی اضافه نکن.
"""

    result = await ai.call(
        [
            {"role": "system", "content": "You are a Persian technology content producer. Return JSON only."},
            {"role": "user", "content": prompt}
        ],
        0.25, 6000, "editorial", persist_health=True
    )

    obj = parse_json_object(result.get("content", ""))
    if not obj:
        repair_prompt = (
            "پاسخ زیر را فقط به JSON معتبر تبدیل کن؛ محتوای آن را تغییر نده. فیلدها: accept,score,category,iran_relevance,freshness,reliability,duplicate_risk,why,title,channel_html,article_html,facts.\n"
            + str(result.get("content", ""))[:12000]
        )
        retry = await ai.call(
            [{"role": "system", "content": "Return valid JSON only."}, {"role": "user", "content": repair_prompt}],
            0, 4000, "editorial_json_repair", persist_health=True
        )
        obj = parse_json_object(retry.get("content", ""))
        result = retry

    if not obj:
        return {"error": "پاسخ AI JSON معتبر نبود", "ai": result}

    raw_title = strip_html_text(obj.get("title") or item.get("title") or "")[:240]
    raw_ch = str(obj.get("channel_html") or obj.get("channel_text") or "")
    raw_ar = str(obj.get("article_html") or obj.get("article_text") or "")

    title = raw_title
    category = str(obj.get("category") or source.get("category") or "tech")

    ch = ensure_rich_channel_format(title, raw_ch, category)
    ar = ensure_rich_article_format(title, raw_ar, item.get("url") or "", category)

    if not ar:
        return {"error": "تولید محتوای کامل ناموفق بود - خروجی خالی", "ai": result}

    if not ch:
        fallback_plain = strip_html_text(raw_ar or raw_ch or title)[:500]
        ch = f"<b>{html.escape(title[:200])}</b>\n{html.escape(fallback_plain, quote=False)}"

    resource_links = sanitize_resource_links(obj.get("resource_links"))
    ar = append_resource_links(ar, resource_links, item.get("url") or "")

    obj["title"] = title
    obj["channel_html"] = ch
    obj["article_html"] = ar
    obj["resource_links"] = resource_links

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

# ============================================================
# Sources / WebScout cycle
# ============================================================
async def add_source(db: D1Database, url: str, category: str = "tech", interval_minutes: Optional[int] = None, priority: int = 5) -> int:
    clean = normalize_url(url)
    if not clean:
        raise ValueError("invalid URL")

    parsed = urllib.parse.urlsplit(clean)
    name = parsed.netloc or clean
    interval = interval_minutes or int(await get_setting(db, "default_source_interval", str(DEFAULT_SOURCE_INTERVAL_MINUTES)))
    now = datetime.now(timezone.utc)

    try:
        res = await db.execute(
            "INSERT INTO sources(name, url, category, enabled, interval_minutes, priority, next_check_at, created_at) VALUES(?, ?, ?, 1, ?, ?, ?, ?) RETURNING id",
            [name, clean, category, interval, priority, now.isoformat(), now.isoformat()]
        )
        source_id = res[0].get("id") if res else 0
    except Exception:
        rows = await db.execute("SELECT id FROM sources WHERE url=?", [clean])
        source_id = rows[0].get("id") if rows else 0

    invalidate_sources()
    return int(source_id)

def make_deep_token(article_id: int) -> str:
    return hashlib.sha256(f"techhow-{article_id}-{time.time_ns()}".encode()).hexdigest()[:18]

async def fetch_source_cycle(db: D1Database, source: Dict[str, Any], ai: AIProviderManager, progress=None, allow_old_test=False):
    stats = {
        "source": source.get("name") or source.get("url"),
        "found": 0, "seen": 0, "candidates": 0, "processed": 0,
        "accepted": 0, "rejected": 0, "errors": 0, "queued": 0,
        "method": "webscout", "diagnostics": []
    }

    url = normalize_url(source.get("url") or "")
    if not url:
        stats["errors"] = 1
        stats["diagnostics"] = ["URL منبع نامعتبر است"]
        return stats

    freshness = float(await get_setting(db, "webscout_freshness_hours", str(WEBSCOUT_FRESHNESS_HOURS)) or WEBSCOUT_FRESHNESS_HOURS)
    weights = {k: float(await get_setting(db, "weight_" + k, "10")) for k in
               ["global", "technology", "ai", "cyber", "education", "iran", "freshness", "source", "novelty"]}
    prompts = await get_manager_editorial_prompts(db)
    min_score = float(await get_setting(db, "min_content_score", str(DEFAULT_MIN_CONTENT_SCORE)))

    recent_rows = await db.execute("SELECT title FROM articles WHERE status IN ('published','ready') ORDER BY id DESC LIMIT 30")
    recent_titles = [r.get("title", "") for r in recent_rows]

    channel_id = await get_channel_id(db)
    if not channel_id:
        raise RuntimeError("CHANNEL_ID تنظیم نشده است")

    scout_prompt = f"""You are the WebScout selection engine for a Persian technology news automation system.
Open TARGET URL with web tools and find the newest substantive item published within the last {freshness:g} hours.
Do not rely on memory. If publication time cannot be verified, return FALSE.
Manager weights:
{json.dumps(weights, ensure_ascii=False)}
Channel instruction:
{prompts.get('channel','')}
Article instruction:
{prompts.get('article','')}
If no qualifying item exists, return exactly: FALSE
Otherwise return ONLY valid JSON with fields: title, article_url, published_at, image_url, score, research_text, resource_links, facts.
research_text must contain rich factual material actually retrieved. Do not invent facts.
"""

    if progress:
        await progress("scout", f"🌐 {source.get('name')}: WebScout در حال بررسی {url}…")

    scout = await ai.webscout_call(url, scout_prompt, max_tokens=8000)
    if not scout.get("ok"):
        stats["errors"] = 1
        stats["diagnostics"] = [str(scout.get("error") or "WebScout failed")]
        return stats

    raw = str(scout.get("content") or "").strip()
    if raw.upper() == "FALSE" or raw.upper().startswith("FALSE\n"):
        stats["diagnostics"] = ["WebScout: FALSE — موردی با معیارهای مدیر پیدا نشد"]
        return stats

    obj = parse_json_object(raw)
    if obj and obj.get("found") is False:
        stats["diagnostics"] = ["WebScout: FALSE — موردی با معیارهای مدیر پیدا نشد"]
        return stats

    if not obj or not obj.get("article_url") or not obj.get("research_text"):
        stats["errors"] = 1
        stats["diagnostics"] = ["WebScout پاسخ ساختاریافته و قابل استفاده نداد"]
        return stats

    pub = parse_publication_datetime(str(obj.get("published_at") or ""))
    if not pub:
        stats["rejected"] = 1
        stats["processed"] = 1
        stats["diagnostics"] = ["تاریخ انتشار قابل تأیید نبود"]
        return stats

    age = (datetime.now(timezone.utc) - pub).total_seconds() / 3600.0
    if age < -0.5 or age > freshness:
        stats["rejected"] = 1
        stats["processed"] = 1
        stats["diagnostics"] = [f"زمان انتشار خارج از پنجره بود: {age:.1f} ساعت"]
        return stats

    stats["found"] = 1
    stats["candidates"] = 1

    item = {
        "title": strip_html_text(str(obj.get("title") or ""))[:500],
        "url": normalize_url(str(obj.get("article_url") or url)),
        "description": "",
        "body": str(obj.get("research_text") or ""),
        "webscout_research": str(obj.get("research_text") or ""),
        "image_url": normalize_url(str(obj.get("image_url") or "")),
        "published_at": str(obj.get("published_at") or ""),
        "links": obj.get("resource_links") if isinstance(obj.get("resource_links"), list) else [],
        "webscout_score": float(obj.get("score") or 0),
    }

    insufficient, reason = is_insufficient_content(item["title"], item["body"], item.get("description", ""))
    if insufficient:
        stats["rejected"] = 1
        stats["processed"] = 1
        stats["diagnostics"] = [reason]
        await log_automation(db, "INFO", "content_rejected", f"{source.get('name')} | {reason}")
        return stats

    # Duplicate prevention
    content_hash = text_hash((item.get("title") or "") + " " + (item.get("body") or "")[:5000])
    dup = await db.execute(
        "SELECT id FROM articles WHERE source_url=? OR content_hash=? LIMIT 1",
        [item["url"], content_hash]
    )
    if dup:
        stats["rejected"] = 1
        stats["processed"] = 1
        stats["diagnostics"] = ["محتوا تکراری است"]
        return stats

    out = await ai_editorial_process(ai, item, source, recent_titles, weights, prompts)
    stats["processed"] = 1

    if out.get("error"):
        stats["errors"] = 1
        stats["diagnostics"] = [str(out.get("error"))]
        return stats

    if not manager_accepts_score(float(out.get("score", 0) or 0), min_score):
        stats["rejected"] = 1
        stats["diagnostics"] = [f"امتیاز نهایی {out.get('score','-')} کمتر از حد مدیر {min_score:g}"]
        await log_automation(db, "INFO", "content_rejected", f"{source.get('name')} | score={out.get('score')}")
        return stats

    now = datetime.now(timezone.utc).isoformat()
    art = await db.execute(
        "INSERT INTO articles(source_item_id,title,channel_text,body,source_url,image_url,category,score,status,created_at,source_published_at,content_hash) VALUES(NULL,?,?,?,?,?,?,?,'ready',?,?,?) RETURNING id",
        [
            out.get("title") or item["title"],
            out.get("channel_html") or out.get("channel_text") or "",
            out.get("article_html") or out.get("article_text") or "",
            item["url"],
            item.get("image_url") or "",
            out.get("category") or source.get("category", "tech"),
            float(out.get("score") or 0),
            now,
            item.get("published_at", "")[:100],
            content_hash,
        ]
    )
    aid = int(art[0]["id"]) if art else 0
    if not aid:
        raise RuntimeError("ذخیره مقاله ناموفق بود")

    token = make_deep_token(aid)
    await db.execute("UPDATE articles SET deep_token=? WHERE id=?", [token, aid])

    scheduled_at = now
    await db.execute(
        "INSERT INTO publication_queue(article_id,scheduled_at,status,attempts,last_error,created_at) VALUES(?,?, 'queued',0,NULL,?)",
        [aid, scheduled_at, now]
    )

    stats["accepted"] = 1
    stats["queued"] = 1
    stats["article_id"] = aid
    stats["scheduled_at"] = scheduled_at
    stats["provider"] = scout.get("provider")
    stats["model"] = scout.get("model")
    stats["interval_minutes"] = int(
        await get_setting(db, "webscout_success_interval_minutes", str(WEBSCOUT_SUCCESS_INTERVAL_MINUTES)) or WEBSCOUT_SUCCESS_INTERVAL_MINUTES
    )
    stats["diagnostics"] = [f"WebScout: TRUE · {item['title'][:120]}"]
    return stats

# ============================================================
# Publication
# ============================================================
async def get_runtime_bot_username(bot: Bot) -> str:
    global BOT_USERNAME_RUNTIME
    if BOT_USERNAME_RUNTIME:
        return BOT_USERNAME_RUNTIME
    try:
        me = await bot.get_me()
        BOT_USERNAME_RUNTIME = me.username or BOT_USERNAME.lstrip("@")
    except Exception:
        BOT_USERNAME_RUNTIME = BOT_USERNAME.lstrip("@")
    return BOT_USERNAME_RUNTIME

def source_is_due(s: Dict[str, Any], now_dt: datetime) -> bool:
    raw = s.get("next_check_at") or ""
    if not raw:
        return True
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")) <= now_dt
    except Exception:
        return True

async def recover_publication_queue(db: D1Database):
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
    cutoff_failed = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    try:
        await db.execute(
            "UPDATE publication_queue SET status='queued', last_error='recovered' WHERE status='publishing' AND COALESCE(last_attempt_at, created_at) < ?",
            [cutoff]
        )
        await db.execute(
            "UPDATE publication_queue SET status='queued', last_error='retry' WHERE status='failed' AND attempts < 3 AND created_at > ?",
            [cutoff_failed]
        )
    except Exception:
        logger.exception("recover_publication_queue failed")

async def can_publish_now(db: D1Database) -> bool:
    if not await get_channel_id(db):
        return False

    enabled = await get_setting(db, "automation_enabled", "0")
    if enabled != "1":
        return False

    tehran = datetime.now(pytz.timezone("Asia/Tehran"))
    start_h = int(await get_setting(db, "publish_start_hour", str(DEFAULT_PUBLISH_START_HOUR)))
    end_h = int(await get_setting(db, "publish_end_hour", str(DEFAULT_PUBLISH_END_HOUR)))

    if start_h <= end_h:
        in_window = start_h <= tehran.hour <= end_h
    else:
        in_window = tehran.hour >= start_h or tehran.hour <= end_h
    if not in_window:
        return False

    day_start = tehran.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc).isoformat()
    count_rows = await db.execute(
        "SELECT COUNT(*) as c FROM articles WHERE status='published' AND COALESCE(published_at,created_at) >= ?",
        [day_start]
    )
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
            min_gap = float(await get_setting(db, "min_post_gap_minutes", str(DEFAULT_MIN_POST_GAP_MINUTES)))
            if delta.total_seconds() < min_gap * 60:
                return False
        except Exception:
            pass

    return True

async def publish_next_article(db: D1Database, bot: Bot, force: bool = False) -> bool:
    global LAST_RECOVER
    async with PUBLISH_LOCK:
        if time.time() - LAST_RECOVER > 600:
            await recover_publication_queue(db)
            LAST_RECOVER = time.time()

        if force:
            channel_id = await get_channel_id(db)
            if not channel_id:
                return False
            max_daily = int(await get_setting(db, "max_daily_posts", str(DEFAULT_MAX_DAILY_POSTS)))
            tehran = datetime.now(pytz.timezone("Asia/Tehran"))
            day_start = tehran.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc).isoformat()
            count_rows = await db.execute(
                "SELECT COUNT(*) c FROM articles WHERE status='published' AND COALESCE(published_at,created_at) >= ?",
                [day_start]
            )
            if (count_rows[0].get("c", 0) if count_rows else 0) >= max_daily:
                return False
        elif not await can_publish_now(db):
            return False

        now_iso = datetime.now(timezone.utc).isoformat()
        schedule_filter = "" if force else " AND (q.scheduled_at IS NULL OR q.scheduled_at <= ?)"
        params = [now_iso] if not force else []

        rows = await db.execute(
            "SELECT q.id as queue_id, q.article_id, a.* FROM publication_queue q JOIN articles a ON a.id=q.article_id "
            "WHERE q.status='queued' AND a.status='ready'" + schedule_filter +
            " ORDER BY COALESCE(a.source_published_at,a.created_at) DESC, a.score DESC, q.created_at ASC LIMIT 1",
            params
        )
        if not rows:
            return False

        row = rows[0]
        queue_id = row["queue_id"]
        article_id = row["article_id"]

        await db.execute(
            "UPDATE publication_queue SET status='publishing', attempts=attempts+1, last_attempt_at=? WHERE id=? AND status='queued'",
            [now_iso, queue_id]
        )

        try:
            token = row.get("deep_token")
            bot_username = await get_runtime_bot_username(bot)
            if not token or not bot_username:
                raise RuntimeError("deep link token یا نام کاربری ربات تنظیم نشده است")

            deep_link = f"https://t.me/{bot_username}?start=article_{token}"
            channel_id = await get_channel_id(db)
            title_out = str(row.get("title") or "مطلب")
            channel_text = sanitize_telegram_html(row.get("channel_text") or "")
            if not strip_html_text(channel_text):
                channel_text = f"<b>{html.escape(title_out[:200])}</b>\n{html.escape(strip_html_text(row.get('body') or '')[:400], quote=False)}"

            image_url = normalize_url(row.get("image_url") or "")
            caption = publication_caption(title_out, channel_text, deep_link)
            sent = None

            if image_url:
                try:
                    sent = await bot.send_photo(
                        chat_id=channel_id,
                        photo=image_url,
                        caption=caption,
                        parse_mode="HTML"
                    )
                except Exception as img_error:
                    await log_automation(db, "WARN", "source_image_failed", f"article={article_id} {img_error}")

            if sent is None:
                try:
                    sent = await bot.send_message(
                        chat_id=channel_id,
                        text=caption,
                        parse_mode="HTML",
                        disable_web_page_preview=True
                    )
                except Exception:
                    plain = strip_html_text(channel_text)[:3500] + f"\n{deep_link}"
                    sent = await bot.send_message(
                        chat_id=channel_id,
                        text=plain,
                        disable_web_page_preview=True
                    )

            published_at = datetime.now(timezone.utc).isoformat()
            await db.execute(
                "UPDATE articles SET status='published', published_message_id=?, published_at=?, image_url='' WHERE id=?",
                [getattr(sent, "message_id", 0), published_at, article_id]
            )
            await db.execute(
                "UPDATE publication_queue SET status='published', published_at=? WHERE id=?",
                [published_at, queue_id]
            )
            await log_automation(db, "INFO", "published", f"article={article_id} message={getattr(sent, 'message_id', 0)} force={force}")
            return True

        except Exception as e:
            await db.execute(
                "UPDATE publication_queue SET status='failed', last_error=? WHERE id=?",
                [str(e)[:1200], queue_id]
            )
            await db.execute("UPDATE articles SET status='ready' WHERE id=?", [article_id])
            await log_automation(db, "ERROR", "publication_failed", f"article={article_id} {e}")
            try:
                if ADMIN_ID:
                    await bot.send_message(ADMIN_ID, f"❌ خطا در انتشار خودکار\nArticle: {article_id}\nError: {html.escape(str(e)[:700])}")
            except Exception:
                pass
            return False

async def recheck_failed_providers(db: D1Database, bot: Bot, manager: AIProviderManager):
    now = datetime.now(timezone.utc).isoformat()
    rows = await db.execute(
        "SELECT id,name,model_name,status,cooldown_until FROM ai_providers WHERE enabled=1 AND status IN ('invalid','cooldown') AND (cooldown_until IS NULL OR cooldown_until <= ?) ORDER BY priority ASC LIMIT 5",
        [now]
    )
    for p in rows:
        try:
            result = await manager.test_provider(int(p["id"]))
            if result.get("ok") and ADMIN_ID:
                try:
                    await bot.send_message(
                        ADMIN_ID,
                        f"✅ <b>مدل دوباره در دسترس است</b>\nProvider: {html.escape(str(p.get('name')))}\nModel: <code>{html.escape(str(p.get('model_name')))}</code>",
                        parse_mode="HTML"
                    )
                except Exception:
                    pass
        except Exception as e:
            await log_automation(db, "ERROR", "provider_recheck_failed", f"provider={p.get('id')} {e}")

# ============================================================
# Automation loop
# ============================================================
async def update_source_after_check(db: D1Database, source: Dict[str, Any], interval_minutes: int, error: Optional[str] = None):
    now = datetime.now(timezone.utc)
    next_check = (now + timedelta(minutes=max(1, int(interval_minutes)))).isoformat()
    try:
        await db.execute(
            "UPDATE sources SET last_checked_at=?, next_check_at=?, last_error=? WHERE id=?",
            [now.isoformat(), next_check, error, source.get("id")]
        )
        invalidate_sources()
    except Exception:
        logger.exception("update source after check failed")

async def automation_loop(db: D1Database, bot: Bot):
    ai = AIProviderManager(db, bot)
    await set_setting(db, "worker_started_at", datetime.now(timezone.utc).isoformat())

    last_cleanup = 0.0
    last_provider_recheck = 0.0
    last_heartbeat = 0.0
    last_publish_try = 0.0
    last_recover = 0.0
    cursor = 0

    try:
        while True:
            now_dt = datetime.now(timezone.utc)
            try:
                if time.time() - last_heartbeat >= WEBSCOUT_HEARTBEAT_SECONDS:
                    await set_setting(db, "worker_heartbeat_at", now_dt.isoformat())
                    last_heartbeat = time.time()

                enabled = await get_setting(db, "automation_enabled", "0")
                if enabled == "1":
                    # Queue recovery and publication are independent of source due time.
                    if time.time() - last_recover > 600:
                        await recover_publication_queue(db)
                        last_recover = time.time()

                    if time.time() - last_publish_try >= PUBLISH_ATTEMPT_INTERVAL:
                        last_publish_try = time.time()
                        await publish_next_article(db, bot)

                    next_run_raw = await get_setting(db, "webscout_next_run_at", "")
                    due = True
                    if next_run_raw:
                        try:
                            due = datetime.fromisoformat(next_run_raw.replace("Z", "+00:00")) <= now_dt
                        except Exception:
                            due = True

                    if due:
                        await set_setting(db, "last_cycle_started_at", now_dt.isoformat())

                        rows = await get_enabled_sources(db)
                        due_sources = [s for s in rows if source_is_due(s, now_dt)]

                        if due_sources:
                            ordered = [due_sources[(cursor + i) % len(due_sources)] for i in range(len(due_sources))]
                        else:
                            ordered = []

                        results = []
                        success = None
                        empty_retry = max(1, int(await get_setting(db, "webscout_empty_retry_minutes", str(WEBSCOUT_EMPTY_RETRY_MINUTES)) or WEBSCOUT_EMPTY_RETRY_MINUTES))

                        for idx, src in enumerate(ordered):
                            cursor = (cursor + 1) % len(due_sources) if due_sources else 0
                            try:
                                r = await fetch_source_cycle(db, src, ai)
                                results.append(r)
                                interval = r.get("interval_minutes") if r.get("accepted") else max(int(src.get("interval_minutes") or empty_retry), empty_retry)
                                await update_source_after_check(db, src, interval, None)
                                if r.get("accepted"):
                                    success = r
                                    break
                            except Exception as exc:
                                r = {"errors": 1, "found": 0, "candidates": 0, "processed": 0, "accepted": 0, "queued": 0, "rejected": 0, "diagnostics": [str(exc)[:250]]}
                                results.append(r)
                                await update_source_after_check(db, src, empty_retry, str(exc)[:500])
                                global LAST_SOURCE_ERROR_NOTICE
                                if ai.bot and ADMIN_ID and time.time() - LAST_SOURCE_ERROR_NOTICE > 1800:
                                    LAST_SOURCE_ERROR_NOTICE = time.time()
                                    try:
                                        await ai.bot.send_message(
                                            ADMIN_ID,
                                            f"❌ <b>WebScout source error</b>\n{html.escape(str(src.get('name') or src.get('url')))}\n<code>{html.escape(str(exc)[:500])}</code>",
                                            parse_mode="HTML"
                                        )
                                    except Exception:
                                        pass

                        end_now = datetime.now(timezone.utc)
                        if success:
                            wait_minutes = max(1, int(success.get("interval_minutes") or await get_setting(db, "webscout_success_interval_minutes", str(WEBSCOUT_SUCCESS_INTERVAL_MINUTES)) or WEBSCOUT_SUCCESS_INTERVAL_MINUTES))
                            await set_setting(db, "webscout_next_run_at", (end_now + timedelta(minutes=wait_minutes)).isoformat())
                        else:
                            await set_setting(db, "webscout_next_run_at", (end_now + timedelta(minutes=empty_retry)).isoformat())

                        summary = {
                            "sources_checked": len(results),
                            "processed": sum((r.get("processed", 0) if isinstance(r, dict) else 0) for r in results),
                            "accepted": sum((r.get("accepted", 0) if isinstance(r, dict) else 0) for r in results),
                            "rejected": sum((r.get("rejected", 0) if isinstance(r, dict) else 0) for r in results),
                            "errors": sum((r.get("errors", 0) if isinstance(r, dict) else 0) for r in results),
                            "queued": sum((r.get("queued", 0) if isinstance(r, dict) else 0) for r in results),
                            "published": False,
                            "mode": "webscout"
                        }

                        published = await publish_next_article(db, bot)
                        summary["published"] = bool(published)
                        last_publish_try = time.time()

                        await set_setting(db, "last_cycle_result", json.dumps(summary, ensure_ascii=False))
                        await set_setting(db, "last_cycle_finished_at", datetime.now(timezone.utc).isoformat())

                    if time.time() - last_provider_recheck > AI_PROVIDER_RECHECK_MINUTES * 60:
                        await recheck_failed_providers(db, bot, ai)
                        last_provider_recheck = time.time()

                    if time.time() - last_cleanup > AUTOMATION_CLEANUP_INTERVAL_SECONDS:
                        await cleanup_automation_data(db)
                        last_cleanup = time.time()

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.exception("automation loop error")
                await log_automation(db, "ERROR", "automation_loop_failed", str(e)[:1200])
                await set_setting(db, "last_cycle_result", json.dumps({"error": str(e)[:800]}, ensure_ascii=False))

            await asyncio.sleep(WEBSCOUT_LOOP_SLEEP_SECONDS)
    finally:
        await ai.close()

# ============================================================
# Report helpers
# ============================================================
def format_duration_minutes(value) -> str:
    try:
        m = max(0, int(float(value)))
    except Exception:
        m = 0
    if m < 60:
        return f"{m} دقیقه"
    h = m // 60
    rem = m % 60
    return f"{h} ساعت" if rem == 0 else f"{h} ساعت و {rem} دقیقه"

async def next_publication_estimate(db: D1Database) -> Dict[str, Any]:
    now = datetime.now(timezone.utc)
    last_manual = await get_setting(db, "last_manual_channel_post_at", "")
    last_pub = await db.execute("SELECT published_at FROM publication_queue WHERE status='published' AND published_at IS NOT NULL ORDER BY id DESC LIMIT 1")
    candidates = [x for x in [last_manual, last_pub[0].get("published_at") if last_pub else ""] if x]

    latest = None
    for raw in candidates:
        try:
            dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
            if latest is None or dt > latest:
                latest = dt
        except Exception:
            pass

    interval_minutes = float(await get_setting(db, "min_post_gap_minutes", str(DEFAULT_MIN_POST_GAP_MINUTES)))
    target = max(now, latest + timedelta(minutes=interval_minutes) if latest else now)
    queued = await db.execute("SELECT COUNT(*) c FROM publication_queue WHERE status='queued'")
    return {
        "target": target,
        "minutes": max(0, int((target - now).total_seconds() / 60)) if target > now else 0,
        "latest": latest,
        "interval_minutes": int(interval_minutes),
        "queued": int(queued[0].get("c", 0)) if queued else 0,
    }

async def automation_report(db: D1Database) -> str:
    settings = {
        "enabled": await get_setting(db, "automation_enabled", "0"),
        "max_daily": await get_setting(db, "max_daily_posts", str(DEFAULT_MAX_DAILY_POSTS)),
        "min_score": await get_setting(db, "min_content_score", str(DEFAULT_MIN_CONTENT_SCORE)),
        "source_interval": await get_setting(db, "default_source_interval", str(DEFAULT_SOURCE_INTERVAL_MINUTES)),
        "publish_gap": await get_setting(db, "min_post_gap_minutes", str(DEFAULT_MIN_POST_GAP_MINUTES)),
    }

    sources = await db.execute("SELECT COUNT(*) c FROM sources WHERE enabled=1")
    queued = await db.execute("SELECT COUNT(*) c FROM publication_queue WHERE status='queued'")
    ready = await db.execute("SELECT COUNT(*) c FROM articles WHERE status='ready'")
    failed = await db.execute("SELECT COUNT(*) c FROM publication_queue WHERE status='failed' AND created_at>=?", [(datetime.now(timezone.utc) - timedelta(days=1)).isoformat()])

    tehran = datetime.now(pytz.timezone("Asia/Tehran"))
    day_start = tehran.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc).isoformat()
    published = await db.execute("SELECT COUNT(*) c FROM articles WHERE status='published' AND COALESCE(published_at,created_at)>=?", [day_start])
    discovered = await db.execute("SELECT COUNT(*) c FROM articles WHERE created_at>=?", [(datetime.now(timezone.utc) - timedelta(days=1)).isoformat()])

    channel = await get_channel_id(db)
    channel_label = await get_setting(db, "channel_username", "") or ("کانال خصوصی تنظیم شده" if channel else "تنظیم نشده")
    hb = await get_setting(db, "worker_heartbeat_at", "")
    last_cycle = await get_setting(db, "last_cycle_finished_at", "")
    last_started = await get_setting(db, "last_cycle_started_at", "")
    result_raw = await get_setting(db, "last_cycle_result", "")

    hb_seconds = None
    if hb:
        try:
            hb_seconds = int((datetime.now(timezone.utc) - datetime.fromisoformat(hb.replace("Z", "+00:00"))).total_seconds())
        except Exception:
            hb_seconds = None

    result_line = "هنوز گزارشی ثبت نشده"
    if result_raw:
        try:
            obj = json.loads(result_raw)
            result_line = (
                f"منابع: {obj.get('sources_checked', 0)} · پردازش: {obj.get('processed', 0)} · "
                f"قبول: {obj.get('accepted', 0)} · صف: {obj.get('queued', 0)} · "
                f"انتشار: {'بله ✅' if obj.get('published') else 'خیر ⏸'}"
            )
            if obj.get("error"):
                result_line = f"خطا: {obj.get('error')}"
        except Exception:
            result_line = "آخرین نتیجه قابل نمایش نیست"

    hb_label = "نامشخص"
    if hb_seconds is not None:
        hb_label = f"{hb_seconds} ثانیه قبل"
        if hb_seconds < 300:
            hb_label += " 🟢"
        elif hb_seconds < 900:
            hb_label += " 🟡"
        else:
            hb_label += " 🔴"

    return (
        "📊 <b>گزارش اتوماسیون</b>\n"
        f"{'🟢' if settings['enabled'] == '1' else '🔴'} وضعیت: <b>{'فعال' if settings['enabled'] == '1' else 'خاموش'}</b>\n"
        f"📢 کانال: <b>{html.escape(channel_label)}</b>\n"
        f"🌐 منابع فعال: <b>{sources[0].get('c', 0) if sources else 0}</b>\n"
        f"📰 کشف/ثبت در ۲۴ ساعت: <b>{discovered[0].get('c', 0) if discovered else 0}</b>\n"
        f"📥 صف فعلی: <b>{queued[0].get('c', 0) if queued else 0}</b>\n"
        f"📝 آماده در آرشیو: <b>{ready[0].get('c', 0) if ready else 0}</b>\n"
        f"📢 منتشرشده امروز: <b>{published[0].get('c', 0) if published else 0}/{settings['max_daily']}</b>\n"
        f"❌ انتشار ناموفق ۲۴ ساعت: <b>{failed[0].get('c', 0) if failed else 0}</b>\n"
        f"⭐ حداقل امتیاز: <b>{settings['min_score']}</b>\n"
        f"⏱ فاصله بررسی منابع: <b>{settings['source_interval']} دقیقه</b>\n"
        f"📢 فاصله انتشار: <b>{format_duration_minutes(settings['publish_gap'])}</b>\n"
        f"💓 Heartbeat: <b>{hb_label}</b>\n"
        f"🕐 آخرین شروع چرخه: <b>{html.escape(last_started or 'هنوز اجرا نشده')}</b>\n"
        f"✅ آخرین پایان چرخه: <b>{html.escape(last_cycle or 'هنوز اجرا نشده')}</b>\n"
        f"📋 آخرین نتیجه: <b>{html.escape(result_line)}</b>"
    )

async def automation_overview(db: D1Database) -> str:
    enabled = (await get_setting(db, "automation_enabled", "0")) == "1"
    sources = await db.execute("SELECT COUNT(*) c FROM sources WHERE enabled=1")
    queued = await db.execute("SELECT COUNT(*) c FROM publication_queue WHERE status='queued'")
    max_daily = await get_setting(db, "max_daily_posts", str(DEFAULT_MAX_DAILY_POSTS))
    gap = await get_setting(db, "min_post_gap_minutes", str(DEFAULT_MIN_POST_GAP_MINUTES))
    interval = await get_setting(db, "default_source_interval", str(DEFAULT_SOURCE_INTERVAL_MINUTES))

    return (
        "📰 <b>اتوماسیون محتوا</b>\n"
        f"🤖 وضعیت: <b>{'🟢 فعال' if enabled else '🔴 خاموش'}</b>\n"
        f"🌐 منابع فعال: <b>{sources[0].get('c', 0) if sources else 0}</b>\n"
        f"📥 صف فعلی: <b>{queued[0].get('c', 0) if queued else 0}</b>\n"
        f"🔢 سقف روزانه: <b>{max_daily}</b>\n"
        f"⏱ فاصله انتشار: <b>{format_duration_minutes(gap)}</b>\n"
        f"🌐 فاصله بررسی منابع: <b>{interval} دقیقه</b>\n"
        "ℹ️ گزارش کامل فقط از دکمه «📊 گزارش» نمایش داده می‌شود."
    )

# ============================================================
# States
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
    admin_post_edit = State()
    user_search_folder = State()
    admin_add_source = State()
    admin_add_provider = State()
    admin_provider_token = State()
    admin_provider_model = State()
    admin_channel_input = State()
    admin_automation_setting = State()
    automation_article_edit = State()
    admin_view_all = State()

# ============================================================
# Keyboards
# ============================================================
FOLDER_NAMES = {
    "cyber": "🔒 امنیت سایبری",
    "tech": "💻 تکنولوژی و فناوری",
    "ai": "🧠 هوش مصنوعی",
    "edu": "📚 آموزش",
}

def get_main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💾 ذخیره‌های من", callback_data="user_saves"), InlineKeyboardButton(text="👤 پروفایل", callback_data="user_profile")],
        [InlineKeyboardButton(text="🤖 چت هوش مصنوعی", callback_data="ai_chat_start"), InlineKeyboardButton(text="❓ راهنما", callback_data="user_help")],
    ])

def get_admin_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📰 اتوماسیون محتوا", callback_data="admin_automation")],
        [InlineKeyboardButton(text="📁 مدیریت محتوای هسته", callback_data="admin_content")],
        [InlineKeyboardButton(text="📢 ارسال همگانی", callback_data="admin_broadcast"), InlineKeyboardButton(text="➕ افزودن پست", callback_data="admin_add_post")],
        [InlineKeyboardButton(text="📊 آمار کلی", callback_data="admin_stats")],
        [InlineKeyboardButton(text="👤 حالت کاربری", callback_data="admin_user_mode")],
    ])

def get_admin_back_kb(target="admin_home") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 بازگشت", callback_data=target)]])

def get_exit_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ لغو و بازگشت", callback_data="cancel_state")]])

def unified_saved_kb(folder: str = "all") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=FOLDER_NAMES["tech"], callback_data="saved_folder_tech"), InlineKeyboardButton(text=FOLDER_NAMES["ai"], callback_data="saved_folder_ai")],
        [InlineKeyboardButton(text=FOLDER_NAMES["cyber"], callback_data="saved_folder_cyber"), InlineKeyboardButton(text=FOLDER_NAMES["edu"], callback_data="saved_folder_edu")],
        [InlineKeyboardButton(text="🗂 همه", callback_data="saved_folder_all")],
        [InlineKeyboardButton(text="🏠 منوی اصلی", callback_data="user_home")],
    ])

def get_post_inline_kb(post_id: int, likes: int, dislikes: int, is_saved: bool) -> InlineKeyboardMarkup:
    save_text = "❌ حذف از ذخیره‌ها" if is_saved else "💾 ذخیره"
    save_cb = f"unsave_{post_id}" if is_saved else f"save_{post_id}"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"👍 {likes}", callback_data=f"like_{post_id}"), InlineKeyboardButton(text=f"👎 {dislikes}", callback_data=f"dis_{post_id}")],
        [InlineKeyboardButton(text=save_text, callback_data=save_cb)],
        [InlineKeyboardButton(text="❓ راهنما", callback_data="user_help"), InlineKeyboardButton(text="🏠 منوی اصلی", callback_data="user_home")],
    ])

def get_article_inline_kb(article_id: int, likes: int, dislikes: int, is_saved: bool) -> InlineKeyboardMarkup:
    save_text = "❌ حذف از ذخیره‌ها" if is_saved else "💾 ذخیره"
    save_cb = f"aunsave_{article_id}" if is_saved else f"asave_{article_id}"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"👍 {likes}", callback_data=f"alike_{article_id}"), InlineKeyboardButton(text=f"👎 {dislikes}", callback_data=f"adis_{article_id}")],
        [InlineKeyboardButton(text=save_text, callback_data=save_cb)],
        [InlineKeyboardButton(text="🏠 منوی اصلی", callback_data="user_home")],
    ])

def get_search_pagination_kb(folder: str, index: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏮ قبلی", callback_data=f"srchpg_prev_{folder}_{index}"), InlineKeyboardButton(text="⏭ بعدی", callback_data=f"srchpg_next_{folder}_{index}")],
        [InlineKeyboardButton(text="🔍 جستجوی مجدد", callback_data=f"f_srch_{folder}")],
    ])

def get_confirm_add_post_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ بله، ثبتش کن!", callback_data="conf_add_yes"), InlineKeyboardButton(text="❌ خیر، بیخیال شو", callback_data="conf_add_no")]
    ])

def get_confirm_broadcast_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚀 بله، ارسال همگانی شود!", callback_data="conf_broad_yes"), InlineKeyboardButton(text="❌ لغو", callback_data="conf_broad_no")]
    ])

def get_content_management_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 جستجو", callback_data="adm_search_text"), InlineKeyboardButton(text="📋 همه محتواها", callback_data="adm_view_all")],
        [InlineKeyboardButton(text="🔙 بازگشت", callback_data="admin_home")],
    ])

def get_admin_search_pagination_kb(post_id: int, index: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏮ قبلی", callback_data=f"asearch_prev_{index}"), InlineKeyboardButton(text="⏭ بعدی", callback_data=f"asearch_next_{index}")],
        [InlineKeyboardButton(text="✏️ ویرایش", callback_data=f"aedit_{post_id}"), InlineKeyboardButton(text="📊 آمار", callback_data=f"astats_{post_id}")],
        [InlineKeyboardButton(text="🗑️ حذف", callback_data=f"adelete_{post_id}"), InlineKeyboardButton(text="🔙 مدیریت محتوا", callback_data="admin_content")],
    ])

def automation_menu_kb(enabled: bool) -> InlineKeyboardMarkup:
    state_text = "⏸ خاموش کردن اتوماسیون" if enabled else "▶️ روشن کردن اتوماسیون"
    state_cb = "auto_off" if enabled else "auto_on"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=state_text, callback_data=state_cb)],
        [InlineKeyboardButton(text="🌐 منابع خبری", callback_data="auto_sources"), InlineKeyboardButton(text="🤖 مدل‌های AI", callback_data="auto_providers")],
        [InlineKeyboardButton(text="📢 انتشار و زمان‌بندی", callback_data="auto_channel"), InlineKeyboardButton(text="🧠 کیفیت محتوا", callback_data="auto_quality")],
        [InlineKeyboardButton(text="🗃 محتوا و داده‌ها", callback_data="auto_content_db")],
        [InlineKeyboardButton(text="🧪 تست و سلامت", callback_data="auto_health"), InlineKeyboardButton(text="📊 گزارش", callback_data="auto_report")],
        [InlineKeyboardButton(text="🔙 پنل اصلی", callback_data="admin_home")],
    ])

def source_list_kb(sources: List[Dict[str, Any]]) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text="➕ افزودن منبع", callback_data="auto_add_source")]]
    for s in sources[:20]:
        mark = "🟢" if s.get("enabled") else "🔴"
        rows.append([InlineKeyboardButton(text=f"{mark} {s.get('name', 'source')[:35]}", callback_data=f"source_view_{s['id']}")])
    rows.append([InlineKeyboardButton(text="🔙 بازگشت", callback_data="auto_back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def provider_list_kb(providers: List[Dict[str, Any]]) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="➕ افزودن مدل جدید", callback_data="auto_add_provider")],
        [InlineKeyboardButton(text="ℹ️ راهنمای مدیریت مدل‌ها", callback_data="provider_help")],
    ]
    for p in providers[:20]:
        status = p.get("status") or "unknown"
        mark = {"healthy": "🟢", "invalid": "🔴", "cooldown": "🟡"}.get(status, "⚪")
        enabled_txt = "فعال" if p.get("enabled") else "خاموش"
        webmark = "🌐" if p.get("web_enabled") else ""
        label = f"{mark} #{p['id']} {webmark} {str(p.get('model_name', 'model'))[:30]} · {enabled_txt}"
        rows.append([InlineKeyboardButton(text=label, callback_data=f"provider_view_{p['id']}")])
    rows.append([InlineKeyboardButton(text="🔙 اتوماسیون محتوا", callback_data="auto_back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def quality_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⭐ حداقل امتیاز انتشار", callback_data="set_min_score")],
        [InlineKeyboardButton(text="🎯 وزن معیارهای محتوا", callback_data="quality_weights")],
        [InlineKeyboardButton(text="✍️ دستورهای تولید محتوا", callback_data="editorial_prompts")],
        [InlineKeyboardButton(text="🔙 اتوماسیون محتوا", callback_data="auto_back")],
    ])

def schedule_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔢 سقف تقریبی پست روزانه", callback_data="set_max_daily")],
        [InlineKeyboardButton(text="⏱ حداقل فاصله پست‌ها", callback_data="set_min_gap")],
        [InlineKeyboardButton(text="🌐 فاصله بررسی پیش‌فرض منابع", callback_data="set_default_interval")],
        [InlineKeyboardButton(text="🧭 فاصله WebScout بعد از موفقیت", callback_data="set_webscout_interval")],
        [InlineKeyboardButton(text="🚀 همین حالا منتشر کن", callback_data="publish_now")],
        [InlineKeyboardButton(text="🔙 اتوماسیون محتوا", callback_data="auto_back")],
    ])

def automation_content_db_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📥 مدیریت صف انتشار", callback_data="auto_queue")],
        [InlineKeyboardButton(text="📰 محتوای تولیدشده", callback_data="auto_articles")],
        [InlineKeyboardButton(text="🗄 آمار و پاکسازی دیتای اتوماسیون", callback_data="auto_db")],
        [InlineKeyboardButton(text="🔙 اتوماسیون محتوا", callback_data="auto_back")],
    ])

# ============================================================
# Middleware
# ============================================================
FUNNY_MESSAGES = [
    "آروم‌تر قهرمان! 🏎️",
    "دکمه‌ها گناه دارن، یواش‌تر! 🥺",
    "اسپم نکن مشتی، یکم استراحت کن ☕",
    "سرعتت زیاده! یواش‌تر بران 🛑",
    "آروم‌تر بکوب رو دکمه‌ها دوست من! 🛠️",
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
# Bot / Router
# ============================================================
router = Router()

async def register_user_if_not_exists(db: D1Database, user_id: int):
    await db.execute("INSERT OR IGNORE INTO users(id, joined_at) VALUES(?, ?)", [user_id, datetime.now(timezone.utc).isoformat()])

async def admin_ok(call: CallbackQuery) -> bool:
    if call.from_user.id != ADMIN_ID:
        try:
            await call.answer("دسترسی ندارید", show_alert=True)
        except Exception:
            pass
        return False
    return True

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
        logger.error("Error sending post content: %s", e)
        return None

async def send_article_content(bot: Bot, chat_id: int, article: dict, reply_markup=None):
    title = html.escape(str(article.get("title") or "مطلب"))
    body = sanitize_telegram_html(article.get("body") or "")
    body = remove_article_metadata_blocks(body)
    body = _remove_duplicate_title_from_body(article.get("title") or "", body)

    source_url = normalize_url(article.get("source_url") or "")
    if source_url and "منبع اصلی" not in strip_html_text(body):
        body = f"{body.rstrip()}\n<a href=\"{html.escape(source_url, quote=True)}\">منبع اصلی</a>"

    relative = relative_time_label(article.get("source_published_at") or article.get("published_at") or "")
    if relative != "زمان نامشخص":
        body = body.rstrip() + f"\n<i>⏱ {relative}</i>"

    full = f"<b>📖 {title}</b>\n{body}"
    chunks = split_html_safe(full, 3800)
    if not chunks:
        chunks = [f"<b>📖 {title}</b>"]

    for i, chunk in enumerate(chunks):
        kb = reply_markup if i == len(chunks) - 1 else None
        try:
            await bot.send_message(chat_id, chunk, parse_mode="HTML", disable_web_page_preview=True, reply_markup=kb)
        except Exception:
            plain = strip_html_text(chunk)[:3800]
            await bot.send_message(chat_id, plain, disable_web_page_preview=True, reply_markup=kb)

async def deliver_article_by_token(message: Message, bot: Bot, db: D1Database, token: str) -> bool:
    token = (token or "").strip()
    if token.startswith(("auto_", "article_")):
        token = token.split("_", 1)[1]

    if not re.fullmatch(r"[A-Za-z0-9_-]{6,64}", token):
        return False

    rows = await db.execute("SELECT * FROM articles WHERE deep_token=? AND status IN ('ready','published','test')", [token])
    if not rows:
        return False

    article = rows[0]
    article_id = int(article.get("id") or 0)

    try:
        await db.execute("UPDATE articles SET deep_views=COALESCE(deep_views,0)+1 WHERE id=?", [article_id])
    except Exception:
        pass

    like_rows = await db.execute(
        "SELECT COUNT(*) c FROM user_content_votes WHERE content_type='article' AND content_id=? AND vote_type='like'",
        [article_id]
    )
    dislike_rows = await db.execute(
        "SELECT COUNT(*) c FROM user_content_votes WHERE content_type='article' AND content_id=? AND vote_type='dislike'",
        [article_id]
    )
    save_rows = await db.execute(
        "SELECT folder FROM user_content_saves WHERE user_id=? AND content_type='article' AND content_id=?",
        [message.from_user.id, article_id]
    )

    kb = get_article_inline_kb(
        article_id,
        like_rows[0].get("c", 0) if like_rows else 0,
        dislike_rows[0].get("c", 0) if dislike_rows else 0,
        bool(save_rows)
    )

    image = normalize_url(article.get("image_url") or "")
    if image:
        try:
            photo_caption = f"<b>📖 {html.escape(str(article.get('title') or 'مطلب'))}</b>"
            relative = relative_time_label(article.get("source_published_at") or article.get("published_at") or "")
            if relative != "زمان نامشخص":
                photo_caption += f"\n<i>⏱ {relative}</i>"
            await bot.send_photo(message.chat.id, photo=image, caption=photo_caption, parse_mode="HTML")
        except Exception:
            pass

    await send_article_content(bot, message.chat.id, article, kb)
    return True

async def _render_unified_saves(call: CallbackQuery, db: D1Database, folder: str = "all"):
    uid = call.from_user.id
    folder_clause = ""
    base = [uid]
    if folder != "all":
        folder_clause = " AND s.folder=?"
        base.append(folder)

    posts = await db.execute(
        f"SELECT p.id, p.text, s.folder FROM user_content_saves s JOIN posts p ON p.id=s.content_id AND s.content_type='post' WHERE s.user_id=? AND p.deleted=0{folder_clause} ORDER BY COALESCE(s.created_at,'') DESC, s.rowid DESC LIMIT 30",
        base
    )
    articles = await db.execute(
        f"SELECT a.id, a.title, a.deep_token, s.folder FROM user_content_saves s JOIN articles a ON a.id=s.content_id AND s.content_type='article' WHERE s.user_id=? AND a.status IN ('ready','published','test'){folder_clause} ORDER BY COALESCE(s.created_at,'') DESC, s.rowid DESC LIMIT 30",
        base
    )

    items = []
    for r in posts:
        txt = strip_html_text(r.get("text") or "").strip().replace("\n", " ")
        items.append((int(r.get("id") or 0), "post", r.get("folder") or "", txt[:80], f"https://t.me/{BOT_USERNAME_RUNTIME or BOT_USERNAME.lstrip('@')}?start={int(r.get('id') or 0)}"))

    for r in articles:
        title = strip_html_text(r.get("title") or "").strip().replace("\n", " ")
        items.append((int(r.get("id") or 0), "article", r.get("folder") or "", title[:90], f"https://t.me/{BOT_USERNAME_RUNTIME or BOT_USERNAME.lstrip('@')}?start=article_{r.get('deep_token', '')}"))

    items.sort(key=lambda x: x[0], reverse=True)
    items = items[:30]
    label = "همه" if folder == "all" else FOLDER_NAMES.get(folder, folder)

    if not items:
        text = f"💾 <b>ذخیره‌های من</b>\n📂 پوشه: <b>{html.escape(label)}</b>\nفعلاً مطلبی در این بخش ذخیره نکردی."
    else:
        lines = [f"💾 <b>ذخیره‌های من</b> — {html.escape(label)}\n"]
        for i, (_, ctype, _, title, url) in enumerate(items, 1):
            icon = "📰" if ctype == "article" else "📌"
            lines.append(f"{i}. {icon} <a href=\"{html.escape(url, quote=True)}\">{html.escape(title or 'مطلب بدون عنوان')}</a>")
        text = "\n".join(lines)

    await call.message.edit_text(text, parse_mode="HTML", disable_web_page_preview=True, reply_markup=unified_saved_kb(folder))
    await call.answer()

async def send_search_item(bot: Bot, chat_id: int, db: D1Database, item: dict, folder: str, index: int):
    kb = get_search_pagination_kb(folder, index)
    if item.get("t") == "p":
        rows = await db.execute("SELECT text, file_id, media_type FROM posts WHERE id=? AND deleted=0", [item["id"]])
        if rows:
            await send_post_content(bot, chat_id, rows[0], kb)
    else:
        rows = await db.execute("SELECT * FROM articles WHERE id=? AND status IN ('ready','published','test')", [item["id"]])
        if rows:
            await send_article_content(bot, chat_id, rows[0], kb)

async def reset_database(db: D1Database):
    queries = [
        {"sql": "DROP TABLE IF EXISTS users"},
        {"sql": "DROP TABLE IF EXISTS posts"},
        {"sql": "DROP TABLE IF EXISTS user_content_saves"},
        {"sql": "DROP TABLE IF EXISTS user_content_votes"},
        {"sql": "DROP TABLE IF EXISTS sources"},
        {"sql": "DROP TABLE IF EXISTS articles"},
        {"sql": "DROP TABLE IF EXISTS publication_queue"},
        {"sql": "DROP TABLE IF EXISTS ai_providers"},
        {"sql": "DROP TABLE IF EXISTS automation_settings"},
        {"sql": "DROP TABLE IF EXISTS automation_logs"},
        {"sql": "DROP TABLE IF EXISTS test_history"},
    ]
    await db.execute_batch(queries)
    SETTINGS_CACHE.clear()
    invalidate_sources()
    invalidate_providers()
    await initialize_database(db)
    await migrate_unified_user_interactions(db)
    await initialize_automation_database(db)

# ============================================================
# Basic commands
# ============================================================
@router.message(Command("help"))
async def cmd_help(message: Message):
    await message.answer(
        "🌐 /help • راهنما\n📞 /man • تماس با مدیر\n🚀 /start • شروع\n✨ انتخاب کن و شروع کن 🚀",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 منوی اصلی", callback_data="user_home")]])
    )

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
            ok = await deliver_article_by_token(message, bot, db, deep_arg)
            if not ok:
                await message.answer(
                    "❌ این لینک ادامه مطلب معتبر نیست یا مقاله دیگر در دسترس نیست.",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 منوی اصلی", callback_data="user_home")]])
                )
            return

        if deep_arg.isdigit():
            post_id = int(deep_arg)
            post_rows = await db.execute("SELECT text, file_id, media_type, likes, dislikes FROM posts WHERE id=? AND deleted=0", [post_id])
            if post_rows:
                post = post_rows[0]
                await db.execute("UPDATE posts SET views = views + 1 WHERE id=?", [post_id])
                save_rows = await db.execute(
                    "SELECT folder FROM user_content_saves WHERE user_id=? AND content_type='post' AND content_id=?",
                    [user_id, post_id]
                )
                is_saved = len(save_rows) > 0
                kb = get_post_inline_kb(post_id, post.get("likes", 0), post.get("dislikes", 0), is_saved)
                await send_post_content(bot, message.chat.id, post, kb)
                return
            else:
                await message.answer("❌ این پست یافت نشد یا حذف شده است.")
                return

    first_name = message.from_user.first_name or "دوست عزیز"
    welcomes = [
        f"سلام {first_name} عزیز! 👋 خیلی خوش اومدی. وقت کاوش تو دنیای تکنولوژیه! 🚀",
        f"درود {first_name}! 🌟 خوشحالیم که اینجایی. آماده‌ای برای مطالب جذاب؟ 📚",
        f"سلام {first_name} جان! 🤖 به پایگاه دانش ما خوش اومدی. بزن بریم که کلی مطلب خفن داریم! 🔥",
    ]
    welcome_text = random.choice(welcomes) + "\nاز دکمه های پایین استفاده کنید👇🏻"
    admin_mode = state_data.get("admin_mode", "user")
    menu = get_admin_menu() if (user_id == ADMIN_ID and admin_mode != "user") else get_main_menu()
    await message.answer(welcome_text, reply_markup=menu)

@router.message(Command("article"))
async def cmd_article(message: Message, db: D1Database, bot: Bot):
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("فرمت: /article TOKEN")
        return
    if not await deliver_article_by_token(message, bot, db, parts[1]):
        await message.answer("❌ مقاله پیدا نشد یا لینک منقضی شده است.")

@router.message(Command("setup_db"))
async def cmd_setup_db(message: Message, db: D1Database):
    if message.from_user.id == ADMIN_ID:
        try:
            await initialize_database(db)
            await migrate_unified_user_interactions(db)
            await initialize_automation_database(db)
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

# ============================================================
# FSM handlers
# ============================================================
@router.message(StateFilter(BotStates.ai_chat))
async def process_ai_chat(message: Message, state: FSMContext, db: D1Database, bot: Bot):
    user_id = message.from_user.id

    if message.text and message.text.strip() in {"/exit", "❌ خروج", "خروج"}:
        await state.set_state(BotStates.idle)
        await message.answer("🚪 از چت هوش مصنوعی خارج شدی.", reply_markup=get_main_menu())
        return

    providers = await db.execute("SELECT id FROM ai_providers WHERE enabled=1 AND status != 'invalid' LIMIT 1")
    if not providers:
        await message.answer("⚠️ هنوز هیچ مدل فعالی در پنل هوش مصنوعی تنظیم نشده است.")
        return

    today_tehran = get_tehran_date()
    user_rows = await db.execute("SELECT tokens_used, last_reset_date FROM users WHERE id=?", [user_id])
    tokens_used = 0
    last_reset = ""
    if user_rows:
        tokens_used = user_rows[0].get("tokens_used") or 0
        last_reset = user_rows[0].get("last_reset_date") or ""

    if last_reset != today_tehran:
        tokens_used = 0
        last_reset = today_tehran
        await db.execute("UPDATE users SET tokens_used=0, last_reset_date=? WHERE id=?", [today_tehran, user_id])

    if tokens_used >= 10000:
        await message.answer("⛔ سهمیه ۱۰۰۰۰ توکن شما برای امروز به پایان رسیده است.")
        return

    user_prompt = ""
    if message.text:
        user_prompt = message.text
    elif message.document:
        await message.answer("⏳ در حال خواندن فایل متنی شما...")
        try:
            file_info = await bot.get_file(message.document.file_id)
            dest = io.BytesIO()
            await bot.download_file(file_info.file_path, destination=dest)
            dest.seek(0)
            text = dest.read().decode("utf-8", errors="ignore")
            if len(text) > 12000:
                text = text[:12000] + "\n[فایل بزرگ بود؛ فقط بخشی بررسی شد]"
            caption = f"\nتوضیحات: {message.caption}" if message.caption else ""
            user_prompt = f"لطفاً این فایل را بررسی کن:\n```\n{text}\n```{caption}"
        except Exception as e:
            await message.answer(f"⚠️ خطا در خواندن فایل:\n{str(e)}")
            return
    else:
        await message.answer("⚠️ لطفاً یک متن یا فایل متنی معتبر ارسال کنید.")
        return

    await bot.send_chat_action(chat_id=message.chat.id, action="typing")
    state_data = await state.get_data()
    history = state_data.get("ai_history", [{"role": "system", "content": "You are a helpful assistant. Reply clearly in Persian."}])
    history.append({"role": "user", "content": user_prompt})
    if len(history) > 11:
        history = [history[0]] + history[-10:]

    ai_manager = AIProviderManager(db, bot)
    try:
        ai_result = await ai_manager.call(history, temperature=0.25, max_tokens=3000, purpose="user_chat")
    finally:
        await ai_manager.close()

    if ai_result.get("error") and not ai_result.get("content"):
        await message.answer("⚠️ هیچ مدل فعالی پاسخ نداد.\n" + html.escape(ai_result.get("error", "خطای نامشخص"))[:1500])
        return

    history.append({"role": "assistant", "content": ai_result["content"]})
    await state.update_data(ai_history=history)

    response_text = ai_result["content"]
    for i in range(0, len(response_text), 3900):
        chunk = response_text[i:i + 3900]
        try:
            await message.answer(chunk, parse_mode="Markdown")
        except Exception:
            await message.answer(chunk)

    tokens_used += int(ai_result.get("tokens") or 0)
    await db.execute("UPDATE users SET tokens_used=?, last_reset_date=? WHERE id=?", [tokens_used, today_tehran, user_id])

@router.message(StateFilter(BotStates.user_chat_admin))
async def process_user_chat_admin(message: Message, state: FSMContext, bot: Bot):
    user_id = message.from_user.id
    if user_id == ADMIN_ID:
        return

    if not ADMIN_ID:
        await message.answer("⚠️ مدیر تنظیم نشده است.")
        return

    hashtag = f"#User_{user_id}"
    caption = message.caption or ""

    try:
        if message.photo:
            await bot.send_photo(chat_id=ADMIN_ID, photo=message.photo[-1].file_id, caption=f"پیام جدید:\n{hashtag}\n{caption}")
        elif message.document:
            await bot.send_document(chat_id=ADMIN_ID, document=message.document.file_id, caption=f"فایل جدید:\n{hashtag}\n{caption}")
        elif message.video:
            await bot.send_video(chat_id=ADMIN_ID, video=message.video.file_id, caption=f"ویدیو جدید:\n{hashtag}\n{caption}")
        elif message.audio:
            await bot.send_audio(chat_id=ADMIN_ID, audio=message.audio.file_id, caption=f"صوت جدید:\n{hashtag}\n{caption}")
        elif message.text:
            await bot.send_message(chat_id=ADMIN_ID, text=f"پیام جدید:\n{hashtag}\n{message.text}")
        await message.answer("✅ پیام شما برای مدیر ارسال شد.")
    except Exception:
        await message.answer("⚠️ ارسال پیام به مدیر ناموفق بود.")

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

    broadcast_caption = caption + "\n#Broadcast"
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
        await message.answer(f"⏱️ موتور جستجوی اختصاصی شما {day_str} ساعت {time_str} فعال میشه\nتا اون موقع می‌تونی دستی پوشه‌هات رو ورق بزنی ! 🕵️‍♂️")
        await state.set_state(BotStates.idle)
        return

    if search_count == 0:
        window_start = now
    search_count += 1
    await state.update_data(search_count=search_count, search_window_start=window_start)

    posts = await db.execute(
        """SELECT posts.id FROM user_content_saves s JOIN posts ON s.content_id = posts.id AND s.content_type='post'
        WHERE s.user_id=? AND s.folder=? AND posts.text LIKE ? AND posts.deleted=0
        ORDER BY posts.id DESC LIMIT 15""",
        [message.from_user.id, folder, f"%{query_text}%"]
    )
    articles = await db.execute(
        """SELECT a.id, a.deep_token FROM user_content_saves s JOIN articles a ON a.id=s.content_id AND s.content_type='article'
        WHERE s.user_id=? AND s.folder=? AND a.title LIKE ? AND a.status IN ('ready','published','test')
        ORDER BY a.id DESC LIMIT 15""",
        [message.from_user.id, folder, f"%{query_text}%"]
    )

    items = [{"t": "p", "id": r["id"]} for r in posts]
    items += [{"t": "a", "id": r["id"]} for r in articles]

    if not items:
        await message.answer("❌ محتوایی با این کلمه پیدا نکردم 🫠\nیه کلمه دیگه بفرست تا دوباره بگردم:")
        return

    await state.update_data(search_items=items, search_index=0)
    await message.answer(f"🎉 {len(items)} تا مطلب با این کلمه پیدا کردم!\n(هر وقت خواستی جستجو رو عوض کنی، یه کلمه جدید بفرست 🔄)")
    await send_search_item(bot, message.chat.id, db, items[0], folder, 0)

# ============================================================
# Global text commands
# ============================================================
@router.message(F.chat.id == ADMIN_ID, F.reply_to_message, StateFilter(None, BotStates.idle))
async def process_admin_replies(message: Message, state: FSMContext, bot: Bot):
    reply_text = message.reply_to_message.text or message.reply_to_message.caption or ""
    match = re.search(r"#User_(\d+)", reply_text)
    if match:
        target_user = int(match.group(1))
        prefix = "پاسخ مدیریت:\n"
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
    "کاربر", "مدیریت", "💾 ذخیره‌های من", "❓ راهنما", "👤 پروفایل", "➕ افزودن پست",
    "📁 مدیریت محتوا", "📊 آمار", "📢 ارسال همگانی", "⚙️ اتوماسیون محتوا"
]

@router.message(F.text.in_(COMMANDS_LIST), StateFilter(None, BotStates.idle))
async def intercept_global_commands(message: Message, state: FSMContext, db: D1Database):
    text = message.text
    user_id = message.from_user.id
    state_data = await state.get_data()

    if text == "کاربر":
        await state.update_data(admin_mode="user")
        await message.answer("✅ فاز کاربری فعال شد.", reply_markup=get_main_menu())

    elif text == "مدیریت":
        if user_id == ADMIN_ID:
            await state.update_data(admin_mode="admin")
            await message.answer("✅ پنل مدیریت فعال شد.", reply_markup=get_admin_menu())
        else:
            await message.answer("⛔ شما دسترسی مدیریت ندارید.")

    elif text == "❓ راهنما":
        await message.answer(
            "🌐 /help • راهنما\n📞 /man • تماس با مدیر\n🚀 /start • شروع",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 منوی اصلی", callback_data="user_home")]])
        )

    elif text == "👤 پروفایل":
        rows = await db.execute("SELECT joined_at, role FROM users WHERE id=?", [user_id])
        joined_str = rows[0].get("joined_at") if rows else None
        user_role_db = rows[0].get("role") if rows else "user"

        time_string = "🌱 وضعیت عضویت: تازه وارد"
        join_date_line = ""
        if joined_str:
            try:
                joined_dt = datetime.fromisoformat(joined_str).replace(tzinfo=timezone.utc)
                delta = datetime.now(timezone.utc) - joined_dt
                days = max(0, delta.days)
                time_string = f"⏱️ مدت همراهی: {days} روز پیش" if days else "⏱️ مدت همراهی: امروز"
                tehran_tz = pytz.timezone("Asia/Tehran")
                joined_tehran = joined_dt.astimezone(tehran_tz)
                date_str = joined_tehran.strftime("%Y/%m/%d")
                join_date_line = f"📅 تاریخ عضویت: {date_str}\n"
            except Exception:
                pass

        saves_count = (await db.execute("SELECT COUNT(*) as c FROM user_content_saves WHERE user_id=?", [user_id]))[0].get("c", 0)
        likes_count = (await db.execute("SELECT COUNT(*) as c FROM user_content_votes WHERE user_id=? AND vote_type='like'", [user_id]))[0].get("c", 0)
        dislikes_count = (await db.execute("SELECT COUNT(*) as c FROM user_content_votes WHERE user_id=? AND vote_type='dislike'", [user_id]))[0].get("c", 0)
        role_display = "مدیر 🌟" if user_role_db == "admin" else "کاربر عادی 🟢"
        first_name_clean = message.from_user.first_name or "عزیز"

        profile_text = f"""👤 <b>پروفایل</b> · {html.escape(first_name_clean)}
🗓 <b>عضویت</b>
{join_date_line}{time_string}
📚 <b>فعالیت</b>
💾 ذخیره‌ها: <b>{saves_count}</b>
👍 لایک‌ها: <b>{likes_count}</b>
👎 دیس‌لایک‌ها: <b>{dislikes_count}</b>
🔰 <b>{role_display}</b>"""
        await message.answer(profile_text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 منوی اصلی", callback_data="user_home")]]))

    elif text == "💾 ذخیره‌های من":
        await message.answer("💾 <b>ذخیره‌های من</b>\nهمه مطالب ذخیره‌شده در یک آرشیو یکپارچه قرار دارند.\nیک پوشه را انتخاب کن:", parse_mode="HTML", reply_markup=unified_saved_kb("all"))

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
            active_posts = (await db.execute("SELECT COUNT(*) as c FROM posts WHERE deleted=0"))[0].get("c", 0)
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
            overview = await automation_overview(db)
            await message.answer(overview, parse_mode="HTML", reply_markup=automation_menu_kb(enabled))
        else:
            await message.answer("⛔ شما دسترسی مدیریت ندارید.")

@router.message(StateFilter(None, BotStates.idle))
async def process_unknown_commands(message: Message, state: FSMContext):
    data = await state.get_data()
    if message.from_user.id == ADMIN_ID and data.get("admin_mode") == "admin":
        await message.answer("❌ دستور نامعتبر است. از منوی همین بخش استفاده کن.", reply_markup=get_admin_menu())
    else:
        await message.answer("❌ دستور نامعتبر است. از منوی همین بخش استفاده کن.", reply_markup=get_main_menu())

# ============================================================
# Admin automation inputs
# ============================================================
@router.message(F.chat.id == ADMIN_ID, StateFilter(BotStates.admin_add_source))
async def admin_add_source_input(message: Message, state: FSMContext, db: D1Database, bot: Bot):
    url = (message.text or "").strip()
    if not url or not re.match(r"^https?://", url, re.I):
        await message.answer("❌ URL معتبر نیست. نمونه:\nhttps://example.com", reply_markup=get_exit_menu())
        return

    try:
        source_id = await add_source(db, url)
        data = await state.get_data()
        panel_id = data.get("panel_message_id")
        await state.set_state(BotStates.idle)
        rows = await db.execute("SELECT * FROM sources ORDER BY priority ASC, id ASC")

        if panel_id:
            try:
                await bot.edit_message_text(chat_id=message.chat.id, message_id=panel_id, text=f"✅ منبع اضافه شد.\nشناسه: {source_id}", reply_markup=source_list_kb(rows))
                return
            except Exception:
                pass

        await message.answer(f"✅ منبع با موفقیت اضافه شد.\nشناسه: {source_id}", reply_markup=source_list_kb(rows))
    except Exception as e:
        await message.answer(f"❌ افزودن منبع ناموفق بود:\n{html.escape(str(e))}", reply_markup=get_exit_menu())

@router.message(F.chat.id == ADMIN_ID, StateFilter(BotStates.admin_automation_setting))
async def admin_automation_setting_input(message: Message, state: FSMContext, db: D1Database, bot: Bot):
    data = await state.get_data()
    key = data.get("automation_setting_key")
    value = (message.text or "").strip()
    parent = data.get("parent_callback", "admin_home")

    try:
        if key == "__source_interval__":
            sid = int(data["source_interval_id"])
            value = str(max(1, int(value)))
            await db.execute("UPDATE sources SET interval_minutes=?, next_check_at=? WHERE id=?", [int(value), datetime.now(timezone.utc).isoformat(), sid])
            invalidate_sources()
            parent = f"source_view_{sid}"

        elif key == "__source_priority__":
            sid = int(data["source_priority_id"])
            value = str(max(1, int(value)))
            await db.execute("UPDATE sources SET priority=? WHERE id=?", [int(value), sid])
            invalidate_sources()
            parent = f"source_view_{sid}"

        elif key == "__provider_priority__":
            pid = int(data["provider_priority_id"])
            value = str(max(1, int(value)))
            await db.execute("UPDATE ai_providers SET priority=?, updated_at=? WHERE id=?", [int(value), datetime.now(timezone.utc).isoformat(), pid])
            invalidate_providers()
            parent = f"provider_view_{pid}"

        elif key.startswith("weight_"):
            value = str(max(0, min(100, float(value))))
            await set_setting(db, key, value)
            parent = "quality_weights"

        elif key in {"max_daily_posts", "default_source_interval", "webscout_success_interval_minutes", "webscout_empty_retry_minutes"}:
            value = str(max(1, int(value)))
            await set_setting(db, key, value)
            if key == "default_source_interval":
                now_interval = datetime.now(timezone.utc).isoformat()
                await db.execute("UPDATE sources SET interval_minutes=?, next_check_at=? WHERE enabled=1", [int(value), now_interval])
                invalidate_sources()

        elif key == "min_content_score":
            value = str(max(0, min(100, float(value))))
            await set_setting(db, key, value)

        elif key in {"editorial_prompt_channel", "editorial_prompt_article"}:
            if not value:
                raise ValueError("پرامپت نمی‌تواند خالی باشد.")
            if len(value) > 5000:
                value = value[:5000]
            await set_setting(db, key, value)

        elif key in {"min_hours_between_posts", "min_post_gap_minutes"}:
            minutes = max(1, int(float(value)))
            await set_setting(db, "min_post_gap_minutes", str(minutes))
            await set_setting(db, "min_hours_between_posts", str(minutes / 60))

        elif key == "ai_verify_mode":
            if value not in {"auto", "always", "off"}:
                raise ValueError("auto / always / off")
            await set_setting(db, key, value)

        elif key == "__publish_delay__":
            delay = max(0, min(10080, int(value)))
            row = await db.execute(
                "SELECT q.article_id FROM publication_queue q JOIN articles a ON a.id=q.article_id WHERE q.status='queued' ORDER BY a.score DESC, q.created_at ASC LIMIT 1"
            )
            if not row:
                raise ValueError("صف انتشار خالی است")
            when = (datetime.now(timezone.utc) + timedelta(minutes=delay)).isoformat()
            await db.execute("UPDATE publication_queue SET scheduled_at=? WHERE article_id=? AND status='queued'", [when, row[0]["article_id"]])

        else:
            raise ValueError("setting not supported")

        await state.set_state(BotStates.idle)
        panel_id = data.get("panel_message_id")

        if panel_id:
            if parent == "auto_schedule" or parent == "auto_channel":
                text, kb = await get_schedule_panel(db)
                await bot.edit_message_text(chat_id=message.chat.id, message_id=panel_id, text=text, parse_mode="HTML", reply_markup=kb)
            elif parent == "auto_quality":
                score = await get_setting(db, "min_content_score", str(DEFAULT_MIN_CONTENT_SCORE))
                await bot.edit_message_text(
                    chat_id=message.chat.id, message_id=panel_id,
                    text=f"🧠 <b>کیفیت محتوا</b>\nحداقل امتیاز: <b>{html.escape(score)}</b>",
                    parse_mode="HTML", reply_markup=quality_menu_kb()
                )
            elif parent == "editorial_prompts":
                ch = await get_setting(db, "editorial_prompt_channel", "")
                ar = await get_setting(db, "editorial_prompt_article", "")
                text = (
                    "✍️ <b>دستورهای محتوای تولید</b>\n"
                    + f"📌 کوتاه: <code>{html.escape(ch[:260])}</code>\n"
                    + f"📌 کامل: <code>{html.escape(ar[:260])}</code>"
                )
                kb = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="✍️ ویرایش کوتاه", callback_data="set_editorial_prompt_channel")],
                    [InlineKeyboardButton(text="📝 ویرایش کامل", callback_data="set_editorial_prompt_article")],
                    [InlineKeyboardButton(text="♻️ پیش‌فرض", callback_data="editorial_prompts_reset")],
                    [InlineKeyboardButton(text="🔙 کیفیت محتوا", callback_data="auto_quality")],
                ])
                await bot.edit_message_text(chat_id=message.chat.id, message_id=panel_id, text=text, parse_mode="HTML", reply_markup=kb)
            elif parent == "quality_weights":
                labels = [("global", "🌍 جهانی"), ("technology", "💻 فناوری"), ("ai", "🤖 AI"), ("cyber", "🔐 سایبری"),
                          ("education", "📚 آموزش"), ("iran", "🇮🇷 ایران/فارسی"), ("freshness", "🆕 تازگی"),
                          ("source", "✅ منبع"), ("novelty", "♻️ عدم تکرار")]
                text = "🎯 <b>وزن معیارها</b>\n" + "\n".join([f"{lab}: <b>{await get_setting(db, 'weight_' + k, '10')}</b>" for k, lab in labels])
                rows = [[InlineKeyboardButton(text=lab, callback_data="weight_" + k)] for k, lab in labels]
                rows.append([InlineKeyboardButton(text="🔙 کیفیت محتوا", callback_data="auto_quality")])
                await bot.edit_message_text(chat_id=message.chat.id, message_id=panel_id, text=text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
            elif parent.startswith("source_view_"):
                await bot.edit_message_text(chat_id=message.chat.id, message_id=panel_id, text="✅ تنظیم منبع ذخیره شد.", reply_markup=get_admin_back_kb(parent))
            elif parent.startswith("provider_view_"):
                await bot.edit_message_text(chat_id=message.chat.id, message_id=panel_id, text="✅ تنظیم مدل ذخیره شد.", reply_markup=get_admin_back_kb(parent))
            else:
                await bot.edit_message_text(chat_id=message.chat.id, message_id=panel_id, text="✅ ذخیره شد.", parse_mode="HTML", reply_markup=get_admin_back_kb("admin_home"))
        else:
            await message.answer("✅ ذخیره شد.", reply_markup=get_admin_menu())

    except Exception as e:
        await message.answer(f"❌ مقدار نامعتبر است: {html.escape(str(e))}", parse_mode="HTML", reply_markup=get_admin_back_kb(parent if parent else "admin_home"))

async def get_schedule_panel(db: D1Database):
    channel_id = await get_channel_id(db)
    channel_username = await get_setting(db, "channel_username", "")
    shown = html.escape(channel_username) if channel_username else ("✅ کانال خصوصی تنظیم شده" if channel_id else "⛔ تنظیم نشده")
    enabled = (await get_setting(db, "automation_enabled", "0")) == "1"
    max_daily = await get_setting(db, "max_daily_posts", str(DEFAULT_MAX_DAILY_POSTS))
    gap = int(float(await get_setting(db, "min_post_gap_minutes", str(DEFAULT_MIN_POST_GAP_MINUTES))))
    src_interval = await get_setting(db, "default_source_interval", str(DEFAULT_SOURCE_INTERVAL_MINUTES))

    est = await next_publication_estimate(db)
    if est["minutes"] <= 0:
        nxt = "آماده انتشار طبق برنامه"
    elif est["minutes"] < 60:
        nxt = f"حدود {est['minutes']} دقیقه دیگر"
    else:
        nxt = f"حدود {est['minutes'] // 60} ساعت و {est['minutes'] % 60} دقیقه دیگر"

    text = (
        "📢 <b>انتشار و زمان‌بندی</b>\n"
        f"📢 کانال: <b>{shown}</b>\n"
        f"🤖 اتوماسیون: <b>{'🟢 فعال' if enabled else '🔴 خاموش'}</b>\n"
        f"🔢 سقف روزانه: <b>{max_daily}</b> پست\n"
        f"⏱ فاصله انتشار: <b>{format_duration_minutes(gap)}</b>\n"
        f"🌐 فاصله بررسی منابع: <b>{src_interval} دقیقه</b>\n"
        f"🕐 نوبت تقریبی بعدی: <b>{nxt}</b>"
    )
    return text, schedule_menu_kb()

async def render_channel_panel(call: CallbackQuery, db: D1Database):
    channel_id = await get_channel_id(db)
    channel_username = await get_setting(db, "channel_username", "")
    if channel_username:
        shown = html.escape(channel_username)
    elif channel_id:
        shown = "✅ کانال خصوصی تنظیم شده (شناسه عددی مخفی)"
    else:
        shown = "⛔ هنوز تنظیم نشده"

    enabled = (await get_setting(db, "automation_enabled", "0")) == "1"
    max_daily = await get_setting(db, "max_daily_posts", str(DEFAULT_MAX_DAILY_POSTS))
    gap = await get_setting(db, "min_post_gap_minutes", str(DEFAULT_MIN_POST_GAP_MINUTES))
    src_interval = await get_setting(db, "default_source_interval", str(DEFAULT_SOURCE_INTERVAL_MINUTES))
    est = await next_publication_estimate(db)

    if est["minutes"] <= 0:
        nxt = "آماده انتشار طبق برنامه"
    elif est["minutes"] < 60:
        nxt = f"حدود {est['minutes']} دقیقه دیگر"
    else:
        nxt = f"حدود {est['minutes'] // 60} ساعت و {est['minutes'] % 60} دقیقه دیگر"

    text = (
        "📢 <b>انتشار و زمان‌بندی</b>\n"
        f"📢 کانال: <b>{shown}</b>\n"
        f"🤖 اتوماسیون: <b>{'🟢 فعال' if enabled else '🔴 خاموش'}</b>\n"
        f"🔢 سقف روزانه: <b>{max_daily}</b>\n"
        f"⏱ فاصله انتشار: <b>{format_duration_minutes(gap)}</b>\n"
        f"🌐 فاصله بررسی منابع: <b>{src_interval} دقیقه</b>\n"
        f"🕐 نوبت بعدی: <b>{nxt}</b>\n"
        "مدیر فقط فاصله‌ها را تعیین می‌کند؛ نوبت هر محتوا خودکار محاسبه می‌شود."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📢 تنظیم/تغییر کانال", callback_data="auto_channel_set")],
        [InlineKeyboardButton(text="🔢 سقف پست روزانه", callback_data="set_max_daily"), InlineKeyboardButton(text="⏱ فاصله انتشار", callback_data="set_min_gap")],
        [InlineKeyboardButton(text="🌐 فاصله بررسی پیش‌فرض منابع", callback_data="set_default_interval")],
        [InlineKeyboardButton(text="🧭 فاصله WebScout بعد از موفقیت", callback_data="set_webscout_interval")],
        [InlineKeyboardButton(text="🚀 همین حالا منتشر کن", callback_data="publish_now"), InlineKeyboardButton(text="🧪 تست کانال", callback_data="channel_test")],
        [InlineKeyboardButton(text="🔙 اتوماسیون محتوا", callback_data="auto_back")],
    ])
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=kb)

async def prompt_for_setting(call: CallbackQuery, state: FSMContext, key: str, label: str, parent: str = "auto_schedule"):
    await state.set_state(BotStates.admin_automation_setting)
    await state.update_data(automation_setting_key=key, panel_message_id=call.message.message_id, parent_callback=parent)
    await call.message.edit_text(label, parse_mode="HTML", reply_markup=get_exit_menu())
    await call.answer()

# ============================================================
# Core admin callbacks
# ============================================================
@router.callback_query(F.data == "admin_home")
async def admin_home(call: CallbackQuery, db: D1Database):
    if not await admin_ok(call):
        return
    text = (
        "🛠 <b>پنل مدیریت</b>\n<code>Build: " + BUILD_VERSION + "</code>\n"
        "اینجا بخش موردنظر را انتخاب کن.\nبرای سلامت، گزارش و انتشار وارد «اتوماسیون محتوا» شو."
    )
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=get_admin_menu())
    await call.answer()

@router.callback_query(F.data == "admin_automation")
async def admin_automation(call: CallbackQuery, db: D1Database):
    if not await admin_ok(call):
        return
    await call.answer()
    enabled = (await get_setting(db, "automation_enabled", "0")) == "1"
    await call.message.edit_text(await automation_overview(db), parse_mode="HTML", reply_markup=automation_menu_kb(enabled))

@router.callback_query(F.data == "admin_stats")
async def admin_stats(call: CallbackQuery, db: D1Database):
    if not await admin_ok(call):
        return
    users = (await db.execute("SELECT COUNT(*) c FROM users"))[0].get("c", 0)
    posts = (await db.execute("SELECT COUNT(*) c FROM posts WHERE deleted=0"))[0].get("c", 0)
    views = (await db.execute("SELECT COALESCE(SUM(views),0) s FROM posts"))[0].get("s", 0)
    await call.message.edit_text(
        f"📊 <b>آمار کلی</b>\n👥 کاربران: {users}\n📝 محتوای فعال: {posts}\n👁 بازدید: {views}",
        parse_mode="HTML", reply_markup=get_admin_back_kb("admin_home")
    )
    await call.answer()

@router.callback_query(F.data == "admin_content")
async def admin_content(call: CallbackQuery, db: D1Database):
    if not await admin_ok(call):
        return
    await call.message.edit_text("📁 <b>مدیریت محتوای هسته</b>\nجستجو، مشاهده و حذف محتوا از آرشیو اصلی.", parse_mode="HTML", reply_markup=get_content_management_kb())
    await call.answer()

@router.callback_query(F.data == "admin_add_post")
async def admin_add_post(call: CallbackQuery, state: FSMContext):
    if not await admin_ok(call):
        return
    await state.set_state(BotStates.waiting_post_content)
    await state.update_data(panel_message_id=call.message.message_id)
    await call.message.edit_text("📝 متن، تصویر، ویدیو یا سند پست را ارسال کن:", reply_markup=get_exit_menu())
    await call.answer()

@router.callback_query(F.data == "admin_broadcast")
async def admin_broadcast(call: CallbackQuery, state: FSMContext):
    if not await admin_ok(call):
        return
    await state.set_state(BotStates.waiting_broadcast_content)
    await state.update_data(panel_message_id=call.message.message_id)
    await call.message.edit_text("📢 پیام همگانی را ارسال کن؛ قبل از ارسال نهایی یک مرحله تأیید می‌گیریم.", reply_markup=get_exit_menu())
    await call.answer()

@router.callback_query(F.data == "admin_user_mode")
async def admin_user_mode(call: CallbackQuery, state: FSMContext):
    await state.update_data(admin_mode="user")
    await call.message.edit_text("👤 حالت کاربری فعال شد.", reply_markup=get_main_menu())
    await call.answer()

# ============================================================
# User callbacks
# ============================================================
@router.callback_query(F.data == "user_home")
async def user_home(call: CallbackQuery):
    await call.message.edit_text("🏠 منوی اصلی کاربر\nچه کاری می‌خواهی انجام بدهی؟", reply_markup=get_main_menu())
    await call.answer()

@router.callback_query(F.data == "ai_chat_start")
async def ai_chat_start(call: CallbackQuery, state: FSMContext):
    await state.set_state(BotStates.ai_chat)
    await call.message.edit_text("🤖 پیام خود را بفرست. برای خروج /exit را بزن.", reply_markup=get_exit_menu())
    await call.answer()

@router.callback_query(F.data == "user_saves")
async def user_saves(call: CallbackQuery, db: D1Database):
    await call.message.edit_text(
        "💾 <b>ذخیره‌های من</b>\nهمه مطالب ذخیره‌شده در یک آرشیو یکپارچه قرار دارند.\nیک پوشه را انتخاب کن:",
        parse_mode="HTML", reply_markup=unified_saved_kb("all")
    )
    await call.answer()

@router.callback_query(F.data.startswith("saved_folder_"))
async def saved_folder(call: CallbackQuery, db: D1Database):
    folder = call.data.split("saved_folder_", 1)[1] or "all"
    await _render_unified_saves(call, db, folder)

@router.callback_query(F.data == "user_profile")
async def user_profile(call: CallbackQuery, db: D1Database):
    uid = call.from_user.id
    rows = await db.execute("SELECT joined_at, role FROM users WHERE id=?", [uid])
    joined = rows[0].get("joined_at") if rows else ""
    role = rows[0].get("role") if rows else "user"

    try:
        joined_dt = datetime.fromisoformat(str(joined).replace("Z", "+00:00"))
        if joined_dt.tzinfo is None:
            joined_dt = joined_dt.replace(tzinfo=timezone.utc)
        days = max(0, (datetime.now(timezone.utc) - joined_dt).days)
    except Exception:
        days = 0

    saves = (await db.execute("SELECT COUNT(*) c FROM user_content_saves WHERE user_id=?", [uid]))[0].get("c", 0)
    likes = (await db.execute("SELECT COUNT(*) c FROM user_content_votes WHERE user_id=? AND vote_type='like'", [uid]))[0].get("c", 0)
    dislikes = (await db.execute("SELECT COUNT(*) c FROM user_content_votes WHERE user_id=? AND vote_type='dislike'", [uid]))[0].get("c", 0)
    role_display = "مدیر 🌟" if role == "admin" else "کاربر 🟢"
    name = html.escape(call.from_user.first_name or "دوست عزیز")

    text = (
        f"👤 <b>پروفایل {name}</b>\n"
        f"🗓 <b>عضویت:</b> {days} روز پیش\n"
        f"🔰 <b>نوع حساب:</b> {role_display}\n"
        f"💾 <b>ذخیره‌ها:</b> {saves}\n"
        f"👍 <b>لایک‌ها:</b> {likes}\n"
        f"👎 <b>دیس‌لایک‌ها:</b> {dislikes}\n"
        "✨ اینجا آمار ساده و کاربردی فعالیتت را می‌بینی."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💾 ذخیره‌های من", callback_data="user_saves")],
        [InlineKeyboardButton(text="🔙 منوی اصلی", callback_data="user_home")],
    ])
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    await call.answer()

@router.callback_query(F.data == "user_help")
async def user_help(call: CallbackQuery):
    await call.message.edit_text(
        "❓ <b>راهنما</b>\n/start • شروع\n/help • راهنما\n/man • تماس با مدیر",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 منوی اصلی", callback_data="user_home")]])
    )
    await call.answer()

@router.callback_query(F.data == "cancel_state")
async def cancel_state(call: CallbackQuery, state: FSMContext, db: D1Database):
    data = await state.get_data()
    parent = data.get("parent_callback") or "admin_home"

    await state.set_state(BotStates.idle)
    await state.update_data(panel_message_id=None, provider_base_url=None, provider_token=None, provider_edit_id=None)

    try:
        if parent == "auto_schedule":
            text, kb = await get_schedule_panel(db)
            await call.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
        elif parent == "auto_quality":
            score = await get_setting(db, "min_content_score", str(DEFAULT_MIN_CONTENT_SCORE))
            await call.message.edit_text(f"🧠 <b>کیفیت محتوا</b>\nحداقل امتیاز فعلی: <b>{html.escape(score)}</b> از 100", parse_mode="HTML", reply_markup=quality_menu_kb())
        elif parent == "quality_weights":
            labels = [("global", "🌍 جهانی"), ("technology", "💻 فناوری"), ("ai", "🤖 AI"), ("cyber", "🔐 سایبری"),
                      ("education", "📚 آموزش"), ("iran", "🇮🇷 ایران/فارسی"), ("freshness", "🆕 تازگی"),
                      ("source", "✅ منبع"), ("novelty", "♻️ عدم تکرار")]
            text = "🎯 <b>وزن معیارها</b>\n" + "\n".join([f"{lab}: <b>{await get_setting(db, 'weight_' + k, '10')}</b>" for k, lab in labels])
            rows = [[InlineKeyboardButton(text=lab, callback_data="weight_" + k)] for k, lab in labels]
            rows.append([InlineKeyboardButton(text="🔙 کیفیت محتوا", callback_data="auto_quality")])
            await call.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
        elif parent == "auto_channel":
            await render_channel_panel(call, db)
        elif parent == "auto_providers":
            rows = await db.execute("SELECT id,name,base_url,model_name,priority,enabled,web_enabled,status,last_error,last_latency_ms FROM ai_providers ORDER BY priority ASC, id ASC")
            await call.message.edit_text("🤖 <b>مدل‌های هوش مصنوعی</b>", parse_mode="HTML", reply_markup=provider_list_kb(rows))
        elif parent.startswith("provider_view_") or parent == "auto_sources":
            rows = await db.execute("SELECT * FROM sources ORDER BY priority ASC, id ASC")
            await call.message.edit_text("🌐 منابع محتوا", reply_markup=source_list_kb(rows))
        elif call.from_user.id == ADMIN_ID:
            await admin_home(call, db)
            return
        else:
            await call.message.edit_text("لغو شد.", reply_markup=get_main_menu())
    except Exception:
        try:
            await call.message.edit_text("لغو شد.", reply_markup=get_main_menu())
        except Exception:
            pass

    await call.answer("لغو شد")

# ============================================================
# Article/post interactions
# ============================================================
@router.callback_query(F.data.startswith("alike_") | F.data.startswith("adis_"))
async def process_article_voting(call: CallbackQuery, db: D1Database):
    parts = call.data.split("_")
    new_vote = "like" if parts[0] == "alike" else "dislike"
    article_id = int(parts[1])
    uid = call.from_user.id

    try:
        existing = await db.execute(
            "SELECT vote_type FROM user_content_votes WHERE user_id=? AND content_type='article' AND content_id=?",
            [uid, article_id]
        )

        if existing and existing[0].get("vote_type") == new_vote:
            await db.execute("DELETE FROM user_content_votes WHERE user_id=? AND content_type='article' AND content_id=?", [uid, article_id])
            msg = "🔄 رأی حذف شد"
        elif existing:
            await db.execute(
                "UPDATE user_content_votes SET vote_type=? WHERE user_id=? AND content_type='article' AND content_id=?",
                [new_vote, uid, article_id]
            )
            msg = "🔄 رأی تغییر کرد"
        else:
            await db.execute(
                "INSERT INTO user_content_votes(user_id,content_type,content_id,vote_type,created_at) VALUES(?,?,?,?,?)",
                [uid, "article", article_id, new_vote, datetime.now(timezone.utc).isoformat()]
            )
            msg = "✅ ثبت شد"

        likes = (await db.execute(
            "SELECT COUNT(*) c FROM user_content_votes WHERE content_type='article' AND content_id=? AND vote_type='like'",
            [article_id]
        ))[0].get("c", 0)
        dislikes = (await db.execute(
            "SELECT COUNT(*) c FROM user_content_votes WHERE content_type='article' AND content_id=? AND vote_type='dislike'",
            [article_id]
        ))[0].get("c", 0)
        saved = bool(await db.execute(
            "SELECT 1 FROM user_content_saves WHERE user_id=? AND content_type='article' AND content_id=?",
            [uid, article_id]
        ))

        await call.message.edit_reply_markup(reply_markup=get_article_inline_kb(article_id, likes, dislikes, saved))
        await call.answer(msg)
    except Exception:
        logger.exception("article vote failed")
        await call.answer("❌ ثبت واکنش انجام نشد", show_alert=True)

@router.callback_query(F.data.startswith("asave_"))
async def process_article_save_action(call: CallbackQuery):
    article_id = int(call.data.split("_")[1])
    await call.answer()
    await call.message.edit_reply_markup(reply_markup=get_save_to_folder_kb("article", article_id, f"article_actions_{article_id}"))

@router.callback_query(F.data.startswith("aunsave_"))
async def process_article_unsave_action(call: CallbackQuery, db: D1Database):
    article_id = int(call.data.split("_")[1])
    uid = call.from_user.id
    try:
        await db.execute("DELETE FROM user_content_saves WHERE user_id=? AND content_type='article' AND content_id=?", [uid, article_id])
        likes = (await db.execute(
            "SELECT COUNT(*) c FROM user_content_votes WHERE content_type='article' AND content_id=? AND vote_type='like'",
            [article_id]
        ))[0].get("c", 0)
        dislikes = (await db.execute(
            "SELECT COUNT(*) c FROM user_content_votes WHERE content_type='article' AND content_id=? AND vote_type='dislike'",
            [article_id]
        ))[0].get("c", 0)
        await call.message.edit_reply_markup(reply_markup=get_article_inline_kb(article_id, likes, dislikes, False))
        await call.answer("🗑️ از ذخیره‌ها حذف شد")
    except Exception:
        await call.answer("❌ حذف نشد", show_alert=True)

@router.callback_query(F.data.startswith("usave_"))
async def process_unified_folder_save(call: CallbackQuery, db: D1Database):
    parts = call.data.split("_")
    if len(parts) != 4:
        await call.answer("❌ خطا", show_alert=True)
        return

    _, ctype, cid_str, folder = parts
    cid = int(cid_str)
    uid = call.from_user.id

    try:
        await db.execute(
            "INSERT OR REPLACE INTO user_content_saves(user_id,content_type,content_id,folder,created_at) VALUES(?,?,?,?,?)",
            [uid, ctype, cid, folder, datetime.now(timezone.utc).isoformat()]
        )

        if ctype == "article":
            likes = (await db.execute(
                "SELECT COUNT(*) c FROM user_content_votes WHERE content_type='article' AND content_id=? AND vote_type='like'",
                [cid]
            ))[0].get("c", 0)
            dislikes = (await db.execute(
                "SELECT COUNT(*) c FROM user_content_votes WHERE content_type='article' AND content_id=? AND vote_type='dislike'",
                [cid]
            ))[0].get("c", 0)
            await call.message.edit_reply_markup(reply_markup=get_article_inline_kb(cid, likes, dislikes, True))
        else:
            rows = await db.execute("SELECT likes, dislikes FROM posts WHERE id=?", [cid])
            p = rows[0] if rows else {}
            await call.message.edit_reply_markup(reply_markup=get_post_inline_kb(cid, p.get("likes", 0), p.get("dislikes", 0), True))

        await call.answer(f"✅ در {FOLDER_NAMES.get(folder, folder)} ذخیره شد")
    except Exception:
        await call.answer("❌ ذخیره‌سازی انجام نشد", show_alert=True)

@router.callback_query(F.data.startswith("article_actions_"))
async def article_actions(call: CallbackQuery, db: D1Database):
    article_id = int(call.data.split("_")[2])
    uid = call.from_user.id
    likes = (await db.execute(
        "SELECT COUNT(*) c FROM user_content_votes WHERE content_type='article' AND content_id=? AND vote_type='like'",
        [article_id]
    ))[0].get("c", 0)
    dislikes = (await db.execute(
        "SELECT COUNT(*) c FROM user_content_votes WHERE content_type='article' AND content_id=? AND vote_type='dislike'",
        [article_id]
    ))[0].get("c", 0)
    saved = bool(await db.execute(
        "SELECT 1 FROM user_content_saves WHERE user_id=? AND content_type='article' AND content_id=?",
        [uid, article_id]
    ))
    await call.message.edit_reply_markup(reply_markup=get_article_inline_kb(article_id, likes, dislikes, saved))
    await call.answer()

@router.callback_query(F.data.startswith("like_") | F.data.startswith("dis_"))
async def process_post_voting(call: CallbackQuery, db: D1Database):
    parts = call.data.split("_")
    new_vote = "like" if parts[0] == "like" else "dislike"
    post_id = int(parts[1])
    user_id = call.from_user.id

    vote_rows = await db.execute(
        "SELECT vote_type FROM user_content_votes WHERE user_id=? AND content_type='post' AND content_id=?",
        [user_id, post_id]
    )
    response_text = ""

    try:
        if not vote_rows:
            await db.execute_batch([
                {"sql": "INSERT INTO user_content_votes(user_id,content_type,content_id,vote_type,created_at) VALUES(?,?,?,?,?)",
                 "params": [user_id, "post", post_id, new_vote, datetime.now(timezone.utc).isoformat()]},
                {"sql": f"UPDATE posts SET {new_vote}s = {new_vote}s + 1 WHERE id = ?", "params": [post_id]},
            ])
            response_text = "✅ رأی خفنت ثبت شد! 😎"
        else:
            current_vote = vote_rows[0].get("vote_type")
            if current_vote == new_vote:
                await db.execute_batch([
                    {"sql": "DELETE FROM user_content_votes WHERE user_id=? AND content_type='post' AND content_id=?", "params": [user_id, post_id]},
                    {"sql": f"UPDATE posts SET {new_vote}s = {new_vote}s - 1 WHERE id = ?", "params": [post_id]},
                ])
                response_text = "🔄 رأیت رو پس گرفتی! 🔙"
            else:
                await db.execute_batch([
                    {"sql": "UPDATE user_content_votes SET vote_type=? WHERE user_id=? AND content_type='post' AND content_id=?", "params": [new_vote, user_id, post_id]},
                    {"sql": f"UPDATE posts SET {new_vote}s = {new_vote}s + 1, {current_vote}s = {current_vote}s - 1 WHERE id = ?", "params": [post_id]},
                ])
                response_text = "🔄 رأیت با موفقیت تغییر کرد!"
    except Exception:
        response_text = "❌ خطا در ثبت رأی"

    await call.answer(response_text, show_alert=True)

    p_rows = await db.execute("SELECT likes, dislikes FROM posts WHERE id=?", [post_id])
    if p_rows:
        p = p_rows[0]
        s_rows = await db.execute(
            "SELECT folder FROM user_content_saves WHERE user_id=? AND content_type='post' AND content_id=?",
            [user_id, post_id]
        )
        kb = get_post_inline_kb(post_id, p.get("likes", 0), p.get("dislikes", 0), len(s_rows) > 0)
        try:
            await call.message.edit_reply_markup(reply_markup=kb)
        except Exception:
            pass

@router.callback_query(F.data.startswith("save_"))
async def process_save_action(call: CallbackQuery):
    post_id = int(call.data.split("_")[1])
    try:
        await call.message.edit_reply_markup(reply_markup=get_save_to_folder_kb("post", post_id, f"post_actions_{post_id}"))
    except Exception:
        pass
    await call.answer()

@router.callback_query(F.data.startswith("unsave_"))
async def process_unsave_action(call: CallbackQuery, db: D1Database):
    post_id = int(call.data.split("_")[1])
    user_id = call.from_user.id
    try:
        await db.execute("DELETE FROM user_content_saves WHERE user_id=? AND content_type='post' AND content_id=?", [user_id, post_id])
        await call.answer("🗑️ مطلب از ذخیره‌هات پاک شد!", show_alert=True)
        p_rows = await db.execute("SELECT likes, dislikes FROM posts WHERE id=?", [post_id])
        if p_rows:
            p = p_rows[0]
            kb = get_post_inline_kb(post_id, p.get("likes", 0), p.get("dislikes", 0), False)
            try:
                await call.message.edit_reply_markup(reply_markup=kb)
            except Exception:
                pass
    except Exception:
        await call.answer("❌ خطا در حذف", show_alert=True)

def get_save_to_folder_kb(content_type: str, content_id: int, back_cb: str = "user_saves") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=FOLDER_NAMES["cyber"], callback_data=f"usave_{content_type}_{content_id}_cyber"),
            InlineKeyboardButton(text=FOLDER_NAMES["tech"], callback_data=f"usave_{content_type}_{content_id}_tech"),
        ],
        [
            InlineKeyboardButton(text=FOLDER_NAMES["ai"], callback_data=f"usave_{content_type}_{content_id}_ai"),
            InlineKeyboardButton(text=FOLDER_NAMES["edu"], callback_data=f"usave_{content_type}_{content_id}_edu"),
        ],
        [InlineKeyboardButton(text="🔙 برگشت", callback_data=back_cb)],
    ])

@router.callback_query(F.data.startswith("f_view_"))
async def process_view_saved_folder(call: CallbackQuery, db: D1Database):
    folder = call.data.split("_")[2]
    await _render_unified_saves(call, db, folder)

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
        await call.message.answer(f"⏱️ موتور جستجوی اختصاصی شما {day_str} ساعت {time_str} فعال میشه\nتا اون موقع می‌تونی دستی پوشه‌هات رو ورق بزنی ! 🕵️‍♂️")
        return

    await state.set_state(BotStates.user_search_folder)
    await state.update_data(folder=folder)
    folder_display = FOLDER_NAMES.get(folder, folder)
    await call.message.answer(f"🔍 کلمات یا واژه‌ای که می‌دونی تو پوشه {folder_display} ذخیره کردی رو بفرست تا برات سرچش کنم 🕵️‍♂️")
    await call.answer()

@router.callback_query(F.data.startswith("srchpg_"))
async def search_pagination(call: CallbackQuery, state: FSMContext, db: D1Database, bot: Bot):
    parts = call.data.split("_")
    direction = parts[1]
    folder = parts[2]
    current_index = int(parts[3])

    state_data = await state.get_data()
    items = state_data.get("search_items", [])
    if not items:
        await call.answer("نتیجه‌ای یافت نشد", show_alert=True)
        return

    new_index = current_index + 1 if direction == "next" else current_index - 1
    new_index = max(0, min(new_index, len(items) - 1))
    if new_index == current_index:
        await call.answer("🚧 رسیدی به انتهای نتایج!")
        return

    await call.answer()
    await state.update_data(search_index=new_index)
    await send_search_item(bot, call.message.chat.id, db, items[new_index], folder, new_index)

# ============================================================
# Automation admin callbacks
# ============================================================
@router.callback_query(F.data == "auto_on")
async def auto_on(call: CallbackQuery, db: D1Database):
    if not await admin_ok(call):
        return
    await call.answer("اتوماسیون فعال شد")
    await set_setting(db, "automation_enabled", "1")
    await call.message.edit_text(await automation_overview(db), parse_mode="HTML", reply_markup=automation_menu_kb(True))

@router.callback_query(F.data == "auto_off")
async def auto_off(call: CallbackQuery, db: D1Database):
    if not await admin_ok(call):
        return
    await call.answer("اتوماسیون خاموش شد")
    await set_setting(db, "automation_enabled", "0")
    await call.message.edit_text(await automation_overview(db), parse_mode="HTML", reply_markup=automation_menu_kb(False))

@router.callback_query(F.data == "auto_back")
async def auto_back(call: CallbackQuery, db: D1Database):
    if not await admin_ok(call):
        return
    await call.answer()
    enabled = (await get_setting(db, "automation_enabled", "0")) == "1"
    await call.message.edit_text(await automation_overview(db), parse_mode="HTML", reply_markup=automation_menu_kb(enabled))

@router.callback_query(F.data == "auto_report")
async def auto_report(call: CallbackQuery, db: D1Database):
    if not await admin_ok(call):
        return
    await call.answer()
    await call.message.edit_text(await automation_report(db), parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 بروزرسانی", callback_data="auto_report")],
        [InlineKeyboardButton(text="🔙 اتوماسیون محتوا", callback_data="auto_back")],
    ]))

@router.callback_query(F.data == "auto_sources")
async def auto_sources(call: CallbackQuery, db: D1Database):
    if not await admin_ok(call):
        return
    await call.answer()
    rows = await db.execute("SELECT * FROM sources ORDER BY priority ASC, id ASC")
    text = "🌐 منابع محتوا\n🟢 فعال = بررسی می‌شود\n🔴 خاموش = بررسی نمی‌شود\n📌 اولویت کمتر = زودتر بررسی\n"
    if not rows:
        text += "هنوز منبع اضافه نشده است."
    else:
        for s in rows[:20]:
            text += f"{'🟢' if s.get('enabled') else '🔴'} #{s.get('id')} {s.get('name')} | {s.get('interval_minutes')}m | {s.get('category')}\n"
    await call.message.edit_text(text, reply_markup=source_list_kb(rows))

@router.callback_query(F.data == "auto_add_source")
async def auto_add_source(call: CallbackQuery, state: FSMContext):
    if not await admin_ok(call):
        return
    await state.set_state(BotStates.admin_add_source)
    await state.update_data(panel_message_id=call.message.message_id)
    await call.message.edit_text("🌐 URL سایت را بفرست:\nمثال: https://example.com", reply_markup=get_exit_menu())
    await call.answer()

@router.callback_query(F.data.startswith("source_view_"))
async def source_view(call: CallbackQuery, db: D1Database):
    if not await admin_ok(call):
        return
    await call.answer()
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
        [InlineKeyboardButton(text="🔙 منابع", callback_data="auto_sources")],
    ])
    text = (
        f"🌐 #{s['id']} {s.get('name')}\n"
        f"URL: {s.get('url')}\n"
        f"دسته: {s.get('category')}\n"
        f"فاصله: {s.get('interval_minutes')} دقیقه\n"
        f"اولویت: {s.get('priority')}\n"
        f"آخرین بررسی: {s.get('last_checked_at') or '-'}\n"
        f"خطا: {s.get('last_error') or '-'}"
    )
    await call.message.edit_text(text, reply_markup=kb)

@router.callback_query(F.data.startswith("source_toggle_"))
async def source_toggle(call: CallbackQuery, db: D1Database):
    if not await admin_ok(call):
        return
    source_id = int(call.data.split("_")[-1])
    rows = await db.execute("SELECT enabled FROM sources WHERE id=?", [source_id])
    if rows:
        await db.execute("UPDATE sources SET enabled=? WHERE id=?", [0 if rows[0].get("enabled") else 1, source_id])
        invalidate_sources()
        await source_view(call, db)

@router.callback_query(F.data.startswith("source_delete_"))
async def source_delete(call: CallbackQuery, db: D1Database):
    if not await admin_ok(call):
        return
    source_id = int(call.data.split("_")[-1])
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗑️ بله، حذف شود", callback_data=f"source_delete_confirm_{source_id}")],
        [InlineKeyboardButton(text="↩️ لغو", callback_data=f"source_view_{source_id}")],
    ])
    await call.message.edit_text("⚠️ <b>حذف منبع</b>\nاین عمل برگشت‌پذیر نیست. ادامه می‌دهی؟", parse_mode="HTML", reply_markup=kb)
    await call.answer()

@router.callback_query(F.data.startswith("source_delete_confirm_"))
async def source_delete_confirm(call: CallbackQuery, db: D1Database):
    if not await admin_ok(call):
        return
    source_id = int(call.data.split("_")[-1])
    await db.execute("DELETE FROM sources WHERE id=?", [source_id])
    invalidate_sources()
    rows = await db.execute("SELECT * FROM sources ORDER BY priority ASC, id ASC")
    await call.message.edit_text("✅ منبع حذف شد.", reply_markup=source_list_kb(rows))
    await call.answer("حذف شد")

@router.callback_query(F.data.startswith("source_priority_"))
async def source_priority(call: CallbackQuery, state: FSMContext):
    if not await admin_ok(call):
        return
    sid = int(call.data.split("_")[-1])
    await state.set_state(BotStates.admin_automation_setting)
    await state.update_data(automation_setting_key="__source_priority__", source_priority_id=sid, parent_callback=f"source_view_{sid}", panel_message_id=call.message.message_id)
    await call.message.edit_text("🔢 اولویت منبع را عددی بفرست.\nعدد کمتر = اولویت بالاتر.", reply_markup=get_exit_menu())
    await call.answer()

@router.callback_query(F.data.startswith("source_interval_"))
async def source_interval(call: CallbackQuery, state: FSMContext):
    if not await admin_ok(call):
        return
    source_id = int(call.data.split("_")[-1])
    await state.update_data(source_interval_id=source_id, panel_message_id=call.message.message_id, parent_callback=f"source_view_{source_id}")
    await state.set_state(BotStates.admin_automation_setting)
    await state.update_data(automation_setting_key="__source_interval__")
    await call.message.edit_text("فاصله بررسی را به دقیقه بفرست. مثلاً 15", reply_markup=get_exit_menu())

@router.callback_query(F.data.startswith("source_test_"))
async def source_test(call: CallbackQuery, db: D1Database, bot: Bot):
    if not await admin_ok(call):
        return
    source_id = int(call.data.split("_")[-1])
    rows = await db.execute("SELECT * FROM sources WHERE id=?", [source_id])
    if not rows:
        await call.answer("منبع یافت نشد", show_alert=True)
        return

    await call.answer("WebScout در حال بررسی منبع است…", show_alert=True)
    ai = AIProviderManager(db, bot)

    try:
        freshness = float(await get_setting(db, "webscout_freshness_hours", str(WEBSCOUT_FRESHNESS_HOURS)) or WEBSCOUT_FRESHNESS_HOURS)
        prompts = await get_manager_editorial_prompts(db)
        weights = {k: float(await get_setting(db, "weight_" + k, "10")) for k in
                   ["global", "technology", "ai", "cyber", "education", "iran", "freshness", "source", "novelty"]}

        p = (
            f"Inspect TARGET URL with your web tools. Find the newest substantive item published within the last {freshness:g} hours "
            f"that matches manager weights {json.dumps(weights, ensure_ascii=False)} and these instructions:\n"
            f"CHANNEL: {prompts.get('channel', '')}\nARTICLE: {prompts.get('article', '')}\n"
            "Return exactly FALSE if none. Otherwise return JSON with title, article_url, published_at, score, research_text."
        )

        r = await ai.webscout_call(rows[0].get("url") or "", p, max_tokens=7000)
        raw = str(r.get("content") or "").strip()

        if not r.get("ok"):
            text = "❌ <b>WebScout ناموفق بود</b>\n" + html.escape(str(r.get("error") or "")[:2000])
        elif raw.upper() == "FALSE":
            text = "🟡 <b>WebScout</b>\nبرای این URL در بازه تعیین‌شده موردی مطابق معیارهای مدیر پیدا نشد."
        else:
            obj = parse_json_object(raw)
            if obj:
                text = (
                    "🟢 <b>WebScout مورد مناسب پیدا کرد</b>\n"
                    f"📰 <b>{html.escape(str(obj.get('title') or '-'))}</b>\n"
                    f"🕒 {html.escape(str(obj.get('published_at') or '-'))}\n"
                    f"⭐ امتیاز: <b>{html.escape(str(obj.get('score') or '-'))}</b>\n"
                    f"🔗 <code>{html.escape(str(obj.get('article_url') or rows[0].get('url') or '-'))}</code>"
                )
            else:
                text = "⚠️ WebScout پاسخ قابل پردازش برنگرداند."

        await call.message.edit_text(text, parse_mode="HTML", reply_markup=get_admin_back_kb(f"source_view_{source_id}"))
    finally:
        await ai.close()

# Providers
@router.callback_query(F.data == "auto_providers")
async def auto_providers(call: CallbackQuery, db: D1Database):
    if not await admin_ok(call):
        return
    rows = await db.execute(
        "SELECT id,name,base_url,model_name,priority,enabled,web_enabled,status,last_error,last_latency_ms FROM ai_providers ORDER BY priority ASC, id ASC"
    )
    text = "🤖 <b>مدل‌های هوش مصنوعی</b>\nهر مدل را باز کن تا ویرایش، تست، فعال/غیرفعال، اولویت‌بندی یا حذفش کنی.\n"
    if not rows:
        text += "هیچ Provider فعالی وجود ندارد."
    else:
        for p in rows:
            text += f"{'🟢' if p.get('enabled') else '🔴'} #{p['id']} {p.get('name')} | {p.get('model_name')} | priority={p.get('priority')}\n"
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=provider_list_kb(rows))

@router.callback_query(F.data == "auto_add_provider")
async def auto_add_provider(call: CallbackQuery, state: FSMContext):
    if not await admin_ok(call):
        return
    await state.set_state(BotStates.admin_add_provider)
    await state.update_data(provider_draft={}, parent_callback="auto_providers", panel_message_id=call.message.message_id)
    await call.message.edit_text(
        "🤖 افزودن مدل جدید\nمرحله ۱ از ۳\n🔗 Base URL خود را ارسال کنید.\nمی‌تواند endpoint کامل /chat/completions باشد یا Base URL استاندارد مثل /v1.",
        reply_markup=get_exit_menu()
    )
    await call.answer()

@router.message(F.chat.id == ADMIN_ID, StateFilter(BotStates.admin_add_provider))
async def provider_base_input(message: Message, state: FSMContext, bot: Bot):
    base_url = (message.text or "").strip()
    if not re.match(r"^https?://", base_url, re.I):
        await message.answer("❌ Base URL معتبر نیست. باید با http:// یا https:// شروع شود.", reply_markup=get_exit_menu())
        return

    data = await state.get_data()
    panel_id = data.get("panel_message_id")
    await state.update_data(provider_base_url=base_url)
    await state.set_state(BotStates.admin_provider_token)

    text = "🤖 افزودن مدل جدید\nمرحله ۲ از ۳\n🔐 توکن/API Key این مدل را ارسال کنید:"
    if panel_id:
        try:
            await bot.edit_message_text(chat_id=message.chat.id, message_id=panel_id, text=text, reply_markup=get_exit_menu())
            return
        except Exception:
            pass
    await message.answer(text, reply_markup=get_exit_menu())

@router.message(F.chat.id == ADMIN_ID, StateFilter(BotStates.admin_provider_token))
async def provider_token_input(message: Message, state: FSMContext, bot: Bot):
    token = (message.text or "").strip()
    if len(token) < 4:
        await message.answer("❌ توکن خیلی کوتاه است. دوباره ارسال کنید.", reply_markup=get_exit_menu())
        return

    data = await state.get_data()
    panel_id = data.get("panel_message_id")
    await state.update_data(provider_token=token)
    await state.set_state(BotStates.admin_provider_model)

    # Security: try to delete token message from chat history.
    try:
        await message.delete()
    except Exception:
        pass

    text = "🤖 افزودن مدل جدید\nمرحله ۳ از ۳\n🧩 نام دقیق Model را دقیقاً همان‌طور که Provider می‌شناسد ارسال کنید:"
    if panel_id:
        try:
            await bot.edit_message_text(chat_id=message.chat.id, message_id=panel_id, text=text, reply_markup=get_exit_menu())
            return
        except Exception:
            pass
    await message.answer(text, reply_markup=get_exit_menu())

@router.message(F.chat.id == ADMIN_ID, StateFilter(BotStates.admin_provider_model))
async def provider_model_input(message: Message, state: FSMContext, db: D1Database, bot: Bot):
    model = (message.text or "").strip()
    data = await state.get_data()
    base_url = data.get("provider_base_url", "")
    token = data.get("provider_token", "")

    if not model:
        await message.answer("❌ نام مدل خالی است.", reply_markup=get_exit_menu())
        return

    panel_id = data.get("panel_message_id")
    if panel_id:
        try:
            await bot.edit_message_text(
                chat_id=message.chat.id,
                message_id=panel_id,
                text="🔎 در حال بررسی endpoint و نام مدل...\n🧪 سپس یک درخواست واقعی TEST_OK به مدل ارسال می‌شود..."
            )
        except Exception:
            await message.answer("🔎 در حال بررسی endpoint و نام مدل...")
    else:
        await message.answer("🧪 در حال تست اتصال و نام دقیق مدل...")

    tester = AIProviderManager(db)
    try:
        result = await tester.test_provider_values(base_url, token, model)
    finally:
        await tester.close()

    if not result.get("ok"):
        parent = data.get("parent_callback", "auto_providers")
        await state.set_state(BotStates.idle)
        await state.update_data(provider_base_url=None, provider_token=None, provider_edit_id=None)

        error_text = (
            "❌ این مدل در تست اولیه قبول نشد.\n"
            f"HTTP/API: {html.escape(str(result.get('error', 'unknown'))[:1000])}\n"
            "هیچ چیزی ذخیره نشد.\n"
            "اگر Gemini است، Base URL رسمی OpenAI-compatible: https://generativelanguage.googleapis.com/v1beta/openai/\n"
            "یا Base URL بومی: https://generativelanguage.googleapis.com/v1beta"
        )
        kb = provider_list_kb(await db.execute(
            "SELECT id,name,base_url,model_name,priority,enabled,web_enabled,status,last_error,last_latency_ms FROM ai_providers ORDER BY priority ASC,id ASC"
        )) if parent == "auto_providers" else get_admin_back_kb(parent)

        if panel_id:
            try:
                await bot.edit_message_text(chat_id=message.chat.id, message_id=panel_id, text=error_text, parse_mode="HTML", reply_markup=kb)
                return
            except Exception:
                pass
        await message.answer(error_text, parse_mode="HTML", reply_markup=kb)
        return

    now = datetime.now(timezone.utc).isoformat()
    host = urllib.parse.urlsplit(base_url).netloc or "provider"
    name = f"{model[:80]} | {host[:30]}"[:120]
    edit_id = data.get("provider_edit_id")

    if edit_id:
        old = await db.execute("SELECT priority,enabled,created_at FROM ai_providers WHERE id=?", [int(edit_id)])
        priority = int(old[0].get("priority") or 10) if old else 10
        await db.execute(
            "UPDATE ai_providers SET name=?, base_url=?, encrypted_api_key=?, model_name=?, updated_at=?, status='healthy', last_error=NULL, cooldown_until=NULL, last_checked_at=?, last_latency_ms=? WHERE id=?",
            [name, base_url, encrypt_secret(token), model, now, now, result.get("latency_ms", 0), int(edit_id)]
        )
        action_text = f"✏️ مدل #{edit_id} با موفقیت ویرایش و تست شد."
    else:
        count = await db.execute("SELECT COALESCE(MAX(priority),0) AS p FROM ai_providers")
        priority = int(count[0].get("p") or 0) + 10 if count else 10
        await db.execute(
            "INSERT INTO ai_providers(name, base_url, encrypted_api_key, model_name, priority, enabled, web_enabled, created_at, updated_at, status, last_checked_at, last_latency_ms, consecutive_failures) VALUES(?, ?, ?, ?, ?, 1, 0, ?, ?, 'healthy', ?, ?, 0)",
            [name, base_url, encrypt_secret(token), model, priority, now, now, now, result.get("latency_ms", 0)]
        )
        action_text = "➕ مدل جدید با موفقیت اضافه و تست شد."

    invalidate_providers()
    await state.set_state(BotStates.idle)
    await state.update_data(provider_base_url=None, provider_token=None, provider_edit_id=None, panel_message_id=None)

    rows = await db.execute(
        "SELECT id,name,base_url,model_name,priority,enabled,web_enabled,status,last_error,last_latency_ms FROM ai_providers ORDER BY priority ASC, id ASC"
    )
    parent = data.get("parent_callback", "auto_providers")
    panel_id = data.get("panel_message_id")

    text = (
        f"✅ {action_text}\n"
        f"🤖 Model: <code>{html.escape(model)}</code>\n"
        f"🔌 Protocol: <code>{html.escape(str(result.get('protocol', 'auto')))}</code>\n"
        f"⚡ زمان پاسخ تست واقعی: {result.get('latency_ms', 0)}ms\n"
        f"🧪 پاسخ مدل: <code>{html.escape(str(result.get('preview', 'TEST_OK'))[:120])}</code>\n"
        f"🔢 اولویت: {priority}"
    )

    if panel_id:
        try:
            await bot.edit_message_text(
                chat_id=message.chat.id,
                message_id=panel_id,
                text=text,
                parse_mode="HTML",
                reply_markup=provider_list_kb(rows) if parent == "auto_providers" else get_admin_back_kb(parent)
            )
            return
        except Exception:
            pass

    await message.answer(text, parse_mode="HTML", reply_markup=provider_list_kb(rows))

@router.callback_query(F.data.startswith("provider_view_"))
async def provider_view(call: CallbackQuery, db: D1Database):
    if not await admin_ok(call):
        return
    await call.answer()
    pid = int(call.data.split("_")[-1])
    rows = await db.execute(
        "SELECT id,name,base_url,model_name,priority,enabled,web_enabled,status,last_error,last_latency_ms,cooldown_until FROM ai_providers WHERE id=?",
        [pid]
    )
    if not rows:
        await call.answer("Provider یافت نشد", show_alert=True)
        return

    p = rows[0]
    status = p.get("status") or "unknown"
    status_text = {
        "healthy": "🟢 سالم",
        "invalid": "🔴 تنظیمات/مدل نامعتبر",
        "cooldown": "🟡 موقتاً در انتظار",
        "unknown": "⚪ تست نشده",
    }.get(status, status)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ ویرایش مدل", callback_data=f"provider_edit_{pid}"), InlineKeyboardButton(text="🧪 تست اتصال", callback_data=f"provider_test_{pid}")],
        [InlineKeyboardButton(text="🔢 تغییر اولویت", callback_data=f"provider_priority_{pid}"), InlineKeyboardButton(text="⏸ خاموش" if p.get("enabled") else "▶️ فعال", callback_data=f"provider_toggle_{pid}")],
        [InlineKeyboardButton(text="🌐 Web Scout: خاموش" if not p.get("web_enabled") else "🌐 Web Scout: روشن", callback_data=f"provider_web_toggle_{pid}")],
        [InlineKeyboardButton(text="🗑 حذف مدل", callback_data=f"provider_delete_{pid}")],
        [InlineKeyboardButton(text="🔙 فهرست مدل‌ها", callback_data="auto_providers")],
    ])

    text = (
        f"🤖 مدل #{p['id']}\n"
        f"Model: <code>{html.escape(str(p.get('model_name')))}</code>\n"
        f"Base URL: <code>{html.escape(str(p.get('base_url')))}</code>\n"
        f"اولویت: {p.get('priority')}\n"
        f"Web Scout: {'🟢 روشن' if p.get('web_enabled') else '⚪ خاموش'}\n"
        f"وضعیت: {status_text}\n"
        f"Latency: {p.get('last_latency_ms') or 0}ms\n"
        f"آخرین خطا: {html.escape(str(p.get('last_error') or '-'))[:500]}"
    )
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=kb)

@router.callback_query(F.data.startswith("provider_web_toggle_"))
async def provider_web_toggle(call: CallbackQuery, db: D1Database):
    if not await admin_ok(call):
        return
    await call.answer()
    pid = int(call.data.rsplit("_", 1)[-1])
    rows = await db.execute("SELECT web_enabled, base_url FROM ai_providers WHERE id=?", [pid])
    if not rows:
        await call.answer("مدل پیدا نشد", show_alert=True)
        return

    current = int(rows[0].get("web_enabled") or 0)
    new_value = 0 if current else 1
    base = str(rows[0].get("base_url") or "").lower()

    if new_value == 1 and not ("generativelanguage.googleapis.com" in base or "openrouter.ai" in base):
        await call.answer("این provider برای WebScout پشتیبانی نمی‌شود. فقط Gemini Native یا OpenRouter.", show_alert=True)
        return

    if new_value == 0:
        active = await db.execute("SELECT COUNT(*) c FROM ai_providers WHERE enabled=1 AND web_enabled=1 AND id!=?", [pid])
        if not active or int(active[0].get("c") or 0) <= 0:
            await call.answer("حداقل یک WebScout باید روشن بماند.", show_alert=True)
            return

    await db.execute("UPDATE ai_providers SET web_enabled=?, updated_at=? WHERE id=?", [new_value, datetime.now(timezone.utc).isoformat(), pid])
    invalidate_providers()
    await call.answer("🌐 Web Scout روشن شد" if new_value else "🌐 Web Scout خاموش شد")
    await provider_view(call, db)

@router.callback_query(F.data == "provider_help")
async def provider_help(call: CallbackQuery):
    if not await admin_ok(call):
        return
    await call.answer()
    text = (
        "🤖 <b>راهنمای مدل‌های هوش مصنوعی</b>\n"
        "برای افزودن هر مدل فقط سه چیز لازم است:\n"
        "1️⃣ Base URL\n2️⃣ Token / API Key\n3️⃣ نام دقیق Model\n"
        "ربات قبل از ذخیره یک درخواست واقعی آزمایشی می‌فرستد.\n"
        "🔢 عدد اولویت کمتر = اولویت بالاتر\n"
        "🌐 Web Scout فقط برای Gemini Native یا OpenRouter فعال می‌شود.\n"
        "🟡 خطاهای موقت مثل 429/503 باعث cooldown می‌شوند.\n"
        "🔴 خطاهایی مثل 404/401/403 به‌عنوان مشکل تنظیمات علامت‌گذاری می‌شوند."
    )
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 فهرست مدل‌ها", callback_data="auto_providers")]]))

@router.callback_query(F.data.startswith("provider_edit_"))
async def provider_edit(call: CallbackQuery, state: FSMContext, db: D1Database):
    if not await admin_ok(call):
        return
    pid = int(call.data.split("_")[-1])
    rows = await db.execute("SELECT id,base_url,model_name FROM ai_providers WHERE id=?", [pid])
    if not rows:
        await call.answer("مدل پیدا نشد", show_alert=True)
        return

    p = rows[0]
    await state.set_state(BotStates.admin_add_provider)
    await state.update_data(provider_edit_id=pid, provider_base_url=None, provider_token=None, panel_message_id=call.message.message_id, parent_callback="provider_view_" + str(pid))
    await call.message.edit_text(
        f"✏️ <b>ویرایش مدل #{pid}</b>\n"
        f"مدل فعلی: <code>{html.escape(str(p.get('model_name')))}</code>\n"
        "مرحله ۱ از ۳ 🔗 Base URL جدید را ارسال کن.",
        parse_mode="HTML", reply_markup=get_exit_menu()
    )

@router.callback_query(F.data.startswith("provider_test_"))
async def provider_test(call: CallbackQuery, db: D1Database):
    if not await admin_ok(call):
        return
    pid = int(call.data.split("_")[-1])
    await call.answer("🧪 در حال تست...", show_alert=False)

    manager = AIProviderManager(db)
    try:
        result = await manager.test_provider(pid)
    finally:
        await manager.close()

    if result.get("ok"):
        await call.message.edit_text(
            f"✅ تست موفق بود.\n⚡ زمان پاسخ: {result.get('latency_ms', 0)}ms\n🤖 پاسخ: {html.escape(result.get('preview', 'OK'))}",
            reply_markup=get_admin_back_kb(f"provider_view_{pid}")
        )
    else:
        await call.message.edit_text(
            f"❌ تست ناموفق بود.\n{html.escape(result.get('error', 'unknown')[:1200])}",
            reply_markup=get_admin_back_kb(f"provider_view_{pid}")
        )

@router.callback_query(F.data.startswith("provider_priority_"))
async def provider_priority(call: CallbackQuery, state: FSMContext):
    if not await admin_ok(call):
        return
    pid = int(call.data.split("_")[-1])
    await state.set_state(BotStates.admin_automation_setting)
    await state.update_data(automation_setting_key="__provider_priority__", provider_priority_id=pid, parent_callback=f"provider_view_{pid}", panel_message_id=call.message.message_id)
    await call.message.edit_text("🔢 اولویت این مدل را به عدد بفرست.\nعدد کمتر = اولویت بالاتر.", reply_markup=get_exit_menu())

@router.callback_query(F.data.startswith("provider_toggle_"))
async def provider_toggle(call: CallbackQuery, db: D1Database):
    if not await admin_ok(call):
        return
    pid = int(call.data.split("_")[-1])
    rows = await db.execute("SELECT enabled FROM ai_providers WHERE id=?", [pid])
    if rows:
        new_enabled = 0 if rows[0].get("enabled") else 1
        if new_enabled == 0:
            active = await db.execute("SELECT COUNT(*) c FROM ai_providers WHERE enabled=1 AND web_enabled=1 AND id!=?", [pid])
            current_web = await db.execute("SELECT web_enabled FROM ai_providers WHERE id=?", [pid])
            if current_web and int(current_web[0].get("web_enabled") or 0) and (not active or int(active[0].get("c") or 0) <= 0):
                await call.answer("حداقل یک WebScout فعال باید روشن بماند.", show_alert=True)
                return

        await db.execute("UPDATE ai_providers SET enabled=?, updated_at=? WHERE id=?", [new_enabled, datetime.now(timezone.utc).isoformat(), pid])
        invalidate_providers()
        await provider_view(call, db)

@router.callback_query(F.data.regexp(r"^provider_delete_(\d+)$"))
async def provider_delete(call: CallbackQuery, db: D1Database):
    if not await admin_ok(call):
        return
    pid = int(call.data.split("_")[-1])
    rows = await db.execute("SELECT id, model_name, name FROM ai_providers WHERE id=?", [pid])
    if not rows:
        await call.answer("مدل پیدا نشد", show_alert=True)
        return

    p = rows[0]
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗑️ بله، حذف شود", callback_data=f"provider_delete_confirm_{pid}")],
        [InlineKeyboardButton(text="↩️ لغو", callback_data=f"provider_view_{pid}")],
    ])
    await call.message.edit_text(
        f"⚠️ <b>حذف مدل</b>\nمدل: <code>{html.escape(str(p.get('model_name')))}</code>\nاین Provider از چرخه Failover حذف خواهد شد. ادامه می‌دهی؟",
        parse_mode="HTML", reply_markup=kb
    )

@router.callback_query(F.data.regexp(r"^provider_delete_confirm_(\d+)$"))
async def provider_delete_confirm(call: CallbackQuery, db: D1Database):
    if not await admin_ok(call):
        return
    pid = int(call.data.split("_")[-1])
    row = await db.execute("SELECT enabled, web_enabled FROM ai_providers WHERE id=?", [pid])

    if row and int(row[0].get("enabled") or 0) and int(row[0].get("web_enabled") or 0):
        active = await db.execute("SELECT COUNT(*) c FROM ai_providers WHERE enabled=1 AND web_enabled=1 AND id!=?", [pid])
        if not active or int(active[0].get("c") or 0) <= 0:
            await call.answer("حداقل یک WebScout فعال باید باقی بماند.", show_alert=True)
            return

    await db.execute("DELETE FROM ai_providers WHERE id=?", [pid])
    invalidate_providers()
    rows = await db.execute(
        "SELECT id,name,base_url,model_name,priority,enabled,web_enabled,status,last_error,last_latency_ms FROM ai_providers ORDER BY priority ASC, id ASC"
    )
    await call.message.edit_text("🗑️ <b>مدل حذف شد.</b>\nفهرست مدل‌ها:", parse_mode="HTML", reply_markup=provider_list_kb(rows))
    await call.answer("حذف شد")

# Schedule / channel
@router.callback_query(F.data == "auto_channel")
async def auto_channel(call: CallbackQuery, db: D1Database):
    if not await admin_ok(call):
        return
    await call.answer()
    await render_channel_panel(call, db)

@router.callback_query(F.data == "auto_channel_set")
async def auto_channel_set(call: CallbackQuery, state: FSMContext, db: D1Database):
    if not await admin_ok(call):
        return
    await call.answer()
    await state.set_state(BotStates.admin_channel_input)
    await state.update_data(panel_message_id=call.message.message_id)
    await call.message.edit_text(
        "📢 <b>تنظیم کانال انتشار</b>\n"
        "آیدی کانال یا @username را ارسال کن.\n"
        "مثال: <code>@my_channel</code> یا <code>-1001234567890</code>\n"
        "ربات باید در کانال ادمین باشد و اجازه انتشار پیام داشته باشد.",
        parse_mode="HTML", reply_markup=get_exit_menu()
    )

@router.callback_query(F.data == "publish_now")
async def publish_now(call: CallbackQuery, db: D1Database, bot: Bot):
    if not await admin_ok(call):
        return
    await call.answer("🚀 در حال انتشار…")
    try:
        ok = await publish_next_article(db, bot, force=True)
        msg = "✅ اولین محتوای آماده همین حالا منتشر شد." if ok else "⏸ محتوای آماده‌ای برای انتشار نیست یا سقف روزانه پر شده است."
    except Exception as e:
        msg = "❌ انتشار دستی شکست خورد:\n" + html.escape(str(e)[:1000])
    await call.message.edit_text(
        msg,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 انتشار و زمان‌بندی", callback_data="auto_channel")]])
    )

@router.callback_query(F.data == "channel_test")
async def channel_test(call: CallbackQuery, db: D1Database, bot: Bot):
    if not await admin_ok(call):
        return
    channel_id = await get_channel_id(db)
    if not channel_id:
        await call.answer("کانال هنوز تنظیم نشده است.", show_alert=True)
        return

    try:
        chat = await bot.get_chat(channel_id)
        me = await bot.get_me()
        member = await bot.get_chat_member(chat.id, me.id)
        status = str(getattr(member, "status", ""))
        if status not in {"administrator", "creator"}:
            raise RuntimeError("ربات در کانال ادمین نیست.")

        perms = getattr(member, "can_post_messages", None)
        if perms is False:
            raise RuntimeError("اجازه انتشار پیام برای ربات فعال نیست.")

        label = "@" + chat.username if getattr(chat, "username", None) else "کانال خصوصی"
        await call.message.edit_text(
            f"✅ <b>کانال سالم است</b>\n📢 {html.escape(label)}\n👤 وضعیت ربات: {html.escape(status)}\n📝 اجازه انتشار: ✅",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 کانال و انتشار", callback_data="auto_channel")]])
        )
    except Exception as e:
        await call.message.edit_text(
            f"❌ <b>تست کانال ناموفق بود</b>\n{html.escape(str(e)[:800])}",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 کانال و انتشار", callback_data="auto_channel")]])
        )
    await call.answer()

@router.message(F.chat.id == ADMIN_ID, StateFilter(BotStates.admin_channel_input))
async def admin_channel_input(message: Message, state: FSMContext, db: D1Database, bot: Bot):
    raw = (message.text or "").strip()
    if not raw:
        return

    if "t.me/" in raw:
        path = urllib.parse.urlsplit(raw).path.strip("/")
        raw = "@" + path.split("/")[-1]

    try:
        chat = await bot.get_chat(raw)
        me = await bot.get_me()
        member = await bot.get_chat_member(chat.id, me.id)
        status = str(getattr(member, "status", ""))
        if status not in {"administrator", "creator"}:
            await message.answer("❌ ربات در این کانال ادمین نیست یا دسترسی کافی ندارد.", reply_markup=get_exit_menu())
            return

        await set_setting(db, "channel_id", str(chat.id))
        await set_setting(db, "channel_username", "@" + chat.username if getattr(chat, "username", None) else "")
        await state.set_state(BotStates.idle)

        label = "@" + chat.username if getattr(chat, "username", None) else "کانال خصوصی تنظیم شد"
        panel_id = (await state.get_data()).get("panel_message_id")
        text = f"✅ <b>کانال با موفقیت تنظیم شد.</b>\n📢 {html.escape(label)}\n🆔 <code>{chat.id}</code>"

        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🧪 تست کانال", callback_data="channel_test")],
            [InlineKeyboardButton(text="🔙 کانال و انتشار", callback_data="auto_channel")],
        ])

        if panel_id:
            try:
                await bot.edit_message_text(chat_id=message.chat.id, message_id=panel_id, text=text, parse_mode="HTML", reply_markup=kb)
                return
            except Exception:
                pass

        await message.answer(text, parse_mode="HTML", reply_markup=kb)
    except Exception as e:
        await message.answer(f"❌ نتوانستم کانال را تأیید کنم:\n{html.escape(str(e)[:800])}\nآیدی/@username را دوباره بفرست.", reply_markup=get_exit_menu())

@router.callback_query(F.data == "set_max_daily")
async def set_max_daily(call: CallbackQuery, state: FSMContext, db: D1Database):
    if not await admin_ok(call):
        return
    current = await get_setting(db, "max_daily_posts", str(DEFAULT_MAX_DAILY_POSTS))
    await prompt_for_setting(call, state, "max_daily_posts", f"🔢 <b>سقف تقریبی پست روزانه</b> را به عدد بفرست.\nفعلاً روی <b>{html.escape(current)}</b> پست است.", "auto_channel")

@router.callback_query(F.data == "set_min_gap")
async def set_min_gap(call: CallbackQuery, state: FSMContext, db: D1Database):
    if not await admin_ok(call):
        return
    current = await get_setting(db, "min_post_gap_minutes", str(DEFAULT_MIN_POST_GAP_MINUTES))
    await prompt_for_setting(
        call, state, "min_post_gap_minutes",
        f"⏱ <b>حداقل فاصله بین دو پست</b> را بر حسب دقیقه بفرست.\nفعلاً روی <b>{format_duration_minutes(current)}</b> است.\nمثال: <code>30</code>",
        "auto_channel"
    )

@router.callback_query(F.data == "set_default_interval")
async def set_default_interval(call: CallbackQuery, state: FSMContext, db: D1Database):
    if not await admin_ok(call):
        return
    current = await get_setting(db, "default_source_interval", str(DEFAULT_SOURCE_INTERVAL_MINUTES))
    await prompt_for_setting(
        call, state, "default_source_interval",
        f"🌐 <b>فاصله بررسی پیش‌فرض منابع</b> را بر حسب دقیقه بفرست.\nفعلاً روی <b>{html.escape(current)}</b> دقیقه است.",
        "auto_channel"
    )

@router.callback_query(F.data == "set_webscout_interval")
async def set_webscout_interval(call: CallbackQuery, state: FSMContext, db: D1Database):
    if not await admin_ok(call):
        return
    current = await get_setting(db, "webscout_success_interval_minutes", str(WEBSCOUT_SUCCESS_INTERVAL_MINUTES))
    await prompt_for_setting(
        call, state, "webscout_success_interval_minutes",
        f"🧭 <b>فاصله WebScout بعد از پیدا شدن محتوای مناسب</b> را بر حسب دقیقه بفرست.\nفعلاً روی <b>{html.escape(current)}</b> دقیقه است.",
        "auto_channel"
    )

# Quality
@router.callback_query(F.data == "auto_quality")
async def auto_quality(call: CallbackQuery, db: D1Database):
    if not await admin_ok(call):
        return
    score = await get_setting(db, "min_content_score", str(DEFAULT_MIN_CONTENT_SCORE))
    text = (
        f"🧠 <b>کیفیت محتوا</b>\nحداقل امتیاز فعلی: <b>{html.escape(score)}</b> از 100\n"
        "این بخش فقط درباره انتخاب و کیفیت خبر است؛ تنظیم زمان و تعداد پست‌ها در بخش «برنامه انتشار» قرار دارد."
    )
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=quality_menu_kb())
    await call.answer()

@router.callback_query(F.data == "set_min_score")
async def set_min_score(call: CallbackQuery, state: FSMContext):
    if not await admin_ok(call):
        return
    await prompt_for_setting(call, state, "min_content_score", "⭐ حداقل امتیاز انتشار را بین 0 تا 100 بفرست. پیشنهاد: 75", "auto_quality")

@router.callback_query(F.data == "quality_weights")
async def quality_weights(call: CallbackQuery, db: D1Database):
    if not await admin_ok(call):
        return
    await call.answer()
    items = [
        ("global", "🌍 اهمیت جهانی"), ("technology", "💻 فناوری"), ("ai", "🤖 هوش مصنوعی"),
        ("cyber", "🔐 امنیت سایبری"), ("education", "📚 آموزش"), ("iran", "🇮🇷 ایران/فارسی"),
        ("freshness", "🆕 تازگی"), ("source", "✅ اعتبار منبع"), ("novelty", "♻️ عدم تکرار"),
    ]
    text = "🎯 <b>وزن معیارها</b>\nعدد بالاتر = اهمیت بیشتر.\n"
    rows = []
    for k, label in items:
        text += f"{label}: <b>{await get_setting(db, 'weight_' + k, '10')}</b>\n"
        rows.append([InlineKeyboardButton(text=label, callback_data="weight_" + k)])
    rows.append([InlineKeyboardButton(text="🔙 کیفیت محتوا", callback_data="auto_quality")])
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))

@router.callback_query(F.data.startswith("weight_"))
async def set_weight(call: CallbackQuery, state: FSMContext):
    if not await admin_ok(call):
        return
    key = call.data
    labels = {
        "weight_global": "🌍 اهمیت جهانی",
        "weight_technology": "💻 فناوری",
        "weight_ai": "🤖 هوش مصنوعی",
        "weight_cyber": "🔐 امنیت سایبری",
        "weight_education": "📚 آموزش",
        "weight_iran": "🇮🇷 ارتباط ایران/فارسی",
        "weight_freshness": "🆕 تازگی",
        "weight_source": "✅ اعتبار منبع",
        "weight_novelty": "♻️ عدم تکرار",
    }
    await prompt_for_setting(call, state, key, f"{labels.get(key, key)} را به عدد 0 تا 100 بفرست.", "quality_weights")

@router.callback_query(F.data == "editorial_prompts")
async def editorial_prompts_panel(call: CallbackQuery, db: D1Database):
    if not await admin_ok(call):
        return
    ch = await get_setting(db, "editorial_prompt_channel", "")
    ar = await get_setting(db, "editorial_prompt_article", "")
    text = (
        "✍️ <b>دستورهای محتوای تولید</b>\n"
        "این دو دستور فقط تعیین می‌کنند چه اطلاعاتی پوشش داده شود.\n"
        f"📌 کوتاه (~500): <code>{html.escape(ch[:260])}</code>\n"
        f"📌 کامل (~2000): <code>{html.escape(ar[:260])}</code>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✍️ ویرایش دستور کوتاه", callback_data="set_editorial_prompt_channel")],
        [InlineKeyboardButton(text="📝 ویرایش دستور کامل", callback_data="set_editorial_prompt_article")],
        [InlineKeyboardButton(text="♻️ بازگردانی دستور پیش‌فرض", callback_data="editorial_prompts_reset")],
        [InlineKeyboardButton(text="🔙 کیفیت محتوا", callback_data="auto_quality")],
    ])
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    await call.answer()

@router.callback_query(F.data == "editorial_prompts_reset")
async def editorial_prompts_reset(call: CallbackQuery, db: D1Database):
    if not await admin_ok(call):
        return
    await set_setting(db, "editorial_prompt_channel", "فقط محتوای فنی، دقیق و واقعاً ارزشمند برای مخاطب فناوری و هوش مصنوعی را پوشش بده؛ مطالب سطحی و کلیشه‌ای را کنار بگذار.")
    await set_setting(db, "editorial_prompt_article", "مقاله کامل باید فنی، غنی و مبتنی بر اطلاعات واقعی منبع باشد؛ جزئیات، زمینه، نحوه کار، اعداد و اثرات قابل اتکا را توضیح بده.")
    await editorial_prompts_panel(call, db)

@router.callback_query(F.data == "set_editorial_prompt_channel")
async def set_editorial_prompt_channel(call: CallbackQuery, state: FSMContext, db: D1Database):
    if not await admin_ok(call):
        return
    current = await get_setting(db, "editorial_prompt_channel", "")
    await prompt_for_setting(
        call, state, "editorial_prompt_channel",
        "✍️ <b>پرامپت محتوای کوتاه کانال</b>\nپرامپت جدید را بفرست.\nفعلی:\n<code>" + html.escape(current[:1800]) + "</code>",
        "editorial_prompts"
    )

@router.callback_query(F.data == "set_editorial_prompt_article")
async def set_editorial_prompt_article(call: CallbackQuery, state: FSMContext, db: D1Database):
    if not await admin_ok(call):
        return
    current = await get_setting(db, "editorial_prompt_article", "")
    await prompt_for_setting(
        call, state, "editorial_prompt_article",
        "📝 <b>پرامپت محتوای کامل داخل ربات</b>\nپرامپت جدید را بفرست.\nفعلی:\n<code>" + html.escape(current[:1800]) + "</code>",
        "editorial_prompts"
    )

# Content DB / queue / articles
@router.callback_query(F.data == "auto_content_db")
async def auto_content_db(call: CallbackQuery, db: D1Database):
    if not await admin_ok(call):
        return
    await call.answer()
    rows = await db.execute(
        "SELECT (SELECT COUNT(*) FROM articles) articles, (SELECT COUNT(*) FROM publication_queue WHERE status='queued') queued, (SELECT COUNT(*) FROM test_history) tests"
    )
    r = rows[0] if rows else {}
    text = (
        "🗃 <b>محتوا و داده‌های اتوماسیون</b>\n"
        f"📥 صف: <b>{r.get('queued', 0)}</b>\n"
        f"📰 مقالات تولیدشده: <b>{r.get('articles', 0)}</b>\n"
        f"🧪 سوابق تست: <b>{r.get('tests', 0)}</b>\n"
        "منابع و مدل‌های AI پاک نمی‌شوند؛ فقط داده‌های محتوایی اتوماسیون مدیریت می‌شوند."
    )
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=automation_content_db_kb())

@router.callback_query(F.data == "auto_db")
async def auto_db(call: CallbackQuery, db: D1Database):
    if not await admin_ok(call):
        return
    await call.answer()
    rows = await db.execute(
        "SELECT (SELECT COUNT(*) FROM articles) articles, (SELECT COUNT(*) FROM publication_queue) queue_all, (SELECT COUNT(*) FROM automation_logs) logs, (SELECT COUNT(*) FROM test_history) tests"
    )
    r = rows[0] if rows else {}
    text = (
        "🗄 <b>دیتای اتوماسیون</b>\n"
        f"📰 مقالات: <b>{r.get('articles', 0)}</b>\n"
        f"📥 رکوردهای صف: <b>{r.get('queue_all', 0)}</b>\n"
        f"🧪 سوابق تست: <b>{r.get('tests', 0)}</b>\n"
        f"📜 لاگ‌ها: <b>{r.get('logs', 0)}</b>\n"
        "⚠️ حذف همه داده‌های محتوایی برگشت‌پذیر نیست. منابع، تنظیمات و مدل‌های AI حفظ می‌شوند."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗑️ حذف کل دیتای محتوایی", callback_data="auto_db_delete_confirm")],
        [InlineKeyboardButton(text="🔙 محتوا و داده‌ها", callback_data="auto_content_db")],
    ])
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=kb)

@router.callback_query(F.data == "auto_db_delete_confirm")
async def auto_db_delete_confirm(call: CallbackQuery):
    if not await admin_ok(call):
        return
    await call.answer()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚠️ بله، همه داده‌ها حذف شود", callback_data="auto_db_delete_yes")],
        [InlineKeyboardButton(text="↩️ لغو", callback_data="auto_db")],
    ])
    await call.message.edit_text(
        "⚠️ <b>تأیید حذف کامل دیتای اتوماسیون</b>\nهمه مقالات، صف، سوابق تست و لاگ‌ها حذف می‌شوند. منابع و مدل‌ها باقی می‌مانند.\nادامه می‌دهی؟",
        parse_mode="HTML", reply_markup=kb
    )

@router.callback_query(F.data == "auto_db_delete_yes")
async def auto_db_delete_yes(call: CallbackQuery, db: D1Database):
    if not await admin_ok(call):
        return
    await call.answer()
    await db.execute_batch([
        {"sql": "DELETE FROM publication_queue"},
        {"sql": "DELETE FROM articles"},
        {"sql": "DELETE FROM test_history"},
        {"sql": "DELETE FROM automation_logs"},
    ])
    now = datetime.now(timezone.utc).isoformat()
    await db.execute("UPDATE sources SET last_checked_at=NULL, next_check_at=?", [now])
    invalidate_sources()
    await call.message.edit_text("✅ <b>داده‌های محتوایی اتوماسیون پاک شد.</b>", parse_mode="HTML", reply_markup=automation_content_db_kb())

@router.callback_query(F.data == "auto_queue")
async def auto_queue(call: CallbackQuery, db: D1Database):
    if not await admin_ok(call):
        return
    await call.answer()
    est = await next_publication_estimate(db)
    rows = await db.execute(
        "SELECT q.id,q.article_id,q.status,q.attempts,a.title,a.score,a.category,a.deep_views FROM publication_queue q JOIN articles a ON a.id=q.article_id WHERE q.status='queued' ORDER BY COALESCE(a.source_published_at,a.created_at) DESC, a.score DESC, q.created_at ASC LIMIT 20"
    )

    last_txt = est["latest"].astimezone(pytz.timezone("Asia/Tehran")).strftime("%H:%M") if est["latest"] else "هنوز منتشر نشده"
    if est["minutes"] <= 0:
        next_txt = "آماده انتشار طبق برنامه"
    elif est["minutes"] < 60:
        next_txt = f"حدود {est['minutes']} دقیقه دیگر"
    else:
        next_txt = f"حدود {est['minutes'] // 60} ساعت و {est['minutes'] % 60} دقیقه دیگر"

    text = (
        "📥 <b>صف انتشار</b>\n"
        f"📦 تعداد در صف: <b>{est['queued']}</b>\n"
        f"🕘 آخرین انتشار: <b>{last_txt}</b>\n"
        f"⏱ فاصله مدیریت‌شده: <b>{est['interval_minutes']} دقیقه</b>\n"
        f"🕐 نوبت بعدی: <b>{next_txt}</b>\n\n"
    )

    if not rows:
        text += "صف فعلاً خالی است."

    kb_rows = []
    for r in rows:
        text += f"#{r['article_id']} · ⭐ {float(r['score'] or 0):.0f} · {str(r['title'])[:70]}\n"
        kb_rows.append([InlineKeyboardButton(text=f"📄 #{r['article_id']} · {str(r['title'])[:22]}", callback_data=f"auto_art_{r['article_id']}")])

    kb_rows += [
        [InlineKeyboardButton(text="🚀 همین حالا منتشر کن", callback_data="auto_publish_now"), InlineKeyboardButton(text="🔄 بروزرسانی", callback_data="auto_queue")],
        [InlineKeyboardButton(text="📰 محتوای تولیدشده", callback_data="auto_articles")],
        [InlineKeyboardButton(text="🔙 محتوا و داده‌ها", callback_data="auto_content_db")],
    ]
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows))

@router.callback_query(F.data == "auto_publish_now")
async def auto_publish_now(call: CallbackQuery, db: D1Database, bot: Bot):
    if not await admin_ok(call):
        return
    try:
        ok = await publish_next_article(db, bot, force=True)
        msg = "✅ اولین محتوای صف همین حالا منتشر شد." if ok else "⚠️ محتوای آماده‌ای برای انتشار فوری پیدا نشد یا سقف روزانه تکمیل شده است."
    except Exception as e:
        msg = "❌ انتشار فوری شکست خورد:\n" + html.escape(str(e)[:1000])
    await call.message.edit_text(msg, parse_mode="HTML", reply_markup=get_admin_back_kb("auto_queue"))
    await call.answer()

@router.callback_query(F.data == "auto_articles")
async def auto_articles(call: CallbackQuery, db: D1Database):
    if not await admin_ok(call):
        return
    await call.answer()
    rows = await db.execute("SELECT id,title,score,status,category,created_at,published_at,deep_views FROM articles ORDER BY id DESC LIMIT 20")
    text = "📰 <b>محتوای تولیدشده</b>\n"
    kb = []
    if not rows:
        text += "هنوز محتوایی تولید نشده."
    for r in rows:
        text += f"#{r['id']} · {'✅' if r.get('status') == 'published' else '📝'} · ⭐{float(r.get('score') or 0):.0f} · {str(r.get('title') or '')[:70]}\n"
        kb.append([InlineKeyboardButton(text=f"📄 مشاهده #{r['id']}", callback_data=f"auto_art_{r['id']}")])
    kb += [[InlineKeyboardButton(text="🔙 محتوا و داده‌ها", callback_data="auto_content_db")]]
    await call.message.edit_text(text[:4000], parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

async def render_automation_article(call: CallbackQuery, db: D1Database, article_id: int):
    rows = await db.execute(
        "SELECT a.*, q.status q_status, q.scheduled_at FROM articles a LEFT JOIN publication_queue q ON q.article_id=a.id WHERE a.id=?",
        [article_id]
    )
    if not rows:
        await call.answer("محتوا پیدا نشد", show_alert=True)
        return

    a = rows[0]
    ch = plain_len(a.get("channel_text") or "")
    ar = plain_len(a.get("body") or "")

    text = (
        f"📰 <b>محتوا #{article_id}</b>\n"
        f"<b>{html.escape(str(a.get('title') or 'بدون عنوان'))}</b>\n"
        f"📌 وضعیت: <b>{html.escape(str(a.get('status') or '-'))}</b>\n"
        f"⭐ امتیاز: <b>{float(a.get('score') or 0):.1f}</b>\n"
        f"📥 وضعیت صف: <b>{html.escape(str(a.get('q_status') or 'ندارد'))}</b>\n"
        f"📏 کانال: <b>{ch}</b> کاراکتر · مقاله: <b>{ar}</b> کاراکتر\n"
        f"👁 بازشدن Deep Link: <b>{int(a.get('deep_views') or 0)}</b>\n"
        f"🌐 منبع: <code>{html.escape(str(a.get('source_url') or '-'))}</code>\n"
        "از اینجا می‌توانی محتوای تولیدشده را ببینی، ویرایش یا حذف کنی."
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👁 مشاهده متن", callback_data=f"auto_art_view_{article_id}")],
        [InlineKeyboardButton(text="✏️ عنوان", callback_data=f"auto_art_edit_title_{article_id}"), InlineKeyboardButton(text="✏️ متن کانال", callback_data=f"auto_art_edit_channel_{article_id}")],
        [InlineKeyboardButton(text="✏️ متن کامل", callback_data=f"auto_art_edit_body_{article_id}"), InlineKeyboardButton(text="📊 آمار", callback_data=f"auto_art_stats_{article_id}")],
        [InlineKeyboardButton(text="🗑 حذف", callback_data=f"auto_art_delete_{article_id}")],
        [InlineKeyboardButton(text="🔙 " + ("صف انتشار" if a.get("q_status") == "queued" else "محتوای تولیدشده"), callback_data="auto_queue" if a.get("q_status") == "queued" else "auto_articles")],
    ])
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    await call.answer()

@router.callback_query(F.data.regexp(r"^auto_art_(\d+)$"))
async def auto_art_view_callback(call: CallbackQuery, db: D1Database):
    if not await admin_ok(call):
        return
    await render_automation_article(call, db, int(call.data.split("_")[-1]))

@router.callback_query(F.data.regexp(r"^auto_art_view_(\d+)$"))
async def auto_art_view_text(call: CallbackQuery, db: D1Database, bot: Bot):
    if not await admin_ok(call):
        return
    aid = int(call.data.split("_")[-1])
    rows = await db.execute("SELECT title, channel_text, body, status FROM articles WHERE id=?", [aid])
    if not rows:
        await call.answer("محتوا پیدا نشد", show_alert=True)
        return

    a = rows[0]
    head = (
        f"📄 <b>{html.escape(str(a.get('title') or ''))}</b>\n"
        f"📢 <b>نسخه کانال:</b>\n{sanitize_telegram_html(a.get('channel_text') or '')}\n"
        "📖 <b>نسخه کامل:</b>\n"
    )
    body = sanitize_telegram_html(a.get("body") or "")
    full = head + body
    chunks = split_html_safe(full, 3800)
    if not chunks:
        chunks = [head + "(متن کامل خالی است.)"]

    await call.message.edit_text(chunks[0][:4000], parse_mode="HTML", reply_markup=get_admin_back_kb(f"auto_art_{aid}"))
    for chunk in chunks[1:]:
        try:
            await bot.send_message(call.message.chat.id, chunk[:4000], parse_mode="HTML")
        except Exception:
            try:
                await bot.send_message(call.message.chat.id, strip_html_text(chunk)[:4000])
            except Exception:
                pass
    await call.answer()

@router.callback_query(F.data.regexp(r"^auto_art_stats_(\d+)$"))
async def auto_art_stats(call: CallbackQuery, db: D1Database):
    if not await admin_ok(call):
        return
    aid = int(call.data.split("_")[-1])
    rows = await db.execute("SELECT a.*, q.status q_status FROM articles a LEFT JOIN publication_queue q ON q.article_id=a.id WHERE a.id=?", [aid])
    if not rows:
        await call.answer("محتوا پیدا نشد", show_alert=True)
        return

    a = rows[0]
    text = (
        f"📊 <b>آمار محتوا #{aid}</b>\nعنوان: <b>{html.escape(str(a.get('title') or ''))}</b>\n"
        f"امتیاز: <b>{float(a.get('score') or 0):.1f}</b>\nوضعیت: <b>{html.escape(str(a.get('status') or '-'))}</b>\n"
        f"صف: <b>{html.escape(str(a.get('q_status') or 'ندارد'))}</b>\nDeep Link: <b>{int(a.get('deep_views') or 0)} بار</b>\n"
        f"تولید: <b>{html.escape(str(a.get('created_at') or '-'))}</b>\nانتشار: <b>{html.escape(str(a.get('published_at') or '-'))}</b>"
    )
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=get_admin_back_kb(f"auto_art_{aid}"))
    await call.answer()

@router.callback_query(F.data.regexp(r"^auto_art_edit_(title|channel|body)_(\d+)$"))
async def auto_art_edit_start(call: CallbackQuery, state: FSMContext, db: D1Database):
    if not await admin_ok(call):
        return
    parts = call.data.split("_")
    field = parts[2]
    aid = int(parts[3])
    rows = await db.execute("SELECT title, channel_text, body FROM articles WHERE id=?", [aid])
    if not rows:
        await call.answer("محتوا پیدا نشد", show_alert=True)
        return

    labels = {"title": "عنوان", "channel": "متن کانال", "body": "متن کامل"}
    await state.set_state(BotStates.automation_article_edit)
    await state.update_data(article_edit_id=aid, article_edit_field=field, parent_message_id=call.message.message_id)
    await call.message.edit_text(f"✏️ <b>ویرایش {labels[field]} #{aid}</b>\nمقدار جدید را بفرست.", parse_mode="HTML", reply_markup=get_exit_menu())
    await call.answer()

@router.message(F.chat.id == ADMIN_ID, StateFilter(BotStates.automation_article_edit))
async def auto_art_edit_input(message: Message, state: FSMContext, db: D1Database, bot: Bot):
    data = await state.get_data()
    aid = int(data["article_edit_id"])
    field = data["article_edit_field"]
    value = (message.text or message.caption or "").strip()

    if not value:
        await message.answer("❌ مقدار خالی است؛ دوباره بفرست.", reply_markup=get_exit_menu())
        return

    col = {"title": "title", "channel": "channel_text", "body": "body"}[field]
    if field == "title":
        value = strip_html_text(value)[:500]
    elif field == "channel":
        value = sanitize_telegram_html(value)[:5000]
    else:
        value = sanitize_telegram_html(value)[:18000]

    await db.execute(f"UPDATE articles SET {col}=? WHERE id=?", [value, aid])

    rows = await db.execute("SELECT published_message_id,status,deep_token,title,channel_text FROM articles WHERE id=?", [aid])
    if rows and rows[0].get("status") == "published" and rows[0].get("published_message_id"):
        try:
            channel_id = await get_channel_id(db)
            token = rows[0].get("deep_token")
            username = await get_runtime_bot_username(bot)
            deep = f"https://t.me/{username}?start=auto_{token}" if token and username else ""

            if field in {"title", "channel"} and deep:
                latest = await db.execute("SELECT title, channel_text FROM articles WHERE id=?", [aid])
                cap = publication_caption(latest[0].get("title") or "", latest[0].get("channel_text") or "", deep)
                try:
                    await bot.edit_message_caption(chat_id=channel_id, message_id=int(rows[0]["published_message_id"]), caption=cap, parse_mode="HTML")
                except Exception:
                    try:
                        await bot.edit_message_text(chat_id=channel_id, message_id=int(rows[0]["published_message_id"]), text=cap, parse_mode="HTML")
                    except Exception:
                        pass
        except Exception:
            pass

    await state.set_state(BotStates.idle)
    await message.answer(
        f"✅ {field} محتوای #{aid} ویرایش شد.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📄 مدیریت همین محتوا", callback_data=f"auto_art_{aid}")],
            [InlineKeyboardButton(text="🔙 محتوا و داده‌ها", callback_data="auto_content_db")],
        ])
    )

@router.callback_query(F.data.regexp(r"^auto_art_delete_(\d+)$"))
async def auto_art_delete(call: CallbackQuery):
    if not await admin_ok(call):
        return
    aid = int(call.data.split("_")[-1])
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗑️ بله، حذف شود", callback_data=f"auto_art_delete_yes_{aid}")],
        [InlineKeyboardButton(text="↩️ لغو", callback_data=f"auto_art_{aid}")],
    ])
    await call.message.edit_text(
        f"⚠️ <b>حذف محتوای #{aid}</b>\nاین عمل مقاله و صف آن را حذف می‌کند. ادامه می‌دهی؟",
        parse_mode="HTML", reply_markup=kb
    )
    await call.answer()

@router.callback_query(F.data.regexp(r"^auto_art_delete_yes_(\d+)$"))
async def auto_art_delete_yes(call: CallbackQuery, db: D1Database):
    if not await admin_ok(call):
        return
    aid = int(call.data.split("_")[-1])
    await db.execute("DELETE FROM publication_queue WHERE article_id=?", [aid])
    await db.execute("DELETE FROM articles WHERE id=?", [aid])
    await db.execute("DELETE FROM user_content_saves WHERE content_type='article' AND content_id=?", [aid])
    await db.execute("DELETE FROM user_content_votes WHERE content_type='article' AND content_id=?", [aid])
    await call.message.edit_text("🗑️ <b>محتوا حذف شد.</b>", parse_mode="HTML", reply_markup=automation_content_db_kb())
    await call.answer("حذف شد")

# Health (light)
@router.callback_query(F.data == "auto_health")
async def auto_health(call: CallbackQuery, db: D1Database, bot: Bot):
    if not await admin_ok(call):
        return
    await call.answer()

    channel = await get_channel_id(db)
    providers = await db.execute("SELECT status, enabled FROM ai_providers")
    healthy = sum(1 for p in providers if p.get("enabled") and p.get("status") == "healthy")
    sources = await db.execute("SELECT COUNT(*) c FROM sources WHERE enabled=1")

    text = (
        f"🧪 <b>تست و سلامت</b>\n"
        f"D1: {'✅ آماده' if db.session and not db.session.closed else '❌'}\n"
        f"کانال: {'✅ تنظیم شده' if channel else '❌ تنظیم نشده'}\n"
        f"مدل سالم: {healthy}/{len(providers)}\n"
        f"منبع فعال: {sources[0].get('c', 0) if sources else 0}\n"
        "از اینجا تست‌ها را مرحله‌به‌مرحله اجرا کن."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🤖 تست مدل‌ها", callback_data="health_test_ai")],
        [InlineKeyboardButton(text="🧪 تولید بدون انتشار", callback_data="health_dry_run")],
        [InlineKeyboardButton(text="🚦 وضعیت اجرا", callback_data="health_deployment")],
        [InlineKeyboardButton(text="📜 لاگ اتوماسیون", callback_data="health_logs")],
        [InlineKeyboardButton(text="🔄 بروزرسانی", callback_data="auto_health"), InlineKeyboardButton(text="🔙 اتوماسیون", callback_data="auto_back")],
    ])
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=kb)

@router.callback_query(F.data == "health_test_ai")
async def health_test_ai(call: CallbackQuery, db: D1Database):
    if not await admin_ok(call):
        return
    rows = await db.execute(
        "SELECT id,name,model_name,priority,status FROM ai_providers WHERE enabled=1 ORDER BY priority ASC,id ASC LIMIT 5"
    )
    if not rows:
        await call.message.edit_text("❌ هیچ مدل فعالی نیست.", reply_markup=get_admin_back_kb("auto_health"))
        return

    await call.answer("تست مدل‌ها شروع شد…")
    await log_automation(db, "INFO", "health_test_ai_started", "manual AI provider test")
    m = AIProviderManager(db, None)
    results = []

    try:
        for p in rows:
            result = await m.test_provider(int(p["id"]))
            if result.get("ok"):
                results.append(f"✅ {html.escape(str(p.get('model_name')))} · {result.get('latency_ms', 0)}ms")
            else:
                results.append(f"❌ {html.escape(str(p.get('model_name')))}\n<code>{html.escape(str(result.get('error', 'unknown'))[:600])}</code>")
    finally:
        await m.close()

    await log_automation(db, "INFO", "health_test_ai_finished", f"providers={len(rows)}")
    await call.message.edit_text("🧪 <b>نتیجه تست مدل‌های AI</b>\n" + "\n".join(results), reply_markup=get_admin_back_kb("auto_health"))
    await call.answer()

@router.callback_query(F.data == "health_dry_run")
async def health_dry_run(call: CallbackQuery, db: D1Database, bot: Bot):
    if not await admin_ok(call):
        return
    await call.answer("تست تولید شروع شد…")

    sources = await get_enabled_sources(db)
    if not sources:
        await call.message.edit_text("❌ هیچ منبع فعالی وجود ندارد.", reply_markup=get_admin_back_kb("auto_health"))
        return

    ai = AIProviderManager(db, bot)
    try:
        src = sources[0]
        freshness = float(await get_setting(db, "webscout_freshness_hours", str(WEBSCOUT_FRESHNESS_HOURS)) or WEBSCOUT_FRESHNESS_HOURS)
        weights = {k: float(await get_setting(db, "weight_" + k, "10")) for k in
                   ["global", "technology", "ai", "cyber", "education", "iran", "freshness", "source", "novelty"]}
        prompts = await get_manager_editorial_prompts(db)

        prompt = (
            f"Inspect TARGET URL with your web tools and find the newest substantive item published within the last {freshness:g} hours.\n"
            f"Manager weights: {json.dumps(weights, ensure_ascii=False)}\n"
            f"Channel instruction: {prompts.get('channel', '')}\n"
            f"Article instruction: {prompts.get('article', '')}\n"
            "Return exactly FALSE if no match. Otherwise return JSON with title, article_url, published_at, image_url, score, research_text, resource_links, facts."
        )

        scout = await ai.webscout_call(src.get("url") or "", prompt, max_tokens=8000)
        if not scout.get("ok"):
            await call.message.edit_text("❌ WebScout ناموفق بود:\n<code>" + html.escape(str(scout.get("error"))[:1800]) + "</code>", parse_mode="HTML", reply_markup=get_admin_back_kb("auto_health"))
            return

        raw = str(scout.get("content") or "").strip()
        if raw.upper() == "FALSE":
            await call.message.edit_text("🟡 موردی برای تست پیدا نشد.", reply_markup=get_admin_back_kb("auto_health"))
            return

        obj = parse_json_object(raw)
        if not obj or not obj.get("research_text"):
            await call.message.edit_text("⚠️ پاسخ WebScout قابل پردازش نیست.", reply_markup=get_admin_back_kb("auto_health"))
            return

        item = {
            "title": strip_html_text(str(obj.get("title") or ""))[:500],
            "url": normalize_url(str(obj.get("article_url") or src.get("url") or "")),
            "description": "",
            "body": str(obj.get("research_text") or ""),
            "webscout_research": str(obj.get("research_text") or ""),
            "image_url": normalize_url(str(obj.get("image_url") or "")),
            "published_at": str(obj.get("published_at") or ""),
            "links": obj.get("resource_links") or [],
        }

        out = await ai_editorial_process(ai, item, src, [], weights, prompts)
        if out.get("error"):
            await call.message.edit_text("❌ <b>تست تولید شکست خورد</b>\n<code>" + html.escape(str(out["error"])[:1800]) + "</code>", parse_mode="HTML", reply_markup=get_admin_back_kb("auto_health"))
            return

        ch = out.get("channel_html") or out.get("channel_text") or ""
        ar = out.get("article_html") or out.get("article_text") or ""
        msg = (
            "✅ <b>تست تولید واقعی موفق شد.</b>\n"
            f"🌐 منبع: <b>{html.escape(str(src.get('name')))}</b>\n"
            f"📰 عنوان: <b>{html.escape(str(out.get('title') or item.get('title')))}</b>\n"
            f"📊 امتیاز: <b>{out.get('score', '-')}</b>\n"
            "<b>📝 کانال:</b>\n" + ch[:1200] + "\n"
            "<b>📖 مقاله:</b>\n" + ar[:3500] + "\n"
            "🚫 <b>انتشار انجام نشد.</b>"
        )
        await call.message.edit_text(msg, parse_mode="HTML", reply_markup=get_admin_back_kb("auto_health"))
    except Exception as e:
        await call.message.edit_text("❌ <b>تست تولید شکست خورد</b>\n<code>" + html.escape(str(e)[:2000]) + "</code>", parse_mode="HTML", reply_markup=get_admin_back_kb("auto_health"))
    finally:
        await ai.close()

@router.callback_query(F.data == "health_deployment")
async def health_deployment(call: CallbackQuery, db: D1Database):
    if not await admin_ok(call):
        return
    await call.answer()
    hb = await get_setting(db, "worker_heartbeat_at", "")
    started = await get_setting(db, "worker_started_at", "")
    cycle = await get_setting(db, "last_cycle_finished_at", "")
    now = datetime.now(timezone.utc)
    hb_age = None

    if hb:
        try:
            hb_age = int((now - datetime.fromisoformat(hb.replace("Z", "+00:00"))).total_seconds())
        except Exception:
            hb_age = None

    alive = hb_age is not None and hb_age < 600
    text = (
        f"🚦 <b>وضعیت اجرا / Deployment</b>\n📦 نسخه: <code>{BUILD_VERSION}</code>\n"
        f"🤖 Worker: {'🟢 زنده' if alive else '🔴 Heartbeat دریافت نمی‌شود'}\n"
        f"⚙️ اتوماسیون: {'🟢 فعال' if await get_setting(db, 'automation_enabled', '0') == '1' else '🔴 خاموش'}\n"
        f"💓 آخرین Heartbeat: {(str(hb_age) + ' ثانیه قبل') if hb_age is not None else 'نداریم'}\n"
        f"🚀 Worker شروع شد: {started or 'نامشخص'}\n🔄 آخرین چرخه: {cycle or 'هنوز اجرا نشده'}"
    )
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=get_admin_back_kb("auto_health"))

@router.callback_query(F.data == "health_logs")
async def health_logs(call: CallbackQuery, db: D1Database):
    if not await admin_ok(call):
        return
    await call.answer()
    rows = await db.execute("SELECT level,event,details,created_at FROM automation_logs ORDER BY id DESC LIMIT 20")
    names = {
        "published": "انتشار موفق",
        "publication_failed": "خطای انتشار",
        "content_rejected": "رد محتوا",
        "automation_loop_failed": "خطای لوپ اتوماسیون",
        "health_test_ai_started": "شروع تست مدل‌ها",
        "health_test_ai_finished": "پایان تست مدل‌ها",
    }
    text = "📜 <b>لاگ کوتاه و زنده اتوماسیون</b>\n"
    if not rows:
        text += "هنوز لاگی ثبت نشده است."
    else:
        for r in rows:
            ev = str(r.get("event") or "")
            label = names.get(ev, ev)
            tm = html.escape(str(r.get("created_at") or ""))[11:19]
            detail = html.escape(str(r.get("details") or "")[:350])
            text += f"<b>{tm} · {html.escape(label)}</b>\n{detail}\n\n"

    await call.message.edit_text(text[:4000], parse_mode="HTML", reply_markup=get_admin_back_kb("auto_health"))

# Channel post tracking
@router.channel_post()
async def on_channel_post(message: Message, db: D1Database):
    configured_channel = await get_channel_id(db)
    if not configured_channel:
        return

    match = False
    if configured_channel.startswith("@"):
        match = bool(message.chat.username) and ("@" + message.chat.username.lower()) == configured_channel.lower()
    else:
        match = str(message.chat.id) == str(configured_channel)

    if not match:
        return

    rows = await db.execute("SELECT id FROM articles WHERE published_message_id=?", [message.message_id])
    if rows:
        return

    now = datetime.now(timezone.utc).isoformat()
    await set_setting(db, "last_manual_channel_post_at", now)

# ============================================================
# Admin content management
# ============================================================
async def send_admin_all_posts_page(bot: Bot, chat_id: int, rows: List[Dict[str, Any]], page: int, total_pages: int, total_count: int, edit_message_id: Optional[int] = None):
    text = "📋 <b>همه محتواها</b>\n" + f"صفحه {page + 1}/{total_pages} · مجموع {total_count}\n"
    buttons = []

    for p in rows:
        preview = html.escape(strip_html_text(p.get("text") or "")[:60].replace("\n", " "))
        text += f"\n<b>#{p['id']}</b> {preview}"
        buttons.append([
            InlineKeyboardButton(text=f"✏️ #{p['id']}", callback_data=f"aedit_{p['id']}"),
            InlineKeyboardButton(text=f"🗑 #{p['id']}", callback_data=f"adelete_{p['id']}"),
        ])

    buttons.append([
        InlineKeyboardButton(text="⏮", callback_data=f"adm_all_page_prev_{page}"),
        InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data="noop"),
        InlineKeyboardButton(text="⏭", callback_data=f"adm_all_page_next_{page}"),
    ])
    buttons.append([InlineKeyboardButton(text="🔙 مدیریت محتوا", callback_data="admin_content")])
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)

    if edit_message_id:
        try:
            await bot.edit_message_text(chat_id=chat_id, message_id=edit_message_id, text=text, parse_mode="HTML", reply_markup=kb)
            return
        except Exception:
            pass

    await bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=kb)

@router.callback_query(F.data == "adm_view_all")
async def callback_admin_view_all(call: CallbackQuery):
    if not await admin_ok(call):
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ نمایش", callback_data="adm_view_all_confirm")],
        [InlineKeyboardButton(text="❌ لغو", callback_data="adm_view_all_cancel")],
    ])
    await call.message.edit_text("📋 <b>همه محتواها</b>\nبرای نمایش ادامه بده:", parse_mode="HTML", reply_markup=kb)
    await call.answer()

@router.callback_query(F.data == "adm_view_all_cancel")
async def callback_admin_view_all_cancel(call: CallbackQuery):
    if not await admin_ok(call):
        return
    await call.message.edit_text("📁 <b>مدیریت محتوای هسته</b>", parse_mode="HTML", reply_markup=get_content_management_kb())
    await call.answer()

@router.callback_query(F.data == "adm_view_all_confirm")
async def callback_admin_view_all_confirm(call: CallbackQuery, state: FSMContext, db: D1Database, bot: Bot):
    if not await admin_ok(call):
        return
    per_page = 10
    total = (await db.execute("SELECT COUNT(*) c FROM posts WHERE deleted=0"))[0].get("c", 0)
    pages = max(1, math.ceil(total / per_page))
    rows = await db.execute("SELECT id,text,likes,dislikes,views,file_id,media_type FROM posts WHERE deleted=0 ORDER BY id DESC LIMIT ? OFFSET ?", [per_page, 0])

    await state.set_state(BotStates.admin_view_all)
    await state.update_data(all_posts_page=0, all_per_page=per_page, all_total_pages=pages, all_total_count=total)

    if rows:
        await send_admin_all_posts_page(bot, call.message.chat.id, rows, 0, pages, total, call.message.message_id)
    else:
        await call.message.edit_text("📭 محتوایی وجود ندارد.", reply_markup=get_content_management_kb())
    await call.answer()

@router.callback_query(F.data.startswith("adm_all_page_"))
async def callback_admin_all_posts_page(call: CallbackQuery, state: FSMContext, db: D1Database, bot: Bot):
    if not await admin_ok(call):
        return
    parts = call.data.split("_")
    direction = parts[3]
    current = int(parts[4])
    data = await state.get_data()

    per = int(data.get("all_per_page", 10))
    total_pages = int(data.get("all_total_pages", 1))
    total = int(data.get("all_total_count", 0))

    new = max(0, min(current + (1 if direction == "next" else -1), total_pages - 1))
    rows = await db.execute("SELECT id,text,likes,dislikes,views,file_id,media_type FROM posts WHERE deleted=0 ORDER BY id DESC LIMIT ? OFFSET ?", [per, new * per])
    await state.update_data(all_posts_page=new)

    if rows:
        await send_admin_all_posts_page(bot, call.message.chat.id, rows, new, total_pages, total, call.message.message_id)
    await call.answer()

@router.callback_query(F.data == "adm_search_text")
async def callback_admin_search_text(call: CallbackQuery, state: FSMContext):
    if not await admin_ok(call):
        return
    await state.set_state(BotStates.admin_search_word)
    await state.update_data(search_ids=[], search_index=0)
    await call.message.edit_text("🔍 <b>جستجو</b>\nکلمه کلیدی یا شماره پست را بفرست.", parse_mode="HTML", reply_markup=get_exit_menu())
    await call.answer()

@router.message(F.chat.id == ADMIN_ID, StateFilter(BotStates.admin_search_word))
async def process_admin_search_word(message: Message, state: FSMContext, db: D1Database, bot: Bot):
    q = (message.text or "").strip()
    if not q:
        return

    if q.isdigit():
        rows = await db.execute("SELECT id FROM posts WHERE id=? AND deleted=0", [int(q)])
    else:
        rows = await db.execute("SELECT id FROM posts WHERE text LIKE ? AND deleted=0 ORDER BY id DESC LIMIT 50", [f"%{q}%"])

    ids = [r["id"] for r in rows]
    if not ids:
        await message.answer(
            "❌ چیزی پیدا نشد.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔍 دوباره", callback_data="adm_search_text"), InlineKeyboardButton(text="🔙 مدیریت محتوا", callback_data="admin_content")]
            ])
        )
        return

    await state.update_data(search_ids=ids, search_index=0)
    await state.set_state(BotStates.admin_search_word)

    p = await db.execute("SELECT id,text,file_id,media_type FROM posts WHERE id=?", [ids[0]])
    if p:
        await bot.send_message(message.chat.id, "نتیجه:", reply_markup=get_admin_search_pagination_kb(ids[0], 0))
        await send_post_content(bot, message.chat.id, p[0], get_admin_search_pagination_kb(ids[0], 0))

@router.callback_query(F.data.startswith("asearch_"))
async def callback_admin_search_pagination(call: CallbackQuery, state: FSMContext, db: D1Database, bot: Bot):
    if not await admin_ok(call):
        return
    parts = call.data.split("_")
    direction = parts[1]
    current = int(parts[2])
    data = await state.get_data()
    ids = data.get("search_ids", [])

    if not ids:
        await call.answer("جستجو تمام شده است", show_alert=True)
        return

    new = max(0, min(current + (1 if direction == "next" else -1), len(ids) - 1))
    await state.update_data(search_index=new)

    p = await db.execute("SELECT id,text,file_id,media_type FROM posts WHERE id=?", [ids[new]])
    if p:
        kb = get_admin_search_pagination_kb(ids[new], new)
        if p[0].get("file_id"):
            try:
                await call.message.delete()
            except Exception:
                pass
            await send_post_content(bot, call.message.chat.id, p[0], kb)
        else:
            await call.message.edit_text(p[0].get("text") or "", reply_markup=kb)
    await call.answer()

@router.callback_query(F.data.startswith("aedit_"))
async def admin_edit_post_start(call: CallbackQuery, state: FSMContext, db: D1Database):
    if not await admin_ok(call):
        return
    pid = int(call.data.split("_")[1])
    rows = await db.execute("SELECT id,text FROM posts WHERE id=? AND deleted=0", [pid])
    if not rows:
        await call.answer("پست پیدا نشد", show_alert=True)
        return

    await state.set_state(BotStates.admin_post_edit)
    await state.update_data(edit_post_id=pid, parent_message_id=call.message.message_id)
    await call.message.edit_text(f"✏️ <b>ویرایش #{pid}</b>\nمتن جدید را بفرست.", parse_mode="HTML", reply_markup=get_exit_menu())
    await call.answer()

@router.message(F.chat.id == ADMIN_ID, StateFilter(BotStates.admin_post_edit))
async def admin_edit_post_input(message: Message, state: FSMContext, db: D1Database):
    data = await state.get_data()
    pid = int(data["edit_post_id"])
    new_text = message.text or message.caption or ""

    if not new_text:
        await message.answer("❌ متن خالی است.")
        return

    await db.execute("UPDATE posts SET text=? WHERE id=?", [new_text, pid])
    await state.set_state(BotStates.idle)
    await message.answer(f"✅ پست #{pid} ویرایش شد.", reply_markup=get_content_management_kb())

@router.callback_query(F.data.startswith("astats_"))
async def callback_admin_search_post_stats(call: CallbackQuery, db: D1Database):
    if not await admin_ok(call):
        return
    pid = int(call.data.split("_")[1])
    p = await db.execute("SELECT likes,dislikes,views FROM posts WHERE id=?", [pid])
    if not p:
        await call.answer("پست پیدا نشد", show_alert=True)
        return
    await call.answer(f"👁 {p[0].get('views', 0)} | 👍 {p[0].get('likes', 0)} | 👎 {p[0].get('dislikes', 0)}", show_alert=True)

@router.callback_query(F.data.startswith("adelete_"))
async def callback_admin_delete_post(call: CallbackQuery, db: D1Database):
    if not await admin_ok(call):
        return
    pid = int(call.data.split("_")[1])
    p = await db.execute("SELECT text FROM posts WHERE id=? AND deleted=0", [pid])
    if not p:
        await call.answer("پست پیدا نشد", show_alert=True)
        return

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗑️ تأیید حذف", callback_data=f"adelete_confirm_{pid}"), InlineKeyboardButton(text="↩️ لغو", callback_data="admin_content")]
    ])
    await call.message.edit_text(f"⚠️ <b>حذف #{pid}</b>\nآیا مطمئنی؟", parse_mode="HTML", reply_markup=kb)
    await call.answer()

@router.callback_query(F.data.startswith("adelete_confirm_"))
async def callback_admin_delete_post_confirm(call: CallbackQuery, db: D1Database):
    if not await admin_ok(call):
        return
    pid = int(call.data.split("_")[-1])
    await db.execute("UPDATE posts SET deleted=1 WHERE id=?", [pid])
    await db.execute("DELETE FROM user_content_saves WHERE content_type='post' AND content_id=?", [pid])
    await db.execute("DELETE FROM user_content_votes WHERE content_type='post' AND content_id=?", [pid])
    await call.message.edit_text("🗑️ حذف شد.", reply_markup=get_content_management_kb())
    await call.answer("حذف شد")

# Confirm add/broadcast
@router.callback_query(F.data == "conf_add_yes")
async def callback_confirm_add_post_yes(call: CallbackQuery, state: FSMContext, db: D1Database):
    if not await admin_ok(call):
        return

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

            await state.update_data(temp_text=None, temp_file_id=None, temp_media_type=None)
            await state.set_state(BotStates.idle)
            await call.message.answer(f"✅ آرشیو شد!\n🔗 لینک:\nhttps://t.me/{BOT_USERNAME_RUNTIME or BOT_USERNAME}?start={post_id}")
            await call.answer("✅ ثبت شد!")
        except Exception as e:
            await call.answer(f"❌ خطا در ثبت: {e}", show_alert=True)
    else:
        await call.answer("❌ اطلاعات ناقص است", show_alert=True)

@router.callback_query(F.data == "conf_add_no")
async def callback_confirm_add_post_no(call: CallbackQuery, state: FSMContext):
    if not await admin_ok(call):
        return
    await state.update_data(temp_text=None, temp_file_id=None, temp_media_type=None)
    await state.set_state(BotStates.idle)
    await call.message.answer("❌ لغو شد.")
    await call.answer("لغو شد")

@router.callback_query(F.data == "conf_broad_yes")
async def callback_confirm_broadcast_yes(call: CallbackQuery, state: FSMContext, db: D1Database, bot: Bot):
    if not await admin_ok(call):
        return

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
        chunk = users[i:i + CHUNK_SIZE]
        tasks = [send_to_user(bot, u["id"], temp_text, temp_file_id, temp_media_type) for u in chunk]
        results = await asyncio.gather(*tasks)
        success_count += sum(1 for r in results if r)
        fail_count += sum(1 for r in results if not r)
        await asyncio.sleep(0.2)

    await state.update_data(temp_text=None, temp_file_id=None, temp_media_type=None)
    await state.set_state(BotStates.idle)
    await call.message.answer(f"✅ ارسال همگانی انجام شد.\nموفق: {success_count} نفر\nناموفق: {fail_count} نفر")
    await call.answer("✅ ارسال همگانی کامل شد!")

@router.callback_query(F.data == "conf_broad_no")
async def callback_confirm_broadcast_no(call: CallbackQuery, state: FSMContext):
    if not await admin_ok(call):
        return
    await state.update_data(temp_text=None, temp_file_id=None, temp_media_type=None)
    await state.set_state(BotStates.idle)
    await call.message.answer("❌ ارسال همگانی لغو شد.")
    await call.answer("لغو شد")

@router.callback_query(F.data == "noop")
async def callback_noop_dummy(call: CallbackQuery):
    await call.answer()

# ============================================================
# Main
# ============================================================
async def main():
    global BOT_USERNAME, BOT_USERNAME_RUNTIME

    if not API_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not configured")
    if not (CF_ACCOUNT_ID and CF_DATABASE_ID and CF_API_TOKEN):
        raise RuntimeError("Cloudflare D1 environment variables are not fully configured")

    bot = Bot(token=API_TOKEN)

    try:
        bot_identity = await bot.get_me()
        if bot_identity.username:
            BOT_USERNAME = bot_identity.username
            BOT_USERNAME_RUNTIME = bot_identity.username
    except Exception:
        pass

    dp = Dispatcher(storage=MemoryStorage())
    db = D1Database(
        account_id=CF_ACCOUNT_ID,
        database_id=CF_DATABASE_ID,
        api_token=CF_API_TOKEN,
    )

    await db.start()
    dp["db"] = db

    # Initialize DB
    await initialize_database(db)
    await migrate_unified_user_interactions(db)
    await initialize_automation_database(db)

    # Register rate limiting middleware
    dp.message.middleware(RateLimitMiddleware(ADMIN_ID))
    dp.callback_query.middleware(RateLimitMiddleware(ADMIN_ID))

    dp.include_router(router)

    automation_task = asyncio.create_task(automation_loop(db, bot))
    logger.info("Bot started successfully in Long Polling mode with fixed content automation...")

    try:
        await dp.start_polling(bot)
    finally:
        automation_task.cancel()
        try:
            await automation_task
        except asyncio.CancelledError:
            pass
        await db.close()
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())