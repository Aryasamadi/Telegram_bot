import os
import io
import re
import time
import math
import random
import logging
import asyncio
import html
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

import pytz
import aiohttp
from dotenv import load_dotenv

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

# تنظیمات اتصال به Cloudflare D1 REST API
CF_ACCOUNT_ID = os.getenv("CF_ACCOUNT_ID")
CF_DATABASE_ID = os.getenv("CF_DATABASE_ID")
CF_API_TOKEN = os.getenv("CF_API_TOKEN")

# تنظیمات مربوط به هوش مصنوعی
AI_API_KEY = os.getenv("AI_API_KEY")
AI_API_URL = os.getenv("AI_API_URL", "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions")
AI_MODEL_NAME = os.getenv("AI_MODEL_NAME", "gemini-1.5-flash")

# تنظیم لاگر برای خطایابی بهتر
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

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

    async def execute(self, sql: str, params: List[Any] = None) -> List[Dict[str, Any]]:
        payload = {"sql": sql}
        if params:
            payload["params"] = params

        async with aiohttp.ClientSession() as session:
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

    async def execute_batch(self, queries: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
        async with aiohttp.ClientSession() as session:
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


async def reset_database(db: D1Database):
    queries = [
        {"sql": "DROP TABLE IF EXISTS users"},
        {"sql": "DROP TABLE IF EXISTS posts"},
        {"sql": "DROP TABLE IF EXISTS saves"},
        {"sql": "DROP TABLE IF EXISTS user_states"},
        {"sql": "DROP TABLE IF EXISTS votes"},
        {"sql": "DROP TABLE IF EXISTS processed_updates"}
    ]
    await db.execute_batch(queries)
    await initialize_database(db)

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

# ============================================================
# بخش کیبوردهای ربات (Keyboards)
# ============================================================
FOLDER_NAMES = {
    "cyber": "🔒 امنیت سایبری",
    "tech": "💻 تکنولوژی و فناوری",
    "ai": "🧠 هوش مصنوعی",
    "edu": "📚 آموزش"
}

def get_main_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🤖 هوش مصنوعی"), KeyboardButton(text="💾 ذخیره‌های من")],
            [KeyboardButton(text="👤 پروفایل"), KeyboardButton(text="❓ راهنما")],
            [KeyboardButton(text="📞 ارتباط با مدیریت")]
        ],
        resize_keyboard=True
    )

def get_admin_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="➕ افزودن پست"), KeyboardButton(text="📁 مدیریت محتوا")],
            [KeyboardButton(text="📊 آمار"), KeyboardButton(text="📢 ارسال همگانی")],
            [KeyboardButton(text="کاربر")]
        ],
        resize_keyboard=True
    )

def get_exit_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="❌ خروج از نشست")]
        ],
        resize_keyboard=True
    )

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

@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext, db: D1Database, bot: Bot):
    user_id = message.from_user.id
    await register_user_if_not_exists(db, user_id)
    await state.set_state(BotStates.idle)
    state_data = await state.get_data()
    
    args = message.text.split()
    if len(args) > 1:
        post_id_str = args[1]
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

# ============================================================
# هدایت و فیلتر کردن دکمه‌های منوی اصلی و استیکی
# ============================================================
COMMANDS_LIST = [
    "کاربر", "مدیریت", "🤖 هوش مصنوعی", "💾 ذخیره‌های من", "📞 ارتباط با مدیریت",
    "❓ راهنما", "👤 پروفایل", "➕ افزودن پست", "📁 مدیریت محتوا",
    "📊 آمار", "📢 ارسال همگانی"
]

@router.message(F.text.in_(COMMANDS_LIST))
async def intercept_global_commands(message: Message, state: FSMContext, db: D1Database):
    text = message.text
    user_id = message.from_user.id
    state_data = await state.get_data()
    
    if text == "🤖 هوش مصنوعی":
        await state.set_state(BotStates.idle)
        history = [{"role": "system", "content": "You are a helpful assistant. Reply clearly in Persian."}]
        await state.set_state(BotStates.ai_chat)
        await state.update_data(ai_history=history)
        await message.answer(
            "سلام من هوش مصنوعی TechNowAi هستم 🦾\nچطور میتونم کمکت کنم ❔",
            reply_markup=get_exit_menu()
        )
        
    elif text == "کاربر":
        await state.set_state(BotStates.idle)
        await state.update_data(admin_mode="user")
        await message.answer("✅ فاز کاربری فعال شد.", reply_markup=get_main_menu())
        
    elif text == "مدیریت":
        await state.set_state(BotStates.idle)
        if user_id == ADMIN_ID:
            await state.update_data(admin_mode="admin")
            await message.answer("✅ پنل مدیریت فعال شد.", reply_markup=get_admin_menu())
        else:
            await message.answer("⛔ شما دسترسی مدیریت ندارید.")
            
    elif text == "❓ راهنما":
        await state.set_state(BotStates.idle)
        first_name = message.from_user.first_name or "دوست"
        help_text = f""" خب {first_name} جان ببین 👀

اینجا فقط یه ابزار ساده نیست، یه دستیار شخصیه که بهت کمک می‌کنه پست‌های طولانی و جذاب کانال @TechNowAi رو خیلی راحت و بدون دردسر بخونی. 🤓

وقتی تو کانال یه مطلب توجهت رو جلب می‌کنه، با زدن روی لینک مستقیم میای اینجا تا هم متن کاملش رو با تمرکز مطالعه کنی و هم اگه دوست داشتی تو آرشیو شخصیت نگهش داری! 📚✨

برای اینکه دقیق‌تر بدونی چطور می‌تونی از همه امکانات استفاده کنی، دکمه پایین رو لمس کن 👇"""
        await message.answer(help_text, reply_markup=get_help_more_kb())
        
    elif text == "👤 پروفایل":
        await state.set_state(BotStates.idle)
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
        await state.set_state(BotStates.idle)
        await message.answer("📂 کدوم پوشه رو میخوای باز کنی؟ 👇", reply_markup=get_folder_selection_kb())
        
    elif text == "📞 ارتباط با مدیریت":
        await state.set_state(BotStates.idle)
        await state.set_state(BotStates.user_chat_admin)
        await message.answer(
            "🛡️ ارتباط امن و ناشناس با مدیریت برقرار شد!\n\nهر پیامی داری همین الان بفرست ...",
            reply_markup=get_exit_menu()
        )
        
    elif text == "➕ افزودن پست":
        await state.set_state(BotStates.idle)
        admin_mode = state_data.get("admin_mode", "user")
        if user_id == ADMIN_ID and admin_mode != "user":
            await state.set_state(BotStates.waiting_post_content)
            await message.answer("📝 لطفاً متن، تصویر، ویدیو یا سند جدید خود را ارسال کنید:")
        else:
            await message.answer("⛔ شما دسترسی مدیریت ندارید.")
            
    elif text == "📁 مدیریت محتوا":
        await state.set_state(BotStates.idle)
        admin_mode = state_data.get("admin_mode", "user")
        if user_id == ADMIN_ID and admin_mode != "user":
            await message.answer("📂 انتخاب کنید:", reply_markup=get_content_management_kb())
        else:
            await message.answer("⛔ شما دسترسی مدیریت ندارید.")
            
    elif text == "📊 آمار":
        await state.set_state(BotStates.idle)
        admin_mode = state_data.get("admin_mode", "user")
        if user_id == ADMIN_ID and admin_mode != "user":
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
        await state.set_state(BotStates.idle)
        admin_mode = state_data.get("admin_mode", "user")
        if user_id == ADMIN_ID and admin_mode != "user":
            await state.set_state(BotStates.waiting_broadcast_content)
            await message.answer("📢 پیام همگانی خود را بفرستید (متن، عکس، ویدیو یا سند):")
        else:
            await message.answer("⛔ شما دسترسی مدیریت ندارید.")

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
# بخش مدیریت پیام‌ها در هر وضعیت فعال (FSM States Processing)
# ============================================================
@router.message(StateFilter(BotStates.ai_chat))
async def process_ai_chat(message: Message, state: FSMContext, db: D1Database, bot: Bot):
    user_id = message.from_user.id
    
    if not AI_API_KEY:
        await message.answer("⚠️ کلید API هوش مصنوعی در محیط ربات تعریف نشده است.")
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
        
    ai_result = await call_ai_with_history(AI_API_URL, AI_API_KEY, AI_MODEL_NAME, history)
    
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

# پاسخ ادمین فقط در حالتی که روی پیام ریپلای کند و در وضعیت خاص دیگری نباشد، فعال می‌شود
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

@router.message(StateFilter(BotStates.waiting_post_content))
async def process_add_post_content(message: Message, state: FSMContext, bot: Bot):
    state_data = await state.get_data()
    admin_mode = state_data.get("admin_mode", "user")
    if message.from_user.id != ADMIN_ID or admin_mode == "user":
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
    state_data = await state.get_data()
    admin_mode = state_data.get("admin_mode", "user")
    if message.from_user.id != ADMIN_ID or admin_mode == "user":
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

@router.message(StateFilter(BotStates.admin_search_word))
async def process_admin_search_word(message: Message, state: FSMContext, db: D1Database, bot: Bot):
    state_data = await state.get_data()
    admin_mode = state_data.get("admin_mode", "user")
    if message.from_user.id != ADMIN_ID or admin_mode == "user":
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

@router.message(StateFilter(None, BotStates.idle))
async def process_unknown_commands(message: Message, state: FSMContext):
    await message.answer("دستور ناشناس ❌\nلطفا از دکمه ها استفاده کنید 👇🏻")

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
# بخش‌های خلاصه لیست و مدیریت پست‌ها برای ادمین
# ============================================================
async def send_admin_all_posts_page(bot: Bot, chat_id: int, posts: list, page: int, total_pages: int, total_count: int, edit_message_id: int = None):
    message_text = f"📋 <b>لیست خلاصه محتواها (صفحه {page + 1} از {total_pages})</b>\n\n"
    for p in posts:
        raw_text = (p.get("text") or "")[:20].replace("\n", " ")
        preview = html.escape(raw_text) if raw_text else "(بدون متن)"
        message_text += f"🔹 #{p['id']} | {preview}...\n"
        message_text += f"   👍 {p.get('likes') or 0} | 👎 {p.get('dislikes') or 0} | 👁️ {p.get('views') or 0}\n\n"
    message_text += f"📊 مجموع: {total_count} پست"
    
    kb = get_admin_all_posts_kb(posts, page, total_pages)
    if edit_message_id:
        try:
            await bot.edit_message_text(chat_id=chat_id, message_id=edit_message_id, text=message_text, reply_markup=kb, parse_mode="HTML")
        except Exception:
            pass
    else:
        await bot.send_message(chat_id=chat_id, text=message_text, reply_markup=kb, parse_mode="HTML")

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
# تایید ارسال‌های ادمین و راهنما
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
            post_id = res[0].get("id") if res else None
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
                await bot_instance_message = await bot_instance.send_message(chat_id=uid, text=safe_text or "پیام همگانی")
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
    bot = Bot(token=API_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    
    db = D1Database(
        account_id=CF_ACCOUNT_ID,
        database_id=CF_DATABASE_ID,
        api_token=CF_API_TOKEN
    )
    dp["db"] = db
    
    await initialize_database(db)
    
    router.message.outer_middleware(RateLimitMiddleware(ADMIN_ID))
    router.callback_query.outer_middleware(RateLimitMiddleware(ADMIN_ID))
    dp.include_router(router)
    
    logger.info("Bot started successfully in Long Polling mode...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())