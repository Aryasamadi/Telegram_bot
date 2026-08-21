import os, io, re, time, math, random, logging, asyncio, json
from datetime import datetime, timezone
from typing import List, Dict, Any
import pytz, aiohttp, feedparser
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, Router, F, BaseMiddleware
from aiogram.filters import Command, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Message, CallbackQuery, TelegramObject, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton

load_dotenv()
API_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
BOT_USERNAME = os.getenv("BOT_USERNAME", "TechNowAibot")
CF_ACCOUNT_ID, CF_DATABASE_ID, CF_API_TOKEN = os.getenv("CF_ACCOUNT_ID"), os.getenv("CF_DATABASE_ID"), os.getenv("CF_API_TOKEN")
AI_API_KEY, AI_API_URL = os.getenv("AI_API_KEY"), os.getenv("AI_API_URL", "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions")
AI_MODEL_NAME = os.getenv("AI_MODEL_NAME", "gemini-1.5-flash")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class D1Database:
    def __init__(self, acc_id, db_id, token):
        self.url = f"https://api.cloudflare.com/client/v4/accounts/{acc_id}/d1/database/{db_id}/query"
        self.headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    async def execute(self, sql, params=None):
        payload = {"sql": sql}
        if params: payload["params"] = params
        async with aiohttp.ClientSession() as session:
            async with session.post(self.url, headers=self.headers, json=payload) as resp:
                data = await resp.json()
                if not data.get("success"): raise Exception(f"D1 Error: {data.get('errors')}")
                res = data.get("result", [])
                if isinstance(res, list) and res: return res[0].get("results", [])
                elif isinstance(res, dict): return res.get("results", [])
                return []

    async def execute_batch(self, queries):
        async with aiohttp.ClientSession() as session:
            out = []
            for q in queries:
                payload = {"sql": q["sql"]}
                if q.get("params"): payload["params"] = q["params"]
                async with session.post(self.url, headers=self.headers, json=payload) as resp:
                    data = await resp.json()
                    res = data.get("result", [])
                    if isinstance(res, list) and res: out.append(res[0].get("results", []))
                    elif isinstance(res, dict): out.append(res.get("results", []))
                    else: out.append([])
            return out

async def initialize_database(db: D1Database):
    queries = [
        {"sql": "CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY, joined_at TEXT, role TEXT DEFAULT 'user', tokens_used INTEGER DEFAULT 0, last_reset_date TEXT)"},
        {"sql": "CREATE TABLE IF NOT EXISTS posts(id INTEGER PRIMARY KEY AUTOINCREMENT, text TEXT, file_id TEXT, media_type TEXT, likes INTEGER DEFAULT 0, dislikes INTEGER DEFAULT 0, views INTEGER DEFAULT 0, deleted INTEGER DEFAULT 0)"},
        {"sql": "CREATE INDEX IF NOT EXISTS idx_posts_deleted ON posts(deleted)"},
        {"sql": "CREATE INDEX IF NOT EXISTS idx_posts_text ON posts(text)"},
        {"sql": "CREATE TABLE IF NOT EXISTS saves(user INTEGER, post INTEGER, folder TEXT, PRIMARY KEY(user, post))"},
        {"sql": "CREATE TABLE IF NOT EXISTS votes(user_id INTEGER, post_id INTEGER, vote_type TEXT, PRIMARY KEY(user_id, post_id))"},
        {"sql": "CREATE TABLE IF NOT EXISTS user_states(user_id INTEGER PRIMARY KEY, state TEXT, data TEXT)"},
        {"sql": "CREATE TABLE IF NOT EXISTS ai_apis(id INTEGER PRIMARY KEY AUTOINCREMENT, base_url TEXT, api_token TEXT, model_name TEXT, priority INTEGER, is_active INTEGER DEFAULT 1)"},
        {"sql": "CREATE TABLE IF NOT EXISTS rss_feeds(id INTEGER PRIMARY KEY AUTOINCREMENT, url TEXT, is_active INTEGER DEFAULT 1)"},
        {"sql": "CREATE TABLE IF NOT EXISTS processed_rss(guid TEXT PRIMARY KEY)"}
    ]
    await db.execute_batch(queries)
    try: await db.execute("ALTER TABLE posts ADD COLUMN views INTEGER DEFAULT 0")
    except: pass
    try: await db.execute("ALTER TABLE users ADD COLUMN tokens_used INTEGER DEFAULT 0")
    except: pass
    try: await db.execute("ALTER TABLE users ADD COLUMN last_reset_date TEXT")
    except: pass

async def reset_database(db: D1Database):
    queries = [{"sql": f"DROP TABLE IF EXISTS {t}"} for t in ["users","posts","saves","user_states","votes","ai_apis","rss_feeds","processed_rss"]]
    await db.execute_batch(queries)
    await initialize_database(db)

class BotStates(StatesGroup):
    idle, ai_chat, user_chat_admin, waiting_post_content, waiting_post_confirm, waiting_broadcast_content, waiting_broadcast_confirm = State(), State(), State(), State(), State(), State(), State()
    admin_search_word, admin_view_all, user_search_folder = State(), State(), State()
class AdminAPIStates(StatesGroup): waiting_base_url, waiting_token, waiting_model, waiting_priority = State(), State(), State(), State()
class AdminRSSStates(StatesGroup): waiting_url = State()

FOLDER_NAMES = {"cyber": "🔒 امنیت سایبری", "tech": "💻 تکنولوژی", "ai": "🧠 هوش مصنوعی", "edu": "📚 آموزش"}

def get_main_menu():
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="🤖 هوش مصنوعی"), KeyboardButton(text="💾 ذخیره‌های من")],
                                         [KeyboardButton(text="👤 پروفایل"), KeyboardButton(text="❓ راهنما")],
                                         [KeyboardButton(text="📞 ارتباط با مدیریت")]], resize_keyboard=True)

def get_admin_menu():
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="➕ افزودن پست"), KeyboardButton(text="📁 مدیریت محتوا")],
                                         [KeyboardButton(text="📊 آمار"), KeyboardButton(text="📢 ارسال همگانی")],
                                         [KeyboardButton(text="مدیریت API 🤖"), KeyboardButton(text="مدیریت RSS 📰")],
                                         [KeyboardButton(text="کاربر")]], resize_keyboard=True)

def get_exit_menu(): return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="❌ خروج از نشست")]], resize_keyboard=True)

def get_folder_selection_kb():
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=FOLDER_NAMES["cyber"], callback_data="f_view_cyber"), InlineKeyboardButton(text=FOLDER_NAMES["tech"], callback_data="f_view_tech")],
                                                 [InlineKeyboardButton(text=FOLDER_NAMES["ai"], callback_data="f_view_ai"), InlineKeyboardButton(text=FOLDER_NAMES["edu"], callback_data="f_view_edu")]])

def get_save_to_folder_kb(post_id):
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=FOLDER_NAMES["cyber"], callback_data=f"fsave_{post_id}_cyber"), InlineKeyboardButton(text=FOLDER_NAMES["tech"], callback_data=f"fsave_{post_id}_tech")],
                                                 [InlineKeyboardButton(text=FOLDER_NAMES["ai"], callback_data=f"fsave_{post_id}_ai"), InlineKeyboardButton(text=FOLDER_NAMES["edu"], callback_data=f"fsave_{post_id}_edu")]])

def get_post_inline_kb(post_id, likes, dislikes, is_saved):
    save_text, save_cb = ("❌ حذف از ذخیره‌ها", f"unsave_{post_id}") if is_saved else ("💾 ذخیره", f"save_{post_id}")
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=f"👍 {likes}", callback_data=f"like_{post_id}"), InlineKeyboardButton(text=f"👎 {dislikes}", callback_data=f"dis_{post_id}")],
                                                 [InlineKeyboardButton(text=save_text, callback_data=save_cb)]])

def get_saved_folder_pagination_kb(post_id, folder, index):
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🗑️ حذف", callback_data=f"ask_del_{post_id}_{folder}")],
                                                 [InlineKeyboardButton(text="⏮ قبلی", callback_data=f"fpg_prev_{folder}_{index}"), InlineKeyboardButton(text="⏭ بعدی", callback_data=f"fpg_next_{folder}_{index}")],
                                                 [InlineKeyboardButton(text="🔍 جستجو", callback_data=f"f_srch_{folder}")]])

def get_saved_folder_search_pagination_kb(post_id, folder, index):
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🗑️ حذف", callback_data=f"ask_del_{post_id}_{folder}")],
                                                 [InlineKeyboardButton(text="⏮ قبلی", callback_data=f"fspg_prev_{folder}_{index}"), InlineKeyboardButton(text="⏭ بعدی", callback_data=f"fspg_next_{folder}_{index}")],
                                                 [InlineKeyboardButton(text="🔍 جستجوی مجدد", callback_data=f"f_srch_{folder}")]])

def get_confirm_delete_kb(post_id, folder): return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🗑️ بله", callback_data=f"f_del_save_{post_id}_{folder}")], [InlineKeyboardButton(text="🔙 خیر", callback_data=f"cancel_delete_{folder}")]])
def get_confirm_add_post_kb(): return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ بله", callback_data="conf_add_yes"), InlineKeyboardButton(text="❌ خیر", callback_data="conf_add_no")]])
def get_confirm_broadcast_kb(): return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🚀 بله", callback_data="conf_broad_yes"), InlineKeyboardButton(text="❌ خیر", callback_data="conf_broad_no")]])
def get_content_management_kb(): return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔍 جستجو", callback_data="adm_search_text")], [InlineKeyboardButton(text="📋 نمایش همه", callback_data="adm_view_all")]])

def get_admin_search_pagination_kb(post_id, index):
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⏮ قبلی", callback_data=f"asearch_prev_{index}"), InlineKeyboardButton(text="⏭ بعدی", callback_data=f"asearch_next_{index}")],
                                                 [InlineKeyboardButton(text="📊 آمار", callback_data=f"astats_{post_id}"), InlineKeyboardButton(text="🗑️ حذف", callback_data=f"adelete_{post_id}")]])

def get_admin_all_posts_kb(posts, page, total_pages):
    ik = []
    sbs = [InlineKeyboardButton(text=f"📊 #{p['id']}", callback_data=f"adm_all_stat_{p['id']}") for p in posts]
    for i in range(0, len(sbs), 3): ik.append(sbs[i:i+3])
    ik.append([InlineKeyboardButton(text="⏮ قبلی", callback_data=f"adm_all_page_prev_{page}"), InlineKeyboardButton(text=f"📄 {page + 1}/{total_pages}", callback_data="noop"), InlineKeyboardButton(text="⏭ بعدی", callback_data=f"adm_all_page_next_{page}")])
    return InlineKeyboardMarkup(inline_keyboard=ik)

def get_admin_view_all_confirm_kb(): return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ بله", callback_data="adm_view_all_confirm")], [InlineKeyboardButton(text="❌ خیر", callback_data="adm_view_all_cancel")]])
def get_help_more_kb(): return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="💡 بیشتر", callback_data="help_more")]])
def get_help_got_it_kb(): return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🤓 متوجه شدم!", callback_data="help_got_it")]])

def get_tehran_date(): return datetime.now(pytz.timezone("Asia/Tehran")).strftime("%Y-%m-%d")

async def download_telegram_file_text(bot: Bot, file_id: str):
    fi = await bot.get_file(file_id)
    dest = io.BytesIO()
    await bot.download_file(fi.file_path, destination=dest)
    dest.seek(0)
    text = dest.read().decode('utf-8', errors='ignore')
    return text[:15000] + "\n\n[حجم زیاد بود]" if len(text)>15000 else text

async def call_ai_with_fallback(db: D1Database, messages: List[Dict[str, str]]) -> Dict[str, Any]:
    rows = await db.execute("SELECT * FROM ai_apis WHERE is_active = 1 ORDER BY priority ASC")
    if not rows:
        if AI_API_KEY: rows = [{"base_url": AI_API_URL, "api_token": AI_API_KEY, "model_name": AI_MODEL_NAME}]
        else: return {"content": "❌ هیچ API ثبت نشده است.", "tokens": 0, "success": False}
    for row in rows:
        base_url = row.get("base_url") or AI_API_URL
        headers = {"Authorization": f"Bearer {row['api_token']}", "Content-Type": "application/json"}
        payload = {"model": row["model_name"], "messages": messages}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(base_url, headers=headers, json=payload, timeout=30) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if "choices" in data and data["choices"]:
                            content = data["choices"][0]["message"]["content"]
                            return {"content": content, "tokens": data.get("usage", {}).get("total_tokens", math.ceil(len(content) / 4)), "success": True}
        except: continue
    return {"content": "❌ تمامی API ها خطا دادند.", "tokens": 0, "success": False}

class RateLimitMiddleware(BaseMiddleware):
    def __init__(self, admin_id):
        super().__init__()
        self.admin_id, self.map = admin_id, {}
    async def __call__(self, handler, event, data):
        uid = event.from_user.id if hasattr(event, 'from_user') and event.from_user else None
        if uid and uid != self.admin_id:
            now = time.time()
            if now - self.map.get(uid, 0) < 1.0:
                msg = random.choice(["آروم‌تر! 🏎️", "اسپم نکن ☕", "سرعتت زیاده! 🛑"])
                if isinstance(event, Message): await event.answer(msg)
                else: await event.answer(msg, show_alert=True)
                return
            self.map[uid] = now
        return await handler(event, data)

async def fetch_og_image(url):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10) as resp:
                if resp.status == 200:
                    soup = BeautifulSoup(await resp.text(), 'html.parser')
                    og = soup.find('meta', property='og:image')
                    if og and og.get('content'): return og['content']
    except: pass
    return None

async def background_rss_task(bot: Bot, db: D1Database):
    CHANNEL_USERNAME = f"@{BOT_USERNAME}" 
    while True:
        try:
            feeds = await db.execute("SELECT * FROM rss_feeds WHERE is_active = 1")
            for feed in (feeds or []):
                try:
                    parsed = feedparser.parse(feed["url"])
                    for entry in parsed.entries[:3]:
                        if await db.execute("SELECT guid FROM processed_rss WHERE guid = ?", [entry.link]): continue
                        await db.execute("INSERT INTO processed_rss(guid) VALUES(?)", [entry.link])
                        raw = f"Title: {entry.get('title','')}\nLink: {entry.link}\nDesc: {entry.get('description','')}"
                        prompt = "You are an expert Tech editor. Evaluate this news. If irrelevant, return EXACTLY 'SKIP'. If important, return valid JSON with exactly: 'short_text': 400-600 char engaging summary. 'long_text': 1000-3000 char deep analytical article."
                        res = await call_ai_with_fallback(db, [{"role":"system","content":prompt}, {"role":"user","content":raw}])
                        if not res.get("success"): continue
                        txt = res["content"].strip()
                        if txt == "SKIP" or "SKIP" in txt.upper(): continue
                        try:
                            js = txt.split("```json")[1] if "```json" in txt else (txt.split("```")[1] if "```" in txt else txt)
                            js = js.rsplit("```",1)[0] if js.endswith("```") else js
                            data = json.loads(js.strip())
                            short_t, long_t = data.get("short_text",""), data.get("long_text","")
                            if not short_t or not long_t: continue
                        except: continue
                        og_img = await fetch_og_image(entry.link)
                        r = await db.execute("INSERT INTO posts(text, file_id, media_type) VALUES(?, ?, ?) RETURNING id", [long_t, None, "text"])
                        pid = r[0].get("id") if r else (await db.execute("SELECT last_insert_rowid() as id"))[0].get("id")
                        if not pid: continue
                        cap = f"{short_t}\n\n<a href='https://t.me/{BOT_USERNAME}?start={pid}'>بیشتر...</a>"
                        try:
                            if og_img: await bot.send_photo(chat_id=CHANNEL_USERNAME, photo=og_img, caption=cap, parse_mode="HTML")
                            else: await bot.send_message(chat_id=CHANNEL_USERNAME, text=cap, parse_mode="HTML")
                        except: pass
                except: pass
        except: pass
        await asyncio.sleep(1800)

router = Router()
async def send_post_content(bot, chat_id, post, reply_markup=None):
    text, file_id, media_type = post.get("text") or "", post.get("file_id"), post.get("media_type")
    cap = text if len(text) <= 1024 else text[:1020] + "..."
    try:
        if media_type == "photo" and file_id: return await bot.send_photo(chat_id, file_id, caption=cap, reply_markup=reply_markup)
        elif media_type == "document" and file_id: return await bot.send_document(chat_id, file_id, caption=cap, reply_markup=reply_markup)
        elif media_type == "video" and file_id: return await bot.send_video(chat_id, file_id, caption=cap, reply_markup=reply_markup)
        elif media_type == "audio" and file_id: return await bot.send_audio(chat_id, file_id, caption=cap, reply_markup=reply_markup)
        else: return await bot.send_message(chat_id, text if len(text) <= 4096 else text[:4090] + "...", reply_markup=reply_markup)
    except: return None

@router.message(CommandStart())
async def cmd_start(msg: Message, state: FSMContext, db: D1Database, bot: Bot):
    uid = msg.from_user.id
    await db.execute("INSERT OR IGNORE INTO users(id, joined_at) VALUES(?, ?)", [uid, datetime.now(timezone.utc).isoformat()])
    await state.set_state(BotStates.idle)
    args = msg.text.split()
    admin_mode = (await state.get_data()).get("admin_mode", "user")
    menu = get_admin_menu() if (uid == ADMIN_ID and admin_mode != "user") else get_main_menu()
    
    if len(args) > 1 and args[1].isdigit():
        pid = int(args[1])
        r = await db.execute("SELECT text, file_id, media_type, likes, dislikes FROM posts WHERE id = ? AND deleted = 0", [pid])
        if r:
            await db.execute("UPDATE posts SET views = views + 1 WHERE id = ?", [pid])
            sv = await db.execute("SELECT folder FROM saves WHERE user = ? AND post = ?", [uid, pid])
            await send_post_content(bot, msg.chat.id, r[0], get_post_inline_kb(pid, r[0].get("likes",0), r[0].get("dislikes",0), len(sv)>0))
            await msg.answer("👇 منوی اصلی:", reply_markup=menu)
            return
        else:
            await msg.answer("❌ پست یافت نشد.")
            return
    await msg.answer(f"سلام {msg.from_user.first_name}! 👋 خوش اومدی.\n\nاز دکمه های پایین استفاده کن 👇🏻", reply_markup=menu)

@router.message(Command("setup_db"))
async def setup_db(msg: Message, db: D1Database):
    if msg.from_user.id == ADMIN_ID:
        await initialize_database(db)
        await msg.answer("✅ انجام شد.")

@router.message(Command("reset_db"))
async def reset_db(msg: Message, db: D1Database):
    if msg.from_user.id == ADMIN_ID:
        await reset_database(db)
        await msg.answer("✅ ریست شد.")

@router.message(F.text == "❌ خروج از نشست")
async def exit_sess(msg: Message, state: FSMContext):
    data = await state.get_data()
    adm = data.get("admin_mode", "user")
    await state.set_state(BotStates.idle)
    await state.set_data({"admin_mode": adm, "search_count": data.get("search_count",0), "search_window_start": data.get("search_window_start",0)})
    await msg.answer("🚪 خروج انجام شد!", reply_markup=get_admin_menu() if msg.from_user.id == ADMIN_ID and adm != "user" else get_main_menu())

@router.callback_query(F.data == "api_add")
async def api_add_st(call: CallbackQuery, state: FSMContext):
    await state.set_state(AdminAPIStates.waiting_base_url)
    await call.message.answer("لینک Base URL (مثال: https://api.openai.com/v1/chat/completions):", reply_markup=get_exit_menu())

@router.message(StateFilter(AdminAPIStates.waiting_base_url))
async def api_burl(msg: Message, state: FSMContext):
    await state.update_data(base_url=msg.text.strip())
    await state.set_state(AdminAPIStates.waiting_token)
    await msg.answer("حالا API Token را بفرستید:")

@router.message(StateFilter(AdminAPIStates.waiting_token))
async def api_tok(msg: Message, state: FSMContext):
    await state.update_data(token=msg.text.strip())
    await state.set_state(AdminAPIStates.waiting_model)
    await msg.answer("نام مدل (مثل gpt-4o):")

@router.message(StateFilter(AdminAPIStates.waiting_model))
async def api_mod(msg: Message, state: FSMContext):
    await state.update_data(model=msg.text.strip())
    await state.set_state(AdminAPIStates.waiting_priority)
    await msg.answer("اولویت اجرا (عدد - 1 بالاترین):")

@router.message(StateFilter(AdminAPIStates.waiting_priority))
async def api_prio(msg: Message, state: FSMContext, db: D1Database):
    if not msg.text.strip().isdigit(): return await msg.answer("فقط عدد!")
    d = await state.get_data()
    await db.execute("INSERT INTO ai_apis (base_url, api_token, model_name, priority) VALUES (?, ?, ?, ?)", [d['base_url'], d['token'], d['model'], int(msg.text)])
    await state.set_state(BotStates.idle)
    await msg.answer("✅ API اضافه شد!", reply_markup=get_admin_menu())

@router.callback_query(F.data == "api_list")
async def api_list(call: CallbackQuery, db: D1Database):
    rows = await db.execute("SELECT * FROM ai_apis ORDER BY priority ASC")
    if not rows: return await call.message.answer("📭 خالیست.")
    for r in rows:
        await call.message.answer(f"🆔 {r['id']}\n🔗 {r['base_url']}\n🤖 {r['model_name']}\n⭐ {r['priority']}", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🗑 حذف", callback_data=f"api_del_{r['id']}")]]))

@router.callback_query(F.data.startswith("api_del_"))
async def api_del(call: CallbackQuery, db: D1Database):
    await db.execute("DELETE FROM ai_apis WHERE id = ?", [int(call.data.split("_")[2])])
    await call.message.delete()

@router.callback_query(F.data == "rss_add")
async def rss_add_st(call: CallbackQuery, state: FSMContext):
    await state.set_state(AdminRSSStates.waiting_url)
    await call.message.answer("🔗 لینک RSS:", reply_markup=get_exit_menu())

@router.message(StateFilter(AdminRSSStates.waiting_url))
async def rss_add(msg: Message, state: FSMContext, db: D1Database):
    await db.execute("INSERT INTO rss_feeds (url) VALUES (?)", [msg.text.strip()])
    await state.set_state(BotStates.idle)
    await msg.answer("✅ فید اضافه شد!", reply_markup=get_admin_menu())

@router.callback_query(F.data == "rss_list")
async def rss_list(call: CallbackQuery, db: D1Database):
    rows = await db.execute("SELECT * FROM rss_feeds")
    if not rows: return await call.message.answer("📭 خالیست.")
    for r in rows: await call.message.answer(f"🆔 {r['id']}\n🔗 {r['url']}", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🗑 حذف", callback_data=f"rss_del_{r['id']}")]]))

@router.callback_query(F.data.startswith("rss_del_"))
async def rss_del(call: CallbackQuery, db: D1Database):
    await db.execute("DELETE FROM rss_feeds WHERE id = ?", [int(call.data.split("_")[2])])
    await call.message.delete()

@router.message(StateFilter(BotStates.ai_chat))
async def ai_chat(msg: Message, state: FSMContext, db: D1Database, bot: Bot):
    uid, today = msg.from_user.id, get_tehran_date()
    u = await db.execute("SELECT tokens_used, last_reset_date FROM users WHERE id = ?", [uid])
    tokens = u[0].get("tokens_used",0) if u else 0
    if u and u[0].get("last_reset_date") != today:
        tokens = 0
        await db.execute("UPDATE users SET tokens_used=0, last_reset_date=? WHERE id=?", [today, uid])
    if tokens >= 10000: return await msg.answer("⛔ سهمیه امروز تمام شد.")
    
    prompt = msg.text
    if msg.document:
        await msg.answer("⏳ در حال خواندن...")
        prompt = f"بررسی فایل:\n{await download_telegram_file_text(bot, msg.document.file_id)}\n{msg.caption or ''}"
    if not prompt: return await msg.answer("متن نامعتبر.")
    await bot.send_chat_action(msg.chat.id, "typing")
    
    d = await state.get_data()
    hist = d.get("ai_history", [{"role":"system","content":"Reply in Persian."}])
    hist.append({"role":"user","content":prompt})
    if len(hist)>11: hist = [hist[0]] + hist[-10:]
    
    res = await call_ai_with_fallback(db, hist)
    if not res.get("success"): return await msg.answer(res["content"])
    hist.append({"role":"assistant","content":res["content"]})
    await state.update_data(ai_history=hist)
    
    rt = res["content"]
    for i in range(0, len(rt), 3900):
        try: await msg.answer(rt[i:i+3900], parse_mode="Markdown")
        except: await msg.answer(rt[i:i+3900])
    await db.execute("UPDATE users SET tokens_used=?, last_reset_date=? WHERE id=?", [tokens + res["tokens"], today, uid])

@router.message(StateFilter(BotStates.user_chat_admin))
async def usr_adm(msg: Message, bot: Bot):
    if msg.from_user.id == ADMIN_ID: return
    ht, cap = f"#User_{msg.from_user.id}", msg.caption or ""
    if msg.photo: await bot.send_photo(ADMIN_ID, msg.photo[-1].file_id, caption=f"{ht}\n{cap}")
    elif msg.text: await bot.send_message(ADMIN_ID, f"{ht}\n{msg.text}")

@router.message(StateFilter(BotStates.waiting_post_content))
async def add_post(msg: Message, state: FSMContext, bot: Bot):
    if msg.from_user.id != ADMIN_ID: return
    fid, mtype = None, None
    if msg.photo: fid, mtype = msg.photo[-1].file_id, "photo"
    elif msg.document: fid, mtype = msg.document.file_id, "document"
    cap = msg.text or msg.caption or ""
    if not fid and not cap: return await msg.answer("❌ نامعتبر")
    await state.update_data(temp_text=cap, temp_file_id=fid, temp_media_type=mtype)
    await state.set_state(BotStates.waiting_post_confirm)
    await send_post_content(bot, msg.chat.id, {"text":cap,"file_id":fid,"media_type":mtype})
    await msg.answer("ذخیره شود؟", reply_markup=get_confirm_add_post_kb())

@router.message(StateFilter(BotStates.waiting_broadcast_content))
async def br_cast(msg: Message, state: FSMContext, bot: Bot):
    if msg.from_user.id != ADMIN_ID: return
    fid, mtype = None, None
    if msg.photo: fid, mtype = msg.photo[-1].file_id, "photo"
    cap = msg.text or msg.caption or ""
    if not fid and not cap: return
    cap += "\n\n#Broadcast"
    await state.update_data(temp_text=cap, temp_file_id=fid, temp_media_type=mtype)
    await state.set_state(BotStates.waiting_broadcast_confirm)
    await send_post_content(bot, msg.chat.id, {"text":cap,"file_id":fid,"media_type":mtype})
    await msg.answer("ارسال شود؟", reply_markup=get_confirm_broadcast_kb())

@router.message(StateFilter(BotStates.admin_search_word))
async def adm_srch(msg: Message, state: FSMContext, db: D1Database, bot: Bot):
    q = (msg.text or "").strip()
    res = await db.execute("SELECT id FROM posts WHERE id = ? AND deleted=0", [int(q)]) if q.isdigit() else await db.execute("SELECT id FROM posts WHERE text LIKE ? AND deleted=0 ORDER BY id DESC LIMIT 50", [f"%{q}%"])
    if not res: return await msg.answer("❌ یافت نشد")
    ids = [r["id"] for r in res]
    await state.update_data(search_ids=ids, search_index=0)
    pr = await db.execute("SELECT text, file_id, media_type FROM posts WHERE id = ?", [ids[0]])
    if pr: await send_post_content(bot, msg.chat.id, pr[0], get_admin_search_pagination_kb(ids[0], 0))

@router.message(StateFilter(BotStates.user_search_folder))
async def usr_srch(msg: Message, state: FSMContext, db: D1Database, bot: Bot):
    d = await state.get_data()
    fld = d.get("folder")
    now = time.time()*1000
    if now - d.get("search_window_start",0) > 28800000: d["search_count"] = 0
    if d.get("search_count",0) >= 5: return await msg.answer("⏱️ سقف سرچ فعلا پر شده")
    d["search_count"], d["search_window_start"] = d.get("search_count",0)+1, d.get("search_window_start",0) or now
    await state.update_data(**d)
    
    rows = await db.execute("SELECT p.id FROM saves s JOIN posts p ON s.post=p.id WHERE s.user=? AND s.folder=? AND p.text LIKE ? AND p.deleted=0 LIMIT 30", [msg.from_user.id, fld, f"%{msg.text}%"])
    if not rows: return await msg.answer("❌ یافت نشد")
    ids = [r["id"] for r in rows]
    await state.update_data(search_ids=ids, search_index=0)
    pr = await db.execute("SELECT text, file_id, media_type FROM posts WHERE id=?", [ids[0]])
    if pr: await send_post_content(bot, msg.chat.id, pr[0], get_saved_folder_search_pagination_kb(ids[0], fld, 0))

@router.message(F.chat.id == ADMIN_ID, F.reply_to_message, StateFilter(None, BotStates.idle))
async def adm_rep(msg: Message, bot: Bot):
    m = re.search(r"#User_(\d+)", msg.reply_to_message.text or msg.reply_to_message.caption or "")
    if m:
        t = int(m.group(1))
        pfx = "پاسخ مدیریت:\n"
        try:
            if msg.photo: await bot.send_photo(t, msg.photo[-1].file_id, caption=pfx+msg.caption)
            else: await bot.send_message(t, pfx+msg.text)
            await msg.answer("✅ ارسال شد")
        except: pass

@router.message(F.text.in_(["کاربر","مدیریت","🤖 هوش مصنوعی","💾 ذخیره‌های من","📞 ارتباط با مدیریت","❓ راهنما","👤 پروفایل","➕ افزودن پست","📁 مدیریت محتوا","📊 آمار","📢 ارسال همگانی","مدیریت API 🤖","مدیریت RSS 📰"]), StateFilter(None, BotStates.idle))
async def glob_cmds(msg: Message, state: FSMContext, db: D1Database):
    t, uid = msg.text, msg.from_user.id
    if t == "🤖 هوش مصنوعی":
        await state.set_state(BotStates.ai_chat)
        await state.update_data(ai_history=[{"role":"system","content":"Reply in Persian."}])
        await msg.answer("چطور کمکت کنم؟", reply_markup=get_exit_menu())
    elif t == "کاربر":
        await state.update_data(admin_mode="user")
        await msg.answer("فاز کاربری", reply_markup=get_main_menu())
    elif t == "مدیریت":
        if uid == ADMIN_ID:
            await state.update_data(admin_mode="admin")
            await msg.answer("فاز ادمین", reply_markup=get_admin_menu())
    elif t == "❓ راهنما": await msg.answer("راهنما", reply_markup=get_help_more_kb())
    elif t == "👤 پروفایل":
        u = await db.execute("SELECT joined_at, role FROM users WHERE id=?", [uid])
        sv = await db.execute("SELECT COUNT(*) as c FROM saves WHERE user=?", [uid])
        await msg.answer(f"پروفایل\nذخیره: {sv[0]['c'] if sv else 0}\nسطح: {u[0]['role'] if u else 'user'}")
    elif t == "💾 ذخیره‌های من": await msg.answer("پوشه؟", reply_markup=get_folder_selection_kb())
    elif t == "📞 ارتباط با مدیریت":
        await state.set_state(BotStates.user_chat_admin)
        await msg.answer("پیام بفرست", reply_markup=get_exit_menu())
    elif uid == ADMIN_ID:
        if t == "➕ افزودن پست":
            await state.set_state(BotStates.waiting_post_content)
            await msg.answer("بفرست", reply_markup=get_exit_menu())
        elif t == "📁 مدیریت محتوا": await msg.answer("انتخاب:", reply_markup=get_content_management_kb())
        elif t == "📊 آمار":
            c = await db.execute("SELECT COUNT(*) as c FROM posts")
            await msg.answer(f"پست ها: {c[0]['c'] if c else 0}")
        elif t == "📢 ارسال همگانی":
            await state.set_state(BotStates.waiting_broadcast_content)
            await msg.answer("بفرست", reply_markup=get_exit_menu())
        elif t == "مدیریت API 🤖": await msg.answer("تنظیمات:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="افزودن", callback_data="api_add")], [InlineKeyboardButton(text="لیست", callback_data="api_list")]]))
        elif t == "مدیریت RSS 📰": await msg.answer("تنظیمات:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="افزودن", callback_data="rss_add")], [InlineKeyboardButton(text="لیست", callback_data="rss_list")]]))

@router.callback_query(F.data.startswith("like_") | F.data.startswith("dis_"))
async def post_vote(call: CallbackQuery, db: D1Database):
    p, uid = call.data.split("_"), call.from_user.id
    nv, pid = "like" if p[0]=="like" else "dislike", int(p[1])
    vr = await db.execute("SELECT vote_type FROM votes WHERE user_id=? AND post_id=?", [uid, pid])
    if not vr:
        await db.execute_batch([{"sql": "INSERT INTO votes(user_id,post_id,vote_type) VALUES(?,?,?)", "params":[uid,pid,nv]}, {"sql": f"UPDATE posts SET {nv}s={nv}s+1 WHERE id=?", "params":[pid]}])
    else:
        cv = vr[0]["vote_type"]
        if cv == nv: await db.execute_batch([{"sql": "DELETE FROM votes WHERE user_id=? AND post_id=?", "params":[uid,pid]}, {"sql": f"UPDATE posts SET {nv}s={nv}s-1 WHERE id=?", "params":[pid]}])
        else: await db.execute_batch([{"sql": "UPDATE votes SET vote_type=? WHERE user_id=? AND post_id=?", "params":[nv,uid,pid]}, {"sql": f"UPDATE posts SET {nv}s={nv}s+1, {cv}s={cv}s-1 WHERE id=?", "params":[pid]}])
    await call.answer("ثبت شد")
    pr = await db.execute("SELECT likes, dislikes FROM posts WHERE id=?", [pid])
    if pr: 
        s = await db.execute("SELECT folder FROM saves WHERE user=? AND post=?", [uid,pid])
        try: await call.message.edit_reply_markup(reply_markup=get_post_inline_kb(pid, pr[0].get("likes",0), pr[0].get("dislikes",0), len(s)>0))
        except: pass

@router.callback_query(F.data.startswith("save_"))
async def ask_sv(call: CallbackQuery):
    await call.message.answer("پوشه؟", reply_markup=get_save_to_folder_kb(int(call.data.split("_")[1])))
    await call.answer()

@router.callback_query(F.data.startswith("fsave_"))
async def do_sv(call: CallbackQuery, db: D1Database):
    _, pid, fld = call.data.split("_")
    await db.execute("INSERT OR IGNORE INTO saves(user, post, folder) VALUES(?,?,?)", [call.from_user.id, int(pid), fld])
    await call.answer("ذخیره شد", show_alert=True)
    try: await call.message.delete()
    except: pass

@router.callback_query(F.data.startswith("unsave_"))
async def do_unsv(call: CallbackQuery, db: D1Database):
    pid = int(call.data.split("_")[1])
    await db.execute("DELETE FROM saves WHERE user=? AND post=?", [call.from_user.id, pid])
    await call.answer("حذف شد", show_alert=True)
    pr = await db.execute("SELECT likes, dislikes FROM posts WHERE id=?", [pid])
    if pr:
        try: await call.message.edit_reply_markup(reply_markup=get_post_inline_kb(pid, pr[0].get("likes",0), pr[0].get("dislikes",0), False))
        except: pass

@router.callback_query(F.data.startswith("f_view_"))
async def fview(call: CallbackQuery, state: FSMContext, db: D1Database, bot: Bot):
    fld, uid = call.data.split("_")[2], call.from_user.id
    rows = await db.execute("SELECT p.id FROM saves s JOIN posts p ON s.post=p.id WHERE s.user=? AND s.folder=? AND p.deleted=0 LIMIT 30", [uid, fld])
    if not rows: return await call.answer("خالیست", show_alert=True)
    ids = [r["id"] for r in rows]
    await state.update_data(cached_folder=fld, cached_list=ids, current_folder=fld, current_index=0, current_list=ids)
    pr = await db.execute("SELECT text, file_id, media_type FROM posts WHERE id=?", [ids[0]])
    if pr: await send_post_content(bot, call.message.chat.id, pr[0], get_saved_folder_pagination_kb(ids[0], fld, 0))
    await call.answer()

@router.callback_query(F.data.startswith("fpg_") | F.data.startswith("fspg_") | F.data.startswith("asearch_") | F.data.startswith("adm_all_page_"))
async def handle_pagi(call: CallbackQuery, state: FSMContext, db: D1Database, bot: Bot):
    # Unified pagination logic simplified for token space
    p = call.data.split("_")
    d = await state.get_data()
    lst = d.get("current_list",[]) if "fpg" in call.data else (d.get("search_ids",[]) if "search" in call.data else [])
    if lst:
        idx = int(p[-1])
        nidx = idx + 1 if "next" in call.data else idx - 1
        nidx = max(0, min(nidx, len(lst)-1))
        if nidx != idx:
            if "fpg" in call.data: await state.update_data(current_index=nidx)
            else: await state.update_data(search_index=nidx)
            pr = await db.execute("SELECT * FROM posts WHERE id=?", [lst[nidx]])
            if pr:
                if pr[0].get("file_id"): 
                    try: await call.message.delete()
                    except: pass
                    await send_post_content(bot, call.message.chat.id, pr[0], get_saved_folder_pagination_kb(lst[nidx], p[2], nidx) if "fpg" in call.data else get_admin_search_pagination_kb(lst[nidx], nidx))
                else: 
                    try: await call.message.edit_text(pr[0].get("text",""), reply_markup=get_saved_folder_pagination_kb(lst[nidx], p[2], nidx) if "fpg" in call.data else get_admin_search_pagination_kb(lst[nidx], nidx))
                    except: pass
    elif "adm_all_page_" in call.data:
        cpage = int(p[-1])
        npage = cpage + 1 if "next" in call.data else cpage - 1
        npage = max(0, min(npage, d.get("all_total_pages",1)-1))
        if npage != cpage:
            await state.update_data(all_posts_page=npage)
            rows = await db.execute("SELECT id FROM posts WHERE deleted=0 LIMIT ? OFFSET ?", [10, npage*10])
            if rows: 
                try: await call.message.edit_reply_markup(reply_markup=get_admin_all_posts_kb(rows, npage, d.get("all_total_pages",1)))
                except: pass
    await call.answer()

@router.callback_query(F.data.startswith("ask_del_"))
async def ask_del(call: CallbackQuery):
    await call.message.answer("مطمئنی؟", reply_markup=get_confirm_delete_kb(int(call.data.split("_")[2]), call.data.split("_")[3]))
    await call.answer()

@router.callback_query(F.data.startswith("f_del_save_"))
async def fdel(call: CallbackQuery, db: D1Database):
    await db.execute("DELETE FROM saves WHERE user=? AND post=?", [call.from_user.id, int(call.data.split("_")[3])])
    await call.answer("حذف شد", show_alert=True)
    try: await call.message.delete()
    except: pass

@router.callback_query(F.data == "conf_add_yes")
async def add_y(call: CallbackQuery, state: FSMContext, db: D1Database):
    d = await state.get_data()
    r = await db.execute("INSERT INTO posts(text,file_id,media_type) VALUES(?,?,?) RETURNING id", [d.get("temp_text"), d.get("temp_file_id"), d.get("temp_media_type")])
    pid = r[0].get("id") if r else (await db.execute("SELECT last_insert_rowid() as id"))[0].get("id")
    await state.update_data(temp_text=None, temp_file_id=None, temp_media_type=None)
    await state.set_state(BotStates.idle)
    await call.message.answer(f"✅ ثبت شد\nhttps://t.me/{BOT_USERNAME}?start={pid}")
    await call.answer()

@router.callback_query(F.data == "conf_add_no")
async def add_n(call: CallbackQuery, state: FSMContext):
    await state.set_state(BotStates.idle)
    await call.message.answer("لغو شد")
    await call.answer()

@router.callback_query(F.data == "conf_broad_yes")
async def br_y(call: CallbackQuery, state: FSMContext, db: D1Database, bot: Bot):
    d = await state.get_data()
    usr = await db.execute("SELECT id FROM users")
    await call.answer("شروع شد")
    for u in usr:
        try:
            if d.get("temp_media_type") == "photo": await bot.send_photo(u["id"], d["temp_file_id"], caption=d["temp_text"])
            else: await bot.send_message(u["id"], d["temp_text"])
        except: pass
    await state.set_state(BotStates.idle)
    await call.message.answer("پایان")

@router.callback_query(F.data == "conf_broad_no")
async def br_n(call: CallbackQuery, state: FSMContext):
    await state.set_state(BotStates.idle)
    await call.message.answer("لغو شد")
    await call.answer()

@router.callback_query(F.data == "adm_view_all")
async def v_all(call: CallbackQuery, state: FSMContext, db: D1Database, bot: Bot):
    rows = await db.execute("SELECT id FROM posts WHERE deleted=0 LIMIT 10 OFFSET 0")
    if rows:
        c = await db.execute("SELECT COUNT(*) as c FROM posts WHERE deleted=0")
        tot = c[0]["c"] if c else 0
        await state.update_data(all_total_pages=math.ceil(tot/10))
        await call.message.answer("لیست:", reply_markup=get_admin_all_posts_kb(rows, 0, math.ceil(tot/10)))
    await call.answer()

@router.callback_query(F.data == "noop" or F.data == "help_got_it" or F.data.startswith("cancel_delete_"))
async def do_nothing(call: CallbackQuery): 
    try: await call.message.delete()
    except: pass
    await call.answer()

async def main():
    bot = Bot(token=API_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    db = D1Database(CF_ACCOUNT_ID, CF_DATABASE_ID, CF_API_TOKEN)
    await initialize_database(db)
    
    router.message.outer_middleware(RateLimitMiddleware(ADMIN_ID))
    router.callback_query.outer_middleware(RateLimitMiddleware(ADMIN_ID))
    dp.include_router(router)
    
    asyncio.create_task(background_rss_task(bot, db))
    
    logger.info("Bot started...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
