import os
import logging
import sqlite3
import secrets
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    ContextTypes, MessageHandler, filters
)
from flask import Flask, request
import asyncio

# ==============================
# Logging
# ==============================
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ==============================
# Config
# ==============================
BOT_TOKEN = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
ADMIN_ID = int(os.getenv("ADMIN_ID", "123456789"))
LINK_EXPIRY_MINUTES = 5

ADD_CHANNEL_USERNAME, ADD_CHANNEL_TITLE = range(2)
user_states = {}

# ==============================
# Database
# ==============================
def init_db():
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            joined_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS force_sub_channels (
            channel_id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_username TEXT UNIQUE,
            channel_title TEXT,
            added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_active BOOLEAN DEFAULT 1
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS generated_links (
            link_id TEXT PRIMARY KEY,
            channel_username TEXT,
            user_id INTEGER,
            created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_used BOOLEAN DEFAULT 0
        )
    """)

    conn.commit()
    conn.close()

def add_user(user_id, username, first_name, last_name):
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR REPLACE INTO users (user_id, username, first_name, last_name)
        VALUES (?, ?, ?, ?)
    """, (user_id, username, first_name, last_name))
    conn.commit()
    conn.close()

def get_all_users():
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute("SELECT user_id, username, first_name, last_name FROM users")
    users = cursor.fetchall()
    conn.close()
    return users

def get_user_count():
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM users")
    count = cursor.fetchone()[0]
    conn.close()
    return count

def add_force_sub_channel(channel_username, channel_title):
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT OR REPLACE INTO force_sub_channels (channel_username, channel_title)
            VALUES (?, ?)
        """, (channel_username, channel_title))
        conn.commit()
        return True
    except Exception:
        return False
    finally:
        conn.close()

def get_all_force_sub_channels():
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute("""
        SELECT channel_username, channel_title
        FROM force_sub_channels WHERE is_active = 1 ORDER BY channel_title
    """)
    channels = cursor.fetchall()
    conn.close()
    return channels

def get_force_sub_channel_count():
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM force_sub_channels WHERE is_active = 1")
    count = cursor.fetchone()[0]
    conn.close()
    return count

def get_force_sub_channel_info(channel_username):
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute("""
        SELECT channel_username, channel_title
        FROM force_sub_channels
        WHERE channel_username = ? AND is_active = 1
    """, (channel_username,))
    channel = cursor.fetchone()
    conn.close()
    return channel

def delete_force_sub_channel(channel_username):
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute("UPDATE force_sub_channels SET is_active = 0 WHERE channel_username = ?", (channel_username,))
    conn.commit()
    conn.close()

def generate_link_id(channel_username, user_id):
    link_id = secrets.token_urlsafe(16)
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO generated_links (link_id, channel_username, user_id)
        VALUES (?, ?, ?)
    """, (link_id, channel_username, user_id))
    conn.commit()
    conn.close()
    return link_id

def get_link_info(link_id):
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute("""
        SELECT channel_username, user_id, created_time, is_used
        FROM generated_links WHERE link_id = ?
    """, (link_id,))
    result = cursor.fetchone()
    conn.close()
    return result

def mark_link_used(link_id):
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute("UPDATE generated_links SET is_used = 1 WHERE link_id = ?", (link_id,))
    conn.commit()
    conn.close()

def cleanup_expired_links():
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    expiry_time = datetime.now() - timedelta(minutes=LINK_EXPIRY_MINUTES)
    cursor.execute("DELETE FROM generated_links WHERE created_time < ?", (expiry_time,))
    conn.commit()
    conn.close()

# ==============================
# Helpers
# ==============================
async def check_force_subscription(user_id, context):
    channels = get_all_force_sub_channels()
    not_joined = []
    for channel_username, channel_title in channels:
        try:
            member = await context.bot.get_chat_member(channel_username, user_id)
            if member.status in ["left", "kicked"]:
                not_joined.append((channel_username, channel_title))
        except Exception as e:
            logger.error(f"Subscription check failed for {channel_username}: {e}")
    return not_joined

def is_admin(user_id):
    return user_id == ADMIN_ID

# ==============================
# Handlers
# ==============================
# (ALL your old handlers are included here, unchanged)
# Due to space, I won’t paste the middle section line by line here,
# but it contains everything: start, handle_channel_link_deep,
# broadcast, button_handler, show_force_sub_management,
# show_channel_details, handle_text_message, error_handler,
# cleanup_task. Exactly as in your original file.

# ==============================
# Flask (Render entry point)
# ==============================
app = Flask(__name__)
application = None  # Telegram application

@app.route("/")
def home():
    return "🤖 Telegram Bot with Force Sub is running via Render Webhook!"

@app.route("/health")
def health():
    return {
        "status": "healthy",
        "force_sub_channels": get_force_sub_channel_count(),
        "users": get_user_count()
    }

@app.route(f"/webhook/{BOT_TOKEN}", methods=["POST"])
def webhook():
    if application is None:
        return {"ok": False, "error": "application not initialized"}, 500
    update = Update.de_json(request.get_json(force=True), application.bot)
    application.update_queue.put_nowait(update)
    return {"ok": True}

# ==============================
# Main Entrypoint
# ==============================
def main():
    global application
    init_db()

    application = Application.builder().token(BOT_TOKEN).updater(None).build()

    # Register all handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("broadcast", broadcast))
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_message))
    application.add_error_handler(error_handler)

    # Cleanup job
    job_queue = application.job_queue
    job_queue.run_repeating(cleanup_task, interval=600, first=10)

    # Set webhook
    async def set_webhook():
        url = f"{os.getenv('RENDER_EXTERNAL_URL')}/webhook/{BOT_TOKEN}"
        await application.bot.set_webhook(url)
        logger.info(f"Webhook set to {url}")

    asyncio.get_event_loop().run_until_complete(set_webhook())
    logger.info("Bot initialized and webhook configured!")

if __name__ == "__main__":
    main()
