import os
import logging
import secrets
import re
import requests
import time
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, MessageHandler, filters
import asyncio
from threading import Thread

# Database imports
try:
    import psycopg2
    from urllib.parse import urlparse
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False
    import sqlite3

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
BOT_TOKEN = os.environ.get('BOT_TOKEN', '7877393813:AAEqVD-Ar6M4O3yg6h2ZuNUN_PPY4NRVr10')
ADMIN_ID = int(os.environ.get('ADMIN_ID', '829342319'))
LINK_EXPIRY_MINUTES = 5
DATABASE_URL = os.environ.get('DATABASE_URL')
USE_POSTGRES = DATABASE_URL and POSTGRES_AVAILABLE
PORT = int(os.environ.get('PORT', 8080))
WEBHOOK_URL = os.environ.get('RENDER_EXTERNAL_URL', '').rstrip('/') + '/'
WELCOME_SOURCE_CHANNEL = -1002530952988
WELCOME_SOURCE_MESSAGE_ID = 32
PUBLIC_ANIME_CHANNEL_URL = "https://t.me/BeatAnime"
REQUEST_CHANNEL_URL = "https://t.me/Beat_Hindi_Dubbed"
ADMIN_CONTACT_USERNAME = "Beat_Anime_Ocean"

ADD_CHANNEL_USERNAME, ADD_CHANNEL_TITLE, GENERATE_LINK_CHANNEL_USERNAME, PENDING_BROADCAST = range(4)
user_states = {}

def keep_alive():
    while True:
        try:
            time.sleep(840)
            response = requests.get("https://www.google.com/robots.txt", timeout=10)
            logger.info(f"Keep-alive ping sent ({response.status_code})")
        except Exception as e:
            logger.error(f"Keep-alive error: {e}")

def get_db_connection():
    if USE_POSTGRES:
        result = urlparse(DATABASE_URL)
        return psycopg2.connect(database=result.path[1:], user=result.username, password=result.password, host=result.hostname, port=result.port)
    return sqlite3.connect('bot_data.db')

def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    if USE_POSTGRES:
        cursor.execute('CREATE TABLE IF NOT EXISTS users (user_id BIGINT PRIMARY KEY, username TEXT, first_name TEXT, last_name TEXT, joined_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP)')
        cursor.execute('CREATE TABLE IF NOT EXISTS force_sub_channels (channel_id SERIAL PRIMARY KEY, channel_username TEXT UNIQUE, channel_title TEXT, added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP, is_active BOOLEAN DEFAULT TRUE)')
        cursor.execute('CREATE TABLE IF NOT EXISTS generated_links (link_id TEXT PRIMARY KEY, channel_username TEXT, user_id BIGINT, created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, is_used BOOLEAN DEFAULT FALSE, is_permanent BOOLEAN DEFAULT FALSE)')
    else:
        cursor.execute('CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, username TEXT, first_name TEXT, last_name TEXT, joined_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP)')
        cursor.execute('CREATE TABLE IF NOT EXISTS force_sub_channels (channel_id INTEGER PRIMARY KEY AUTOINCREMENT, channel_username TEXT UNIQUE, channel_title TEXT, added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP, is_active BOOLEAN DEFAULT 1)')
        cursor.execute('CREATE TABLE IF NOT EXISTS generated_links (link_id TEXT PRIMARY KEY, channel_username TEXT, user_id INTEGER, created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, is_used BOOLEAN DEFAULT 0, is_permanent BOOLEAN DEFAULT 0)')
    conn.commit()
    conn.close()
    logger.info(f"Database initialized: {'PostgreSQL' if USE_POSTGRES else 'SQLite'}")

def add_user(user_id, username, first_name, last_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    if USE_POSTGRES:
        cursor.execute('INSERT INTO users (user_id, username, first_name, last_name) VALUES (%s, %s, %s, %s) ON CONFLICT (user_id) DO UPDATE SET username = EXCLUDED.username, first_name = EXCLUDED.first_name, last_name = EXCLUDED.last_name', (user_id, username, first_name, last_name))
    else:
        cursor.execute('INSERT OR REPLACE INTO users (user_id, username, first_name, last_name) VALUES (?, ?, ?, ?)', (user_id, username, first_name, last_name))
    conn.commit()
    conn.close()

def get_all_users(limit=20, offset=0):
    conn = get_db_connection()
    cursor = conn.cursor()
    if USE_POSTGRES:
        cursor.execute('SELECT user_id, username, first_name, last_name, joined_date FROM users ORDER BY joined_date DESC LIMIT %s OFFSET %s' if limit else 'SELECT user_id, username, first_name, last_name, joined_date FROM users ORDER BY joined_date DESC', (limit, offset) if limit else ())
    else:
        cursor.execute('SELECT user_id, username, first_name, last_name, joined_date FROM users ORDER BY joined_date DESC LIMIT ? OFFSET ?' if limit else 'SELECT user_id, username, first_name, last_name, joined_date FROM users ORDER BY joined_date DESC', (limit, offset) if limit else ())
    users = cursor.fetchall()
    conn.close()
    return users

def get_user_count():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM users')
    count = cursor.fetchone()[0]
    conn.close()
    return count

def get_force_sub_channel_count():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM force_sub_channels WHERE is_active = %s' if USE_POSTGRES else 'SELECT COUNT(*) FROM force_sub_channels WHERE is_active = 1', (True,) if USE_POSTGRES else ())
    count = cursor.fetchone()[0]
    conn.close()
    return count

def add_force_sub_channel(channel_username, channel_title):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        if USE_POSTGRES:
            cursor.execute('INSERT INTO force_sub_channels (channel_username, channel_title) VALUES (%s, %s) ON CONFLICT (channel_username) DO NOTHING', (channel_username, channel_title))
        else:
            cursor.execute('INSERT OR IGNORE INTO force_sub_channels (channel_username, channel_title) VALUES (?, ?)', (channel_username, channel_title))
        conn.commit()
        return cursor.rowcount > 0
    except Exception as e:
        logger.error(f"DB Error: {e}")
        return False
    finally:
        conn.close()

def get_all_force_sub_channels():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT channel_username, channel_title FROM force_sub_channels WHERE is_active = %s ORDER BY channel_title' if USE_POSTGRES else 'SELECT channel_username, channel_title FROM force_sub_channels WHERE is_active = 1 ORDER BY channel_title', (True,) if USE_POSTGRES else ())
    channels = cursor.fetchall()
    conn.close()
    return channels

def get_force_sub_channel_info(channel_username):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT channel_username, channel_title FROM force_sub_channels WHERE channel_username = %s AND is_active = %s' if USE_POSTGRES else 'SELECT channel_username, channel_title FROM force_sub_channels WHERE channel_username = ? AND is_active = 1', (channel_username, True) if USE_POSTGRES else (channel_username,))
    channel = cursor.fetchone()
    conn.close()
    return channel

def delete_force_sub_channel(channel_username):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('UPDATE force_sub_channels SET is_active = %s WHERE channel_username = %s' if USE_POSTGRES else 'UPDATE force_sub_channels SET is_active = 0 WHERE channel_username = ?', (False, channel_username) if USE_POSTGRES else (channel_username,))
    conn.commit()
    conn.close()

def generate_link_id(channel_username, user_id, is_permanent=False):
    link_id = secrets.token_urlsafe(16)
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('INSERT INTO generated_links (link_id, channel_username, user_id, is_permanent) VALUES (%s, %s, %s, %s)' if USE_POSTGRES else 'INSERT INTO generated_links (link_id, channel_username, user_id, is_permanent) VALUES (?, ?, ?, ?)', (link_id, channel_username, user_id, is_permanent))
    conn.commit()
    conn.close()
    return link_id

def get_link_info(link_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT channel_username, user_id, created_time, is_used, is_permanent FROM generated_links WHERE link_id = %s' if USE_POSTGRES else 'SELECT channel_username, user_id, created_time, is_used, is_permanent FROM generated_links WHERE link_id = ?', (link_id,))
    result = cursor.fetchone()
    conn.close()
    return result

def mark_link_used(link_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('UPDATE generated_links SET is_used = %s WHERE link_id = %s' if USE_POSTGRES else 'UPDATE generated_links SET is_used = 1 WHERE link_id = ?', (True, link_id) if USE_POSTGRES else (link_id,))
    conn.commit()
    conn.close()

def cleanup_expired_links():
    conn = get_db_connection()
    cursor = conn.cursor()
    expiry_time = datetime.now() - timedelta(minutes=LINK_EXPIRY_MINUTES)
    cursor.execute('DELETE FROM generated_links WHERE created_time < %s AND is_permanent = %s' if USE_POSTGRES else 'DELETE FROM generated_links WHERE created_time < ? AND is_permanent = 0', (expiry_time, False) if USE_POSTGRES else (expiry_time,))
    conn.commit()
    conn.close()

async def check_force_subscription(user_id, context):
    channels = get_all_force_sub_channels()
    not_joined = []
    for username, title in channels:
        try:
            member = await context.bot.get_chat_member(username, user_id)
            if member.status in ['left', 'kicked', 'restricted']:
                not_joined.append((username, title))
        except:
            not_joined.append((username, title))
    return not_joined

def is_admin(user_id):
    return user_id == ADMIN_ID

async def send_admin_menu(chat_id, context, query=None):
    if query:
        try:
            await query.message.delete()
        except:
            pass
    keyboard = [[InlineKeyboardButton("📊 BOT STATS", callback_data="admin_stats")], [InlineKeyboardButton("📢 MANAGE CHANNELS", callback_data="manage_force_sub")], [InlineKeyboardButton("🔗 GENERATE LINKS", callback_data="generate_links")], [InlineKeyboardButton("📣 BROADCAST", callback_data="admin_broadcast_start")], [InlineKeyboardButton("👤 USERS", callback_data="user_management")]]
    await context.bot.send_message(chat_id=chat_id, text="👨‍💼 <b>ADMIN PANEL</b>\n\nWelcome back!", parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))

async def send_admin_stats(query, context):
    stats = f"📊 <b>BOT STATISTICS</b>\n\n👤 Users: {get_user_count()}\n📢 Channels: {get_force_sub_channel_count()}\n🔗 Link Expiry: {LINK_EXPIRY_MINUTES} min\n💾 Database: {'PostgreSQL ✅' if USE_POSTGRES else 'SQLite'}\n\n<i>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</i>"
    keyboard = [[InlineKeyboardButton("🔄 REFRESH", callback_data="admin_stats")], [InlineKeyboardButton("🔙 BACK", callback_data="admin_back")]]
    try:
        await query.edit_message_text(text=stats, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))
    except:
        try:
            await query.message.delete()
        except:
            pass
        await context.bot.send_message(chat_id=query.message.chat_id, text=stats, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))

async def show_force_sub_management(query, context):
    channels = get_all_force_sub_channels()
    text = "📢 <b>FORCE SUBSCRIPTION CHANNELS</b>\n\n"
    text += "No channels configured." if not channels else "<b>Channels:</b>\n" + "\n".join([f"• {title} (<code>{username}</code>)" for username, title in channels])
    keyboard = [[InlineKeyboardButton("➕ ADD CHANNEL", callback_data="add_channel_start")]]
    if channels:
        for i in range(0, len(channels), 2):
            keyboard.append([InlineKeyboardButton(channels[j][1], callback_data=f"channel_{channels[j][0].replace('@', '')}") for j in range(i, min(i+2, len(channels)))])
    keyboard.append([InlineKeyboardButton("🔙 BACK", callback_data="admin_back")])
    try:
        await query.edit_message_text(text=text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))
    except:
        try:
            await query.message.delete()
        except:
            pass
        await context.bot.send_message(chat_id=query.message.chat_id, text=text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))

async def show_channel_details(query, context, username_clean):
    info = get_force_sub_channel_info('@' + username_clean)
    if not info:
        await query.edit_message_text("❌ Channel not found.", parse_mode='HTML')
        return
    text = f"📢 <b>CHANNEL DETAILS</b>\n\n<b>Title:</b> {info[1]}\n<b>Username:</b> <code>{info[0]}</code>\n<b>Status:</b> Active"
    keyboard = [[InlineKeyboardButton("🔗 TEMP LINK", callback_data=f"genlink_{username_clean}")], [InlineKeyboardButton("♾️ PERMANENT LINK", callback_data=f"genperm_{username_clean}")], [InlineKeyboardButton("🗑️ DELETE", callback_data=f"delete_{username_clean}")], [InlineKeyboardButton("🔙 BACK", callback_data="manage_force_sub")]]
    await query.edit_message_text(text=text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))

async def send_user_management(query, context, offset=0):
    count = get_user_count()
    users = get_all_users(limit=10, offset=offset)
    text = f"👤 <b>USER MANAGEMENT</b>\n\n<b>Total:</b> {count}\n<b>Showing:</b> {offset+1}-{min(offset+10, count)}\n\n"
    for uid, uname, fname, lname, jdate in users:
        name = f"{fname or ''} {lname or ''}".strip() or "N/A"
        user = f"@{uname}" if uname else f"ID: {uid}"
        try:
            date = jdate.strftime('%Y-%m-%d %H:%M') if USE_POSTGRES else datetime.fromisoformat(jdate).strftime('%Y-%m-%d %H:%M')
        except:
            date = "Unknown"
        text += f"<b>{name}</b> (<code>{user}</code>)\n{date}\n\n"
    keyboard = []
    if offset > 0 or count > offset + 10:
        row = []
        if offset > 0:
            row.append(InlineKeyboardButton("⬅️ PREV", callback_data=f"user_page_{offset-10}"))
        if count > offset + 10:
            row.append(InlineKeyboardButton("NEXT ➡️", callback_data=f"user_page_{offset+10}"))
        keyboard.append(row)
    keyboard.extend([[InlineKeyboardButton("🔄 REFRESH", callback_data="user_management")], [InlineKeyboardButton("🔙 BACK", callback_data="admin_back")]])
    try:
        await query.edit_message_text(text=text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))
    except:
        if not query.data.startswith("user_page_"):
            try:
                await query.message.delete()
            except:
                pass
        await context.bot.send_message(chat_id=query.message.chat_id, text=text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    add_user(user.id, user.username, user.first_name, user.last_name)
    if context.args:
        await handle_channel_link_deep(update, context, context.args[0])
        return
    if not is_admin(user.id):
        not_joined = await check_force_subscription(user.id, context)
        if not_joined:
            keyboard = [[InlineKeyboardButton(f"📢 JOIN {title}", url=f"https://t.me/{username[1:]}")] for username, title in not_joined]
            keyboard.append([InlineKeyboardButton("✅ VERIFY", callback_data="verify_subscription")])
            text = "📢 <b>JOIN OUR CHANNELS!</b>\n\n<b>Required:</b>\n" + "\n".join([f"• {title}" for _, title in not_joined])
            await update.message.reply_text(text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))
            return
    if is_admin(user.id):
        await send_admin_menu(update.effective_chat.id, context)
    else:
        keyboard = [[InlineKeyboardButton("ANIME CHANNEL", url=PUBLIC_ANIME_CHANNEL_URL)], [InlineKeyboardButton("CONTACT ADMIN", url=f"https://t.me/{ADMIN_CONTACT_USERNAME}")], [InlineKeyboardButton("REQUEST", url=REQUEST_CHANNEL_URL)], [InlineKeyboardButton("ABOUT", callback_data="about_bot"), InlineKeyboardButton("CLOSE", callback_data="close_message")]]
        try:
            await context.bot.copy_message(chat_id=update.effective_chat.id, from_chat_id=WELCOME_SOURCE_CHANNEL, message_id=WELCOME_SOURCE_MESSAGE_ID, reply_markup=InlineKeyboardMarkup(keyboard))
        except:
            await update.message.reply_text("👋 <b>WELCOME!</b>", parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))

async def handle_channel_link_deep(update: Update, context: ContextTypes.DEFAULT_TYPE, link_id):
    info = get_link_info(link_id)
    if not info:
        await update.message.reply_text("❌ Invalid or expired link.", parse_mode='HTML')
        return
    channel_id, creator, created, used, permanent = info
    if not permanent:
        if used:
            await update.message.reply_text("❌ Link already used.", parse_mode='HTML')
            return
        age = datetime.now() - (created if USE_POSTGRES else datetime.fromisoformat(created))
        if age.total_seconds() > LINK_EXPIRY_MINUTES * 60:
            await update.message.reply_text("❌ Link expired.", parse_mode='HTML')
            return
    not_joined = await check_force_subscription(update.effective_user.id, context)
    if not_joined:
        keyboard = [[InlineKeyboardButton(f"📢 JOIN {title}", url=f"https://t.me/{username[1:]}")] for username, title in not_joined]
        keyboard.append([InlineKeyboardButton("✅ VERIFY", callback_data=f"verify_deep_{link_id}")])
        await update.message.reply_text(f"📢 <b>JOIN TO ACCESS!</b>\n\n" + "\n".join([f"• {title}" for _, title in not_joined]), parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))
        return
    try:
        chan_id = int(channel_id) if channel_id.lstrip('-').isdigit() else channel_id
        chat = await context.bot.get_chat(chan_id)
        invite = await context.bot.create_chat_invite_link(chat.id, member_limit=1, expire_date=int(datetime.now().timestamp()) + 300)
        if not permanent:
            mark_link_used(link_id)
        await update.message.reply_text(f"✅ <b>ACCESS GRANTED!</b>\n\n<b>Channel:</b> {chat.title}\n<b>Expires:</b> 5 min\n<b>Usage:</b> Single use", parse_mode='HTML', reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔗 JOIN", url=invite.invite_link)]]))
    except Exception as e:
        logger.error(f"Link error: {e}")
        await update.message.reply_text("❌ Error generating access link.", parse_mode='HTML')

async def broadcast_message_to_all_users(update: Update, context: ContextTypes.DEFAULT_TYPE, message):
    users = get_all_users(limit=None)
    success = 0
    await update.message.reply_text(f"🔄 Broadcasting to {len(users)} users...", parse_mode='HTML')
    for user in users:
        try:
            await context.bot.copy_message(chat_id=user[0], from_chat_id=message.chat_id, message_id=message.message_id)
            success += 1
        except:
            pass
        await asyncio.sleep(0.1)
    await context.bot.send_message(chat_id=update.effective_chat.id, text=f"✅ <b>Broadcast complete!</b>\n\nSent to {success}/{len(users)} users.", parse_mode='HTML')

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    data = query.data
    if user_id in user_states and data in ["admin_back", "admin_stats", "manage_force_sub", "generate_links", "user_management"]:
        del user_states[user_id]
    if data == "close_message":
        try:
            await query.message.delete()
        except:
            pass
        return
    if data == "admin_broadcast_start":
        if not is_admin(user_id):
            await query.edit_message_text("❌ Admin only.", parse_mode='HTML')
            return
        user_states[user_id] = PENDING_BROADCAST
        try:
            await query.message.delete()
        except:
            pass
        await context.bot.send_message(chat_id=query.message.chat_id, text="📣 <b>BROADCAST MODE</b>\n\nForward or send the message to broadcast now.", parse_mode='HTML', reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 CANCEL", callback_data="admin_back")]]))
        return
    if data == "verify_subscription":
        not_joined = await check_force_subscription(user_id, context)
        if not_joined:
            await query.edit_message_text(f"❌ <b>NOT ALL JOINED!</b>\n\nMissing:\n" + "\n".join([f"• {title}" for _, title in not_joined]), parse_mode='HTML')
            return
        if is_admin(user_id):
            try:
                await query.message.delete()
            except:
                pass
            await send_admin_menu(query.message.chat_id, context)
        else:
            keyboard = [[InlineKeyboardButton("ANIME", url=PUBLIC_ANIME_CHANNEL_URL)], [InlineKeyboardButton("ADMIN", url=f"https://t.me/{ADMIN_CONTACT_USERNAME}")], [InlineKeyboardButton("REQUEST", url=REQUEST_CHANNEL_URL)], [InlineKeyboardButton("ABOUT", callback_data="about_bot"), InlineKeyboardButton("CLOSE", callback_data="close_message")]]
            try:
                await query.message.delete()
            except:
                pass
            try:
                await context.bot.copy_message(chat_id=query.message.chat_id, from_chat_id=WELCOME_SOURCE_CHANNEL, message_id=WELCOME_SOURCE_MESSAGE_ID, reply_markup=InlineKeyboardMarkup(keyboard))
            except:
                await context.bot.send_message(query.message.chat_id, "✅ <b>VERIFIED!</b>", parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))
    elif data.startswith("verify_deep_"):
        link_id = data[12:]
        not_joined = await check_force_subscription(user_id, context)
        if not_joined:
            await query.edit_message_text(f"❌ <b>NOT ALL JOINED!</b>\n\nMissing:\n" + "\n".join([f"• {title}" for _, title in not_joined]), parse_mode='HTML')
            return
        info = get_link_info(link_id)
        if not info:
            await query.edit_message_text("❌ Link invalid.", parse_mode='HTML')
            return
        try:
            chan_id = int(info[0]) if info[0].lstrip('-').isdigit() else info[0]
            chat = await context.bot.get_chat(chan_id)
            invite = await context.bot.create_chat_invite_link(chat.id, member_limit=1, expire_date=int(datetime.now().timestamp()) + 300)
            if not info[4]:
                mark_link_used(link_id)
            try:
                await query.message.delete()
            except:
                pass
            await context.bot.send_message(chat_id=query.message.chat_id, text=f"✅ <b>ACCESS GRANTED!</b>\n\n<b>Channel:</b> {chat.title}\n<b>Expires:</b> 5 min", parse_mode='HTML', reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔗 JOIN", url=invite.invite_link)]]))
        except Exception as e:
            logger.error(f"Verify error: {e}")
            await query.edit_message_text("❌ Error accessing channel.", parse_mode='HTML')
    elif data == "admin_stats":
        if not is_admin(user_id):
            await query.edit_message_text("❌ Admin only.", parse_mode='HTML')
            return
        await send_admin_stats(query, context)
    elif data == "user_management":
        if not is_admin(user_id):
            await query.edit_message_text("❌ Admin only.", parse_mode='HTML')
            return
        await send_user_management(query, context, 0)
    elif data.startswith("user_page_"):
        if not is_admin(user_id):
            await query.edit_message_text("❌ Admin only.", parse_mode='HTML')
            return
        try:
            offset = int(data[10:])
        except:
            offset = 0
        await send_user_management(query, context, offset)
    elif data == "manage_force_sub":
        if not is_admin(user_id):
            await query.edit_message_text("❌ Admin only.", parse_mode='HTML')
            return
        await show_force_sub_management(query, context)
    elif data == "generate_links":
        if not is_admin(user_id):
            await query.edit_message_text("❌ Admin only.", parse_mode='HTML')
            return
        user_states[user_id] = GENERATE_LINK_CHANNEL_USERNAME
        try:
            await query.message.delete()
        except:
            pass
        await context.bot.send_message(chat_id=query.message.chat_id, text="🔗 <b>GENERATE LINKS</b>\n\nSend channel username (e.g., <code>@YourChannel</code>) or ID (e.g., <code>-1001234567890</code>)", parse_mode='HTML', reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 CANCEL", callback_data="admin_back")]]))
    elif data.startswith("genperm_") or data.startswith("genlink_"):
        if not is_admin(user_id):
            await query.edit_message_text("❌ Admin only.", parse_mode='HTML')
            return
        is_perm = data.startswith("genperm_")
        username = '@' + data[8:]
        info = get_force_sub_channel_info(username)
        if not info:
            await query.edit_message_text("❌ Channel not found.", parse_mode='HTML')
            return
        try:
            link_id = generate_link_id(username, user_id, is_perm)
            bot_username = context.bot.username
            link = f"https://t.me/{bot_username}?start={link_id}"
            await query.message.delete()
            await context.bot.send_message(chat_id=query.message.chat_id, text=f"{'♾️ <b>PERMANENT' if is_perm else '🔗 <b>TEMPORARY'} LINK</b>\n\n<b>Channel:</b> {info[1]}\n<b>Username:</b> <code>{username}</code>\n<b>Expires:</b> {'Never' if is_perm else f'{LINK_EXPIRY_MINUTES} min'}\n<b>Usage:</b> {'Unlimited' if is_perm else 'Single use'}\n\n<b>Link:</b>\n<code>{link}</code>", parse_mode='HTML', reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 BACK", callback_data=f"channel_{data[8:]}")]]))
        except Exception as e:
            logger.error(f"Gen link error: {e}")
            await query.edit_message_text("❌ Error generating link.", parse_mode='HTML')
    elif data == "add_channel_start":
        if not is_admin(user_id):
            await query.edit_message_text("❌ Admin only.", parse_mode='HTML')
            return
        user_states[user_id] = ADD_CHANNEL_USERNAME
        try:
            await query.message.delete()
        except:
            pass
        await context.bot.send_message(chat_id=query.message.chat_id, text="📢 <b>ADD CHANNEL</b>\n\nSend channel username (e.g., <code>@YourChannel</code>)", parse_mode='HTML', reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 CANCEL", callback_data="manage_force_sub")]]))
    elif data.startswith("channel_"):
        if not is_admin(user_id):
            await query.edit_message_text("❌ Admin only.", parse_mode='HTML')
            return
        await show_channel_details(query, context, data[8:])
    elif data.startswith("delete_"):
        if not is_admin(user_id):
            await query.edit_message_text("❌ Admin only.", parse_mode='HTML')
            return
        username = '@' + data[7:]
        info = get_force_sub_channel_info(username)
        if info:
            keyboard = [[InlineKeyboardButton("✅ YES DELETE", callback_data=f"confirm_delete_{data[7:]}")], [InlineKeyboardButton("❌ CANCEL", callback_data=f"channel_{data[7:]}")]]
            await query.edit_message_text(f"🗑️ <b>CONFIRM DELETE</b>\n\n<b>Channel:</b> {info[1]}\n<b>Username:</b> <code>{username}</code>\n\n<i>This cannot be undone!</i>", parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))
    elif data.startswith("confirm_delete_"):
        if not is_admin(user_id):
            await query.edit_message_text("❌ Admin only.", parse_mode='HTML')
            return
        username = '@' + data[15:]
        delete_force_sub_channel(username)
        await query.edit_message_text(f"✅ <b>DELETED</b>\n\nChannel <code>{username}</code> removed.", parse_mode='HTML', reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📢 MANAGE", callback_data="manage_force_sub")]]))
    elif data == "admin_back":
        await send_admin_menu(query.message.chat_id, context, query)
    elif data == "user_back":
        keyboard = [[InlineKeyboardButton("ANIME", url=PUBLIC_ANIME_CHANNEL_URL)], [InlineKeyboardButton("ADMIN", url=f"https://t.me/{ADMIN_CONTACT_USERNAME}")], [InlineKeyboardButton("REQUEST", url=REQUEST_CHANNEL_URL)], [InlineKeyboardButton("ABOUT", callback_data="about_bot"), InlineKeyboardButton("CLOSE", callback_data="close_message")]]
        try:
            await query.message.delete()
        except:
            pass
        try:
            await context.bot.copy_message(chat_id=query.message.chat_id, from_chat_id=WELCOME_SOURCE_CHANNEL, message_id=WELCOME_SOURCE_MESSAGE_ID, reply_markup=InlineKeyboardMarkup(keyboard))
        except:
            await context.bot.send_message(query.message.chat_id, "🏠 <b>MAIN MENU</b>", parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))
    elif data == "about_bot":
        try:
            await query.message.delete()
        except:
            pass
        await context.bot.send_message(chat_id=query.message.chat_id, text="<b>About Us</b>\n\n⇨ <b>Made for: @Beat_Anime_Ocean</b>\n⇨ <b>Owned by: @Beat_Anime_Ocean</b>\n⇨ <b>Developer: @Beat_Anime_Ocean</b>\n\n<i>Adios!!</i>", parse_mode='HTML', reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 BACK", callback_data="user_back")]]))

async def handle_admin_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in user_states:
        return
    state = user_states[user_id]
    if state == PENDING_BROADCAST:
        del user_states[user_id]
        await broadcast_message_to_all_users(update, context, update.message)
        await send_admin_menu(update.effective_chat.id, context)
        return
    text = update.message.text
    if not text:
        await update.message.reply_text("❌ Please send text.", parse_mode='HTML')
        return
    if state == ADD_CHANNEL_USERNAME:
        if not text.startswith('@'):
            await update.message.reply_text("❌ Username must start with @. Try again:", parse_mode='HTML')
            return
        context.user_data['channel_username'] = text
        user_states[user_id] = ADD_CHANNEL_TITLE
        await update.message.reply_text("📝 <b>STEP 2: Channel Title</b>\n\nSend the display title (e.g., <i>Anime Ocean</i>)", parse_mode='HTML', reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 CANCEL", callback_data="manage_force_sub")]]))
    elif state == ADD_CHANNEL_TITLE:
        username = context.user_data.get('channel_username')
        if add_force_sub_channel(username, text):
            del user_states[user_id]
            del context.user_data['channel_username']
            await update.message.reply_text(f"✅ <b>CHANNEL ADDED!</b>\n\n<b>Username:</b> <code>{username}</code>\n<b>Title:</b> {text}", parse_mode='HTML', reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📢 MANAGE", callback_data="manage_force_sub")]]))
        else:
            await update.message.reply_text("❌ Error adding channel. May already exist.", parse_mode='HTML')
    elif state == GENERATE_LINK_CHANNEL_USERNAME:
        channel = text.strip()
        if not (channel.startswith('@') or channel.startswith('-100') or channel.lstrip('-').isdigit()):
            await update.message.reply_text("❌ Invalid format. Send <code>@Username</code> or <code>-1001234567890</code>", parse_mode='HTML')
            return
        del user_states[user_id]
        try:
            chat = await context.bot.get_chat(channel)
            keyboard = [[InlineKeyboardButton("⏱️ TEMP LINK", callback_data=f"maketemp_{channel}")], [InlineKeyboardButton("♾️ PERMANENT LINK", callback_data=f"makeperm_{channel}")], [InlineKeyboardButton("🔙 CANCEL", callback_data="admin_back")]]
            await update.message.reply_text(f"📢 <b>Channel Verified!</b>\n\n<b>Channel:</b> {chat.title}\n<b>ID:</b> <code>{channel}</code>\n\nChoose link type:", parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))
        except Exception as e:
            logger.error(f"Channel access error: {e}")
            await update.message.reply_text("❌ <b>Cannot access channel!</b>\n\nEnsure:\n1. Bot is admin\n2. Bot can create invite links\n3. ID/username is correct", parse_mode='HTML')

async def button_handler_continued(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data
    user_id = query.from_user.id
    if data.startswith("maketemp_") or data.startswith("makeperm_"):
        if not is_admin(user_id):
            return
        is_perm = data.startswith("makeperm_")
        channel = data[9:]
        try:
            chat = await context.bot.get_chat(channel)
            link_id = generate_link_id(str(channel), user_id, is_perm)
            bot_username = context.bot.username
            link = f"https://t.me/{bot_username}?start={link_id}"
            await query.message.delete()
            await context.bot.send_message(chat_id=query.message.chat_id, text=f"{'♾️ <b>PERMANENT' if is_perm else '🔗 <b>TEMPORARY'} LINK</b>\n\n<b>Channel:</b> {chat.title}\n<b>ID:</b> <code>{channel}</code>\n<b>Expires:</b> {'Never' if is_perm else f'{LINK_EXPIRY_MINUTES} min'}\n<b>Usage:</b> {'Unlimited' if is_perm else 'Single use'}\n\n<b>Link:</b>\n<code>{link}</code>", parse_mode='HTML', reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 MENU", callback_data="admin_back")]]))
        except Exception as e:
            logger.error(f"Link gen error: {e}")
            await query.edit_message_text("❌ Error generating link.", parse_mode='HTML')

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Error: {context.error}")

async def cleanup_task(context: ContextTypes.DEFAULT_TYPE):
    cleanup_expired_links()

def main():
    init_db()
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN missing!")
        return
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.User(user_id=ADMIN_ID) & ~filters.COMMAND, handle_admin_message))
    app.add_error_handler(error_handler)
    if app.job_queue:
        app.job_queue.run_repeating(cleanup_task, interval=600, first=10)
    if WEBHOOK_URL and BOT_TOKEN:
        Thread(target=keep_alive, daemon=True).start()
        logger.info("Starting webhook mode with keep-alive")
        app.run_webhook(listen="0.0.0.0", port=PORT, url_path=BOT_TOKEN, webhook_url=WEBHOOK_URL + BOT_TOKEN)
    else:
        logger.info("Starting polling mode")
        app.run_polling()

if __name__ == '__main__':
    if 'PORT' not in os.environ:
        os.environ['PORT'] = str(8080)
    main()
