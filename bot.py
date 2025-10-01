import os
import logging
import secrets
import requests
import time
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, MessageHandler, filters
import asyncio
from threading import Thread
import psycopg2
from urllib.parse import urlparse

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Bot configuration
BOT_TOKEN = os.environ.get('BOT_TOKEN', '7877393813:AAGKvpRBlYWwO70B9pQpD29BhYCXwiZGngw')
ADMIN_ID = int(os.environ.get('ADMIN_ID', 829342319))
LINK_EXPIRY_MINUTES = 5

# Global variables for webhook configuration
PORT = int(os.environ.get('PORT', 8080))
WEBHOOK_URL = os.environ.get('RENDER_EXTERNAL_URL', '').rstrip('/') + '/'

# Customization constants
WELCOME_SOURCE_CHANNEL = -1002530952988
WELCOME_SOURCE_MESSAGE_ID = 32  
PUBLIC_ANIME_CHANNEL_URL = "https://t.me/BeatAnime"
REQUEST_CHANNEL_URL = "https://t.me/Beat_Hindi_Dubbed"
ADMIN_CONTACT_USERNAME = "Beat_Anime_Ocean"

# User states
ADD_CHANNEL_USERNAME, ADD_CHANNEL_TITLE, GENERATE_LINK_CHANNEL_USERNAME, PENDING_BROADCAST = range(4)
user_states = {}

# Database configuration
def get_db_connection():
    """Get database connection from environment variable"""
    database_url = os.environ.get('DATABASE_URL')
    
    if database_url:
        # Parse the database URL (Render provides this)
        result = urlparse(database_url)
        return psycopg2.connect(
            database=result.path[1:],
            user=result.username,
            password=result.password,
            host=result.hostname,
            port=result.port
        )
    else:
        # Fallback to SQLite for local development
        import sqlite3
        return sqlite3.connect('bot_data.db')

def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Users table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            joined_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_active BOOLEAN DEFAULT TRUE
        )
    ''')
    
    # Force sub channels table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS force_sub_channels (
            channel_id SERIAL PRIMARY KEY,
            channel_username TEXT UNIQUE,
            channel_title TEXT,
            added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_active BOOLEAN DEFAULT TRUE
        )
    ''')
    
    # Generated links table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS generated_links (
            link_id TEXT PRIMARY KEY,
            channel_username TEXT,
            user_id BIGINT,
            created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_used BOOLEAN DEFAULT FALSE,
            is_permanent BOOLEAN DEFAULT FALSE,
            parent_link_id TEXT DEFAULT NULL
        )
    ''')
    
    conn.commit()
    conn.close()
    logger.info("Database tables initialized/verified")

def add_user(user_id, username, first_name, last_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT INTO users (user_id, username, first_name, last_name) 
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (user_id) 
            DO UPDATE SET 
                username = EXCLUDED.username,
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                is_active = TRUE
        ''', (user_id, username, first_name, last_name))
        conn.commit()
    except Exception as e:
        logger.error(f"Error adding user: {e}")
    finally:
        conn.close()

def get_all_users(limit=20, offset=0):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        if limit is None:
            cursor.execute('''
                SELECT user_id, username, first_name, last_name, joined_date 
                FROM users WHERE is_active = TRUE 
                ORDER BY joined_date DESC
            ''')
        else:
            cursor.execute('''
                SELECT user_id, username, first_name, last_name, joined_date 
                FROM users WHERE is_active = TRUE 
                ORDER BY joined_date DESC LIMIT %s OFFSET %s
            ''', (limit, offset))
        users = cursor.fetchall()
        return users
    except Exception as e:
        logger.error(f"Error getting users: {e}")
        return []
    finally:
        conn.close()

def get_user_count():
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT COUNT(*) FROM users WHERE is_active = TRUE')
        count = cursor.fetchone()[0]
        return count
    except Exception as e:
        logger.error(f"Error getting user count: {e}")
        return 0
    finally:
        conn.close()

def get_force_sub_channel_count():
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT COUNT(*) FROM force_sub_channels WHERE is_active = TRUE')
        count = cursor.fetchone()[0]
        return count
    except Exception as e:
        logger.error(f"Error getting channel count: {e}")
        return 0
    finally:
        conn.close()

def add_force_sub_channel(channel_username, channel_title):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT INTO force_sub_channels (channel_username, channel_title) 
            VALUES (%s, %s)
            ON CONFLICT (channel_username) 
            DO UPDATE SET 
                channel_title = EXCLUDED.channel_title,
                is_active = TRUE
        ''', (channel_username, channel_title))
        conn.commit()
        return cursor.rowcount > 0
    except Exception as e:
        logger.error(f"Error adding channel: {e}")
        return False
    finally:
        conn.close()

def get_all_force_sub_channels():
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            SELECT channel_username, channel_title 
            FROM force_sub_channels 
            WHERE is_active = TRUE 
            ORDER BY channel_title
        ''')
        channels = cursor.fetchall()
        return channels
    except Exception as e:
        logger.error(f"Error getting channels: {e}")
        return []
    finally:
        conn.close()

def get_force_sub_channel_info(channel_username):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            SELECT channel_username, channel_title 
            FROM force_sub_channels 
            WHERE channel_username = %s AND is_active = TRUE
        ''', (channel_username,))
        channel = cursor.fetchone()
        return channel
    except Exception as e:
        logger.error(f"Error getting channel info: {e}")
        return None
    finally:
        conn.close()

def delete_force_sub_channel(channel_username):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            UPDATE force_sub_channels 
            SET is_active = FALSE 
            WHERE channel_username = %s
        ''', (channel_username,))
        conn.commit()
    except Exception as e:
        logger.error(f"Error deleting channel: {e}")
    finally:
        conn.close()

def generate_link_id(channel_username, user_id, permanent=False, parent_link_id=None):
    link_id = secrets.token_urlsafe(16)
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT INTO generated_links (link_id, channel_username, user_id, is_permanent, parent_link_id)
            VALUES (%s, %s, %s, %s, %s)
        ''', (link_id, channel_username, user_id, permanent, parent_link_id))
        conn.commit()
        return link_id
    except Exception as e:
        logger.error(f"Error generating link: {e}")
        return None
    finally:
        conn.close()

def get_link_info(link_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            SELECT channel_username, user_id, created_time, is_used, is_permanent, parent_link_id
            FROM generated_links WHERE link_id = %s
        ''', (link_id,))
        result = cursor.fetchone()
        return result
    except Exception as e:
        logger.error(f"Error getting link info: {e}")
        return None
    finally:
        conn.close()

def mark_link_used(link_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('UPDATE generated_links SET is_used = TRUE WHERE link_id = %s', (link_id,))
        conn.commit()
    except Exception as e:
        logger.error(f"Error marking link used: {e}")
    finally:
        conn.close()

def cleanup_expired_links():
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        expiry_time = datetime.now() - timedelta(minutes=LINK_EXPIRY_MINUTES)
        cursor.execute('''
            DELETE FROM generated_links 
            WHERE created_time < %s AND is_permanent = FALSE
        ''', (expiry_time,))
        conn.commit()
        logger.info("Cleaned up expired links")
    except Exception as e:
        logger.error(f"Error cleaning up links: {e}")
    finally:
        conn.close()

def mark_user_inactive(user_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('UPDATE users SET is_active = FALSE WHERE user_id = %s', (user_id,))
        conn.commit()
        logger.info(f"Marked user {user_id} as inactive")
    except Exception as e:
        logger.error(f"Error marking user inactive: {e}")
    finally:
        conn.close()

# Keep-alive service
def keep_alive():
    """Pings a reliable external URL every 14 minutes to prevent sleep"""
    while True:
        try:
            time.sleep(840)
            response = requests.get("https://www.google.com/robots.txt", timeout=10)
            logger.info(f"Keep-alive: Sent outgoing ping ({response.status_code}). Bot remains active 24/7.")
        except Exception as e:
            logger.error(f"Keep-alive error: {e}")

async def check_force_subscription(user_id, context):
    channels = get_all_force_sub_channels()
    not_joined_channels = []
    
    for channel_username, channel_title in channels:
        try:
            # Ensure channel_username starts with @
            if not channel_username.startswith('@'):
                channel_username = '@' + channel_username
                
            member = await context.bot.get_chat_member(channel_username, user_id)
            if member.status in ['left', 'kicked']:
                not_joined_channels.append((channel_username, channel_title))
        except Exception as e:
            logger.error(f"Error checking subscription for {channel_username}: {e}")
            # If we can't check, assume user hasn't joined
            not_joined_channels.append((channel_username, channel_title))
    
    return not_joined_channels

def is_admin(user_id):
    return user_id == ADMIN_ID

async def send_admin_menu(chat_id, context, query=None):
    if query:
        try:
            await query.delete_message()
        except Exception:
            pass
            
    keyboard = [
        [InlineKeyboardButton("📊 BOT STATS", callback_data="admin_stats")],
        [InlineKeyboardButton("📢 MANAGE FORCE SUB CHANNELS", callback_data="manage_force_sub")],
        [InlineKeyboardButton("🔗 GENERATE CHANNEL LINKS", callback_data="generate_links")],
        [InlineKeyboardButton("📣 START MEDIA BROADCAST", callback_data="admin_broadcast_start")],
        [InlineKeyboardButton("👤 USER MANAGEMENT", callback_data="user_management")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    text = "👨‍💼 <b>ADMIN PANEL</b> 👨‍💼\n\nWelcome back, Admin! Choose an option below:"
    
    await context.bot.send_message(
        chat_id=chat_id,
        text=text,
        parse_mode='HTML',
        reply_markup=reply_markup
    )

async def send_admin_stats(query, context):
    try:
        await query.delete_message()
    except Exception:
        pass
        
    user_count = get_user_count()
    channel_count = get_force_sub_channel_count()
    
    stats_text = (
        "📊 <b>BOT STATISTICS</b> 📊\n\n" +
        f"👤 <b>Total Users:</b> {user_count}\n" +
        f"📢 <b>Force Sub Channels:</b> {channel_count}\n" +
        f"🔗 <b>Link Expiry:</b> {LINK_EXPIRY_MINUTES} minutes\n\n" +
        f"<i>Last Cleanup:</i> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    
    keyboard = [
        [InlineKeyboardButton("🔄 REFRESH", callback_data="admin_stats")],
        [InlineKeyboardButton("🔙 BACK", callback_data="admin_back")]
    ]
    
    await context.bot.send_message(
        chat_id=query.message.chat_id, 
        text=stats_text, 
        parse_mode='HTML', 
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def show_force_sub_management(query, context):
    channels = get_all_force_sub_channels()
    
    channels_text = "📢 <b>FORCE SUBSCRIPTION CHANNELS</b> 📢\n\n"
    
    if not channels:
        channels_text += "No channels configured currently."
    else:
        channels_text += "<b>Configured Channels:</b>\n"
        for channel_username, channel_title in channels:
            channels_text += f"• {channel_title} (<code>{channel_username}</code>)\n"

    keyboard = [
        [InlineKeyboardButton("➕ ADD NEW CHANNEL", callback_data="add_channel_start")]
    ]
    
    if channels:
        channel_buttons = [
            InlineKeyboardButton(channel_title, callback_data=f"channel_{channel_username.replace('@', '')}") 
            for channel_username, channel_title in channels
        ]
        
        grouped_buttons = [channel_buttons[i:i + 2] for i in range(0, len(channel_buttons), 2)]
        
        for row in grouped_buttons:
            keyboard.append(row)

        keyboard.append([InlineKeyboardButton("🗑️ DELETE CHANNEL", callback_data="delete_channel_prompt")])

    keyboard.append([InlineKeyboardButton("🔙 BACK TO MENU", callback_data="admin_back")])
    
    try:
        await query.delete_message()
    except Exception:
        pass
        
    await context.bot.send_message(
        chat_id=query.message.chat_id,
        text=channels_text,
        parse_mode='HTML',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def show_channel_details(query, context, channel_username_clean):
    channel_username = '@' + channel_username_clean
    channel_info = get_force_sub_channel_info(channel_username)
    
    if not channel_info:
        await query.edit_message_text(
            "❌ Channel not found.", 
            parse_mode='HTML', 
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 MANAGE CHANNELS", callback_data="manage_force_sub")]])
        )
        return
        
    channel_username, channel_title = channel_info
    
    details_text = f"""
📢 <b>CHANNEL DETAILS</b> 📢

<b>Title:</b> {channel_title}
<b>Username:</b> <code>{channel_username}</code>
<b>Status:</b> <i>Active Force Sub</i>

<i>Choose an action below.</i>
    """
    
    keyboard = [
        [InlineKeyboardButton("🔗 GENERATE PERMANENT LINK", callback_data=f"genlink_{channel_username_clean}")],
        [InlineKeyboardButton("🗑️ DELETE CHANNEL", callback_data=f"delete_{channel_username_clean}")],
        [InlineKeyboardButton("🔙 BACK TO MANAGEMENT", callback_data="manage_force_sub")]
    ]
    
    await query.edit_message_text(
        text=details_text,
        parse_mode='HTML',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def send_user_management(query, context, offset=0):
    if not query.data.startswith("user_page_"):
        try:
            await query.delete_message()
        except Exception:
            pass
            
    user_count = get_user_count()
    users = get_all_users(limit=10, offset=offset)
    
    has_next = user_count > offset + 10
    has_prev = offset > 0
    
    user_list_text = ""
    if users:
        for user_id, username, first_name, last_name, joined_date in users:
            display_name = f"{first_name or ''} {last_name or ''}".strip() or "N/A"
            display_username = f"@{username}" if username else f"ID: {user_id}"
            
            try:
                formatted_date = datetime.fromisoformat(joined_date).strftime('%Y-%m-%d %H:%M')
            except:
                formatted_date = "Unknown"
            
            user_list_text += f"<b>{display_name}</b> (<code>{display_username}</code>)\n"
            user_list_text += f"Joined: {formatted_date}\n\n"
    
    if not user_list_text:
        user_list_text = "No users found in the database."

    stats_text = (
        "👤 <b>USER MANAGEMENT</b> 👤\n\n" +
        f"<b>Total Users:</b> {user_count}\n" +
        f"<b>Showing:</b> {offset + 1}-{min(offset + 10, user_count)} of {user_count}\n\n" +
        user_list_text
    )
    
    pagination_buttons = []
    if has_prev:
        pagination_buttons.append(InlineKeyboardButton("⬅️ PREV", callback_data=f"user_page_{offset - 10}"))
    if has_next:
        pagination_buttons.append(InlineKeyboardButton("NEXT ➡️", callback_data=f"user_page_{offset + 10}"))
        
    keyboard = []
    if pagination_buttons:
        keyboard.append(pagination_buttons)
    
    keyboard.append([InlineKeyboardButton("🔄 REFRESH", callback_data="user_management")])
    keyboard.append([InlineKeyboardButton("🔙 BACK TO MENU", callback_data="admin_back")])
    
    if query.data.startswith("user_page_"):
        await query.edit_message_text(
            text=stats_text,
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    else:
        await context.bot.send_message(
            chat_id=query.message.chat_id, 
            text=stats_text, 
            parse_mode='HTML', 
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    add_user(user.id, user.username, user.first_name, user.last_name)
    
    if context.args and len(context.args) > 0:
        link_id = context.args[0]
        await handle_channel_link_deep(update, context, link_id)
        return
    
    # Check force subscription for ALL users
    not_joined_channels = await check_force_subscription(user.id, context)
    
    if not_joined_channels:
        keyboard = []
        for channel_username, channel_title in not_joined_channels:
            clean_username = channel_username.lstrip('@')
            keyboard.append([InlineKeyboardButton(f"📢 ᴊᴏɪɴ {channel_title}", url=f"https://t.me/{clean_username}")])
        
        keyboard.append([InlineKeyboardButton("✅ ᴠᴇʀɪғʏ sᴜʙsᴄʀɪᴘᴛɪᴏɴ", callback_data="verify_subscription")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        channels_text = "\n".join([f"• {title} (<code>{username}</code>)" for username, title in not_joined_channels])
        
        await update.message.reply_text(
            f"📢 <b>ᴘʟᴇᴀsᴇ ᴊᴏɪɴ ᴏᴜʀ ᴄʜᴀɴɴᴇʟs ᴛᴏ ᴜsᴇ ᴛʜɪs ʙᴏᴛ!</b>\n\n"
            f"<b>ʀᴇǫᴜɪʀᴇᴅ ᴄʜᴀɴɴᴇʟs:</b>\n{channels_text}\n\n"
            f"ᴊᴏɪɴ ᴀʟʟ ᴄʜᴀɴɴᴇʟs ᴀʙᴏᴠᴇ ᴀɴᴅ ᴛʜᴇɴ ᴄʟɪᴄᴋ ᴠᴇʀɪғʏ sᴜʙsᴄʀɪᴘᴛɪᴏɴ.",
            parse_mode='HTML',
            reply_markup=reply_markup
        )
        return
    
    # Only show admin menu if user is admin AND has joined all channels
    if is_admin(user.id):
        await send_admin_menu(update.effective_chat.id, context)
    else:
        keyboard = [
            [InlineKeyboardButton("ᴀɴɪᴍᴇ ᴄʜᴀɴɴᴇʟ", url=PUBLIC_ANIME_CHANNEL_URL)], 
            [InlineKeyboardButton("ᴄᴏɴᴛᴀᴄᴛ ᴀᴅᴍɪɴ", url=f"https://t.me/{ADMIN_CONTACT_USERNAME}")],
            [InlineKeyboardButton("ʀᴇǫᴜᴇsᴛ ᴀɴɪᴍᴇ ᴄʜᴀɴɴᴇʟ", url=REQUEST_CHANNEL_URL)],
            [
                InlineKeyboardButton("ᴀʙᴏᴜᴛ ᴍᴇ", callback_data="about_bot"),
                InlineKeyboardButton("ᴄʟᴏsᴇ", callback_data="close_message")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        try:
            await context.bot.copy_message(
                chat_id=update.effective_chat.id,
                from_chat_id=WELCOME_SOURCE_CHANNEL,
                message_id=WELCOME_SOURCE_MESSAGE_ID,
                reply_markup=reply_markup
            )
        except Exception as e:
            logger.error(f"Error copying welcome message from channel: {e}")
            fallback_text = "👋 <b>ᴡᴇʟᴄᴏᴍᴇ ᴛᴏ ᴛʜᴇ ᴀᴅᴠᴀɴᴄᴇᴅ ʟɪɴᴋs sʜᴀʀɪɴɢ ʙᴏᴛ</b>\n\nᴜsᴇ ᴛʜɪs ʙᴏᴛ ᴛᴏ ɢᴇᴛ ᴀᴄᴄᴇss ᴛᴏ ᴏᴜʀ ᴇxᴄʟᴜsɪᴠᴇ ᴄᴏɴᴛᴇɴᴛ. ᴇxᴘʟᴏʀᴇ ᴛʜᴇ ᴏᴘᴛɪᴏɴs ʙᴇʟᴏᴡ ᴛᴏ ɢᴇᴛ sᴛᴀʀᴛᴇᴅ"
            await update.message.reply_text(fallback_text, parse_mode='HTML', reply_markup=reply_markup)

async def handle_channel_link_deep(update: Update, context: ContextTypes.DEFAULT_TYPE, link_id):
    link_info = get_link_info(link_id)
    
    if not link_info:
        await update.message.reply_text("❌ ᴛʜɪs ʟɪɴᴋ ʜᴀs ᴇxᴘɪʀᴇᴅ ᴏʀ ɪs ɪɴᴠᴀʟɪᴅ.", parse_mode='HTML')
        return
    
    channel_identifier, creator_id, created_time, is_used, is_permanent, parent_link_id = link_info
    
    user = update.effective_user
    
    # Check force subscription
    not_joined_channels = await check_force_subscription(user.id, context)
    if not_joined_channels:
        keyboard = []
        for chan_user, chan_title in not_joined_channels:
            clean_username = chan_user.lstrip('@')
            keyboard.append([InlineKeyboardButton(f"📢 ᴊᴏɪɴ {chan_title}", url=f"https://t.me/{clean_username}")])
        
        keyboard.append([InlineKeyboardButton("✅ ᴠᴇʀɪғʏ sᴜʙsᴄʀɪᴘᴛɪᴏɴ", callback_data=f"verify_deep_{link_id}")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        channels_text = "\n".join([f"• {title}" for _, title in not_joined_channels])
        
        await update.message.reply_text(
            f"📢 <b>ᴘʟᴇᴀsᴇ ᴊᴏɪɴ ᴏᴜʀ ᴄʜᴀɴɴᴇʟs ᴛᴏ ɢᴇᴛ ᴀᴄᴄᴇss!</b>\n\n"
            f"<b>ʀᴇǫᴜɪʀᴇᴅ ᴄʜᴀɴɴᴇʟs:</b>\n{channels_text}\n\n"
            f"ᴊᴏɪɴ ᴀʟʟ ᴄʜᴀɴɴᴇʟs ᴀʙᴏᴠᴇ ᴀɴᴅ ᴛʜᴇɴ ᴄʟɪᴄᴋ ᴠᴇʀɪғʏ sᴜʙsᴄʀɪᴘᴛɪᴏɴ.",
            parse_mode='HTML',
            reply_markup=reply_markup
        )
        return
    
    try:
        if channel_identifier.lstrip('-').isdigit():
            channel_identifier = int(channel_identifier)
        
        chat = await context.bot.get_chat(channel_identifier)
        
        # For permanent links, generate a new temporary link
        temp_link_id = generate_link_id(channel_identifier, user.id, permanent=False, parent_link_id=link_id)
        
        # Create temporary invite link (5 minutes)
        invite_link = await context.bot.create_chat_invite_link(
            chat.id, 
            member_limit=1,
            expire_date=int(datetime.now().timestamp()) + 300  # 5 minutes
        )
        
        mark_link_used(temp_link_id)  # Mark the temporary link as used
        
        success_message = (
            f"<b>ᴄʜᴀɴɴᴇʟ:</b> {chat.title}\n"
            f"<b>ᴇxᴘɪʀᴇs ɪɴ:</b> 5 minutes\n"
            f"<b>Usage:</b> One-time join\n\n"
            f"<i>ʜᴇʀᴇ ɪs ʏᴏᴜʀ ᴊᴏɪɴ ʟɪɴᴋ! ᴄʟɪᴄᴋ ʙᴇʟᴏᴡ ᴛᴏ ᴊᴏɪɴ:</i>"
        )
        
        keyboard = [[InlineKeyboardButton("🔗 Join Channel", url=invite_link.invite_link)]]
        
        await update.message.reply_text(
            success_message,
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
    except Exception as e:
        logger.error(f"Error generating invite link for {channel_identifier}: {e}")
        await update.message.reply_text("❌ ᴇʀʀᴏʀ ᴀᴄᴄᴇssɪɴɢ ᴄʜᴀɴɴᴇʟ ʟɪɴᴋ. ᴘʟᴇᴀsᴇ ᴄᴏɴᴛᴀᴄᴛ ᴛʜᴇ ᴀᴅᴍɪɴ ɪғ ᴛʜɪs ɪssᴜᴇ ᴘᴇʀsɪsᴛs.", parse_mode='HTML')

async def broadcast_message_to_all_users(update: Update, context: ContextTypes.DEFAULT_TYPE, message_to_copy):
    users = get_all_users(limit=None, offset=0)
    success_count = 0
    total_users = len(users)
    
    await update.message.reply_text(f"🔄 Starting broadcast to {total_users} users. Please wait.", parse_mode='HTML')

    for user in users:
        target_chat_id = user[0]
        try:
            await context.bot.copy_message(
                chat_id=target_chat_id,
                from_chat_id=message_to_copy.chat_id,
                message_id=message_to_copy.message_id
            )
            success_count += 1
        except Exception:
            pass
        await asyncio.sleep(0.1)
    
    await context.bot.send_message(
        chat_id=update.effective_chat.id, 
        text=f"✅ <b>Broadcast complete!</b>\n\n📊 Sent to {success_count}/{total_users} users.",
        parse_mode='HTML'
    )

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    data = query.data
    
    if user_id in user_states:
        current_state = user_states.get(user_id)
        if current_state in [PENDING_BROADCAST, GENERATE_LINK_CHANNEL_USERNAME, ADD_CHANNEL_TITLE, ADD_CHANNEL_USERNAME] and data in ["admin_back", "admin_stats", "manage_force_sub", "generate_links", "user_management"]:
            del user_states[user_id]
            
    if data == "close_message":
        try:
            await query.delete_message()
        except Exception as e:
            logger.warning(f"Could not delete message: {e}")
        return

    if data == "admin_broadcast_start":
        if not is_admin(user_id):
            await query.edit_message_text("❌ ᴀᴅᴍɪɴ ᴏɴʟʏ.", parse_mode='HTML')
            return
        
        user_states[user_id] = PENDING_BROADCAST
        
        keyboard = [[InlineKeyboardButton("🔙 CANCEL", callback_data="admin_back")]]
        
        try:
            await query.delete_message()
        except Exception:
            pass
            
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text="📣 <b>MEDIA BROADCAST MODE</b>\n\nPlease <b>forward</b> the message (image, video, file, sticker, or text) you wish to broadcast <i>now</i>.\n\n<b>Note:</b> Any message you send next will be copied to all users.",
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return
    
    if data == "verify_subscription":
        not_joined_channels = await check_force_subscription(user_id, context)
        
        if not_joined_channels:
            channels_text = "\n".join([f"• {title}" for _, title in not_joined_channels])
            await query.edit_message_text(
                f"❌ <b>ʏᴏᴜ ʜᴀᴠᴇɴ'ᴛ ᴊᴏɪɴᴇᴅ ᴀʟʟ ʀᴇǫᴜɪʀᴇᴅ ᴄʜᴀɴɴᴇʟs</b>\n\n"
                f"<b>sᴛɪʟʟ ᴍɪssɪɴɢ:</b>\n{channels_text}\n\n"
                f"ᴘʟᴇᴀsᴇ ᴊᴏɪɴ ᴀʟʟ ᴄʜᴀɴɴᴇʟs ᴀɴᴅ ᴛʀʏ ᴀɢᴀɪɴ.",
                parse_mode='HTML'
            )
            return
        
        if is_admin(user_id):
            try:
                await query.delete_message()
            except Exception:
                pass
            await send_admin_menu(query.message.chat_id, context)
        else:
            keyboard = [
                [InlineKeyboardButton("ᴀɴɪᴍᴇ ᴄʜᴀɴɴᴇʟ", url=PUBLIC_ANIME_CHANNEL_URL)], 
                [InlineKeyboardButton("ᴄᴏɴᴛᴀᴄᴛ ᴀᴅᴍɪɴ", url=f"https://t.me/{ADMIN_CONTACT_USERNAME}")],
                [InlineKeyboardButton("ʀᴇǫᴜᴇsᴛ ᴀɴɪᴍᴇ ᴄʜᴀɴɴᴇʟ", url=REQUEST_CHANNEL_URL)],
                [
                    InlineKeyboardButton("ᴀʙᴏᴜᴛ ᴍᴇ", callback_data="about_bot"),
                    InlineKeyboardButton("ᴄʟᴏsᴇ", callback_data="close_message")
                ]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            try:
                await query.delete_message()
            except Exception:
                pass
            
            try:
                await context.bot.copy_message(
                    chat_id=query.message.chat_id,
                    from_chat_id=WELCOME_SOURCE_CHANNEL,
                    message_id=WELCOME_SOURCE_MESSAGE_ID,
                    reply_markup=reply_markup
                )
            except Exception as e:
                logger.error(f"ᴇʀʀᴏʀ ᴄᴏᴘʏɪɴɢ ᴠᴇʀɪғɪᴇᴅ ᴡᴇʟᴄᴏᴍᴇ ᴍᴇssᴀɢᴇ: {e}")
                fallback_text = "✅ <b>sᴜʙsᴄʀɪᴘᴛɪᴏɴ ᴠᴇʀɪғɪᴇᴅ!</b>\n\nWelcome to the bot!"
                await context.bot.send_message(query.message.chat_id, fallback_text, parse_mode='HTML', reply_markup=reply_markup)
        
    elif data.startswith("verify_deep_"):
        link_id = data[12:]
        not_joined_channels = await check_force_subscription(user_id, context)
        
        if not_joined_channels:
            channels_text = "\n".join([f"• {title}" for _, title in not_joined_channels])
            await query.edit_message_text(
                f"❌ <b>ʏᴏᴜ ʜᴀᴠᴇɴ'ᴛ ᴊᴏɪɴᴇᴅ ᴀʟʟ ʀᴇǫᴜɪʀᴇᴅ ᴄʜᴀɴɴᴇʟs</b>\n\n"
                f"<b>sᴛɪʟʟ ᴍɪssɪɴɢ:</b>\n{channels_text}\n\n"
                f"ᴘʟᴇᴀsᴇ ᴊᴏɪɴ ᴀʟʟ ᴄʜᴀɴɴᴇʟs ᴀɴᴅ ᴛʀʏ ᴀɢᴀɪɴ.",
                parse_mode='HTML'
            )
            return
        
        link_info = get_link_info(link_id)
        if not link_info:
            await query.edit_message_text("❌ ʟɪɴᴋ ᴇxᴘɪʀᴇᴅ ᴏʀ ɪɴᴠᴀʟɪᴅ.", parse_mode='HTML')
            return
        
        channel_identifier, _, _, _, is_permanent, _ = link_info
        
        try:
            if channel_identifier.lstrip('-').isdigit():
                channel_identifier = int(channel_identifier)
            
            chat = await context.bot.get_chat(channel_identifier)
            
            # Generate temporary link from permanent link
            temp_link_id = generate_link_id(channel_identifier, user_id, permanent=False, parent_link_id=link_id)
            
            invite_link = await context.bot.create_chat_invite_link(
                chat.id, 
                member_limit=1,
                expire_date=int(datetime.now().timestamp()) + 300  # 5 minutes
            )
            
            mark_link_used(temp_link_id)
            
            success_message = (
                f"<b>ᴄʜᴀɴɴᴇʟ:</b> {chat.title}\n"
                f"<b>ᴇxᴘɪʀᴇs ɪɴ:</b> 5 minutes\n"
                f"<b>Usage:</b> One-time join\n\n"
                f"<i>ʜᴇʀᴇ ɪs ʏᴏᴜʀ ᴊᴏɪɴ ʟɪɴᴋ! ᴄʟɪᴄᴋ ʙᴇʟᴏᴡ ᴛᴏ ᴊᴏɪɴ:</i>"
            )
            
            keyboard = [[InlineKeyboardButton("🔗 Join Channel", url=invite_link.invite_link)]]

            try:
                await query.delete_message()
            except Exception:
                pass
                
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=success_message,
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            
        except Exception as e:
            logger.error(f"Error generating deep verify link: {e}")
            await query.edit_message_text("❌ ᴇʀʀᴏʀ ᴀᴄᴄᴇssɪɴɢ ᴄʜᴀɴɴᴇʟ ʟɪɴᴋ. ᴘʟᴇᴀsᴇ ᴄᴏɴᴛᴀᴄᴛ ᴛʜᴇ ᴀᴅᴍɪɴ ɪғ ᴛʜɪs ɪssᴜᴇ ᴘᴇʀsɪsᴛs.", parse_mode='HTML')
    
    elif data == "admin_stats":
        if not is_admin(user_id):
            await query.edit_message_text("❌ ᴀᴅᴍɪɴ ᴏɴʟʏ.", parse_mode='HTML')
            return
        await send_admin_stats(query, context)
        return
    
    elif data == "user_management":
        if not is_admin(user_id):
            await query.edit_message_text("❌ ᴀᴅᴍɪɴ ᴏɴʟʏ.", parse_mode='HTML')
            return
        await send_user_management(query, context, offset=0)
        return
    
    elif data.startswith("user_page_"):
        if not is_admin(user_id):
            await query.edit_message_text("❌ ᴀᴅᴍɪɴ ᴏɴʟʏ.", parse_mode='HTML')
            return
        try:
            offset = int(data[10:])
        except ValueError:
            offset = 0
        await send_user_management(query, context, offset=offset)
        return
    
    elif data == "manage_force_sub":
        if not is_admin(user_id):
            await query.edit_message_text("❌ ᴀᴅᴍɪɴ ᴏɴʟʏ.", parse_mode='HTML')
            return
        await show_force_sub_management(query, context)
    
    elif data == "generate_links":
        if not is_admin(user_id):
            await query.edit_message_text("❌ ᴀᴅᴍɪɴ ᴏɴʟʏ.", parse_mode='HTML')
            return
        
        user_states[user_id] = GENERATE_LINK_CHANNEL_USERNAME
        keyboard = [[InlineKeyboardButton("🔙 CANCEL", callback_data="admin_back")]]
        
        try:
            await query.delete_message()
        except Exception:
            pass
            
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text="🔗 <b>GENERATE CHANNEL LINKS</b>\n\nPlease send:\n• Channel username (e.g., <code>@YourChannel</code>) OR\n• Private channel ID (e.g., <code>-1001234567890</code>)\n\nTo get private channel ID, forward any message from that channel to @userinfobot",
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    elif data == "add_channel_start":
        if not is_admin(user_id):
            await query.edit_message_text("❌ ᴀᴅᴍɪɴ ᴏɴʟʏ.", parse_mode='HTML')
            return
        
        user_states[user_id] = ADD_CHANNEL_USERNAME
        
        try:
            await query.delete_message()
        except Exception:
            pass
            
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text="📢 <b>ADD FORCE SUBSCRIPTION CHANNEL</b>\n\nPlease send me the channel username (starting with @):\n\nExample: <code>@Beat_Anime_Ocean</code>",
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 CANCEL", callback_data="manage_force_sub")]])
        )
    
    elif data.startswith("channel_"):
        if not is_admin(user_id):
            await query.edit_message_text("❌ ᴀᴅᴍɪɴ ᴏɴʟʏ.", parse_mode='HTML')
            return
        await show_channel_details(query, context, data[8:])
    
    elif data.startswith("genlink_"):
        if not is_admin(user_id):
            await query.edit_message_text("❌ ᴀᴅᴍɪɴ ᴏɴʟʏ.", parse_mode='HTML')
            return
        
        channel_username_clean = data[8:]
        channel_username = '@' + channel_username_clean
        
        # Generate permanent link
        link_id = generate_link_id(channel_username, user_id, permanent=True)
        bot_username = context.bot.username
        
        deep_link = f"https://t.me/{bot_username}?start={link_id}"
        
        await query.edit_message_text(
            f"🔗 <b>PERMANENT LINK GENERATED</b> 🔗\n\n"
            f"<b>Channel:</b> {channel_username}\n"
            f"<b>Type:</b> Permanent (Generates 5-min links)\n"
            f"<b>Usage:</b> Generates one-time join links\n\n"
            f"<b>Share this permanent link:</b>\n<code>{deep_link}</code>\n\n"
            f"<i>When users click this link, they'll get a 5-minute one-time join link.</i>",
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 BACK", callback_data=f"channel_{channel_username_clean}")]])
        )
    
    elif data.startswith("delete_"):
        if not is_admin(user_id):
            await query.edit_message_text("❌ ᴀᴅᴍɪɴ ᴏɴʟʏ.", parse_mode='HTML')
            return
        channel_username_clean = data[7:]
        channel_username = '@' + channel_username_clean
        
        channel_info = get_force_sub_channel_info(channel_username)
        
        if channel_info:
            keyboard = [
                [InlineKeyboardButton("✅ YES, DELETE", callback_data=f"confirm_delete_{channel_username_clean}")],
                [InlineKeyboardButton("❌ NO, CANCEL", callback_data=f"channel_{channel_username_clean}")]
            ]
            
            await query.edit_message_text(
                f"🗑️ <b>CONFIRM DELETION</b>\n\n"
                f"Are you sure you want to delete this force sub channel?\n\n"
                f"<b>Channel:</b> {channel_info[1]}\n"
                f"<b>Username:</b> <code>{channel_info[0]}</code>\n\n"
                f"<i>This action cannot be undone!</i>",
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
    
    elif data.startswith("confirm_delete_"):
        if not is_admin(user_id):
            await query.edit_message_text("❌ ᴀᴅᴍɪɴ ᴏɴʟʏ.", parse_mode='HTML')
            return
        channel_username_clean = data[15:]
        channel_username = '@' + channel_username_clean
        
        delete_force_sub_channel(channel_username)
        
        await query.edit_message_text(
            f"✅ <b>CHANNEL DELETED</b>\n\n"
            f"Force sub channel <code>{channel_username}</code> has been deleted successfully.",
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📢 MANAGE CHANNELS", callback_data="manage_force_sub")]])
        )
    
    elif data in ["admin_back", "user_back", "channels_back"]:
        if is_admin(user_id):
            await send_admin_menu(query.message.chat_id, context, query)
        else:
            keyboard = [
                [InlineKeyboardButton("ᴀɴɪᴍᴇ ᴄʜᴀɴɴᴇʟ", url=PUBLIC_ANIME_CHANNEL_URL)], 
                [InlineKeyboardButton("ᴄᴏɴᴛᴀᴄᴛ ᴀᴅᴍɪɴ", url=f"https://t.me/{ADMIN_CONTACT_USERNAME}")],
                [InlineKeyboardButton("ʀᴇǫᴜᴇsᴛ ᴀɴɪᴍᴇ ᴄʜᴀɴɴᴇʟ", url=REQUEST_CHANNEL_URL)],
                [
                    InlineKeyboardButton("ᴀʙᴏᴜᴛ ᴍᴇ", callback_data="about_bot"),
                    InlineKeyboardButton("ᴄʟᴏsᴇ", callback_data="close_message")
                ]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            try:
                await query.delete_message()
            except Exception:
                pass
            
            try:
                await context.bot.copy_message(
                    chat_id=query.message.chat_id,
                    from_chat_id=WELCOME_SOURCE_CHANNEL,
                    message_id=WELCOME_SOURCE_MESSAGE_ID,
                    reply_markup=reply_markup
                )
            except Exception as e:
                logger.error(f"Error copying 'user_back' message: {e}")
                fallback_text = "🏠 <b>MAIN MENU</b>\n\nChoose an option:"
                await context.bot.send_message(query.message.chat_id, fallback_text, parse_mode='HTML', reply_markup=reply_markup)

    elif data == "about_bot":
        about_me_text = """
<b>About Us</b>

⇨ <b>Made for: @Beat_Anime_Ocean </b>
⇨ <b>Owned by: @Beat_Anime_Ocean </b> 
⇨ <b>Developer: @Beat_Anime_Ocean </b>

<i>Adios !!</i>
"""
        keyboard = [[InlineKeyboardButton("🔙 BACK", callback_data="user_back")]]
        
        try:
            await query.delete_message()
        except Exception:
            pass

        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text=about_me_text,
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

async def handle_admin_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    if user_id not in user_states:
        return 

    state = user_states[user_id]
    
    if state == PENDING_BROADCAST:
        if user_id in user_states:
            del user_states[user_id]
            await broadcast_message_to_all_users(update, context, update.message)
            await send_admin_menu(update.effective_chat.id, context)
            return
            
    text = update.message.text
    if text is None:
        await update.message.reply_text("❌ Please send a text message as requested.", parse_mode='HTML')
        return

    if state == ADD_CHANNEL_USERNAME:
        if not text.startswith('@'):
            await update.message.reply_text("❌ Please provide a valid channel username starting with @. Try again:", parse_mode='HTML')
            return
        
        context.user_data['channel_username'] = text
        user_states[user_id] = ADD_CHANNEL_TITLE
        
        await update.message.reply_text(
            "📝 <b>STEP 2: Channel Title</b>\n\nNow please send me the display title for this channel:\n\nExample: <i>Anime Ocean Channel</i>",
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 CANCEL", callback_data="manage_force_sub")]])
        )
    
    elif state == ADD_CHANNEL_TITLE:
        channel_username = context.user_data.get('channel_username')
        channel_title = text
        
        if add_force_sub_channel(channel_username, channel_title):
            if user_id in user_states:
                del user_states[user_id]
            if 'channel_username' in context.user_data:
                del context.user_data['channel_username']
            
            await update.message.reply_text(
                f"✅ <b>FORCE SUB CHANNEL ADDED SUCCESSFULLY!</b>\n\n"
                f"<b>Username:</b> <code>{channel_username}</code>\n"
                f"<b>Title:</b> {channel_title}\n\n"
                f"Channel has been added to force subscription list!",
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📢 MANAGE CHANNELS", callback_data="manage_force_sub")]])
            )
        else:
            await update.message.reply_text("❌ Error adding channel. It might already exist.", parse_mode='HTML')
            
    elif state == GENERATE_LINK_CHANNEL_USERNAME:
        channel_identifier = text.strip()
        
        if not (channel_identifier.startswith('@') or channel_identifier.startswith('-100') or channel_identifier.lstrip('-').isdigit()):
            await update.message.reply_text(
                "❌ Invalid format. Please send either:\n"
                "• Channel username: <code>@YourChannel</code>\n"
                "• Private channel ID: <code>-1001234567890</code>\n\n"
                "Try again:",
                parse_mode='HTML'
            )
            return
            
        if user_id in user_states:
            del user_states[user_id]
        
        try:
            chat = await context.bot.get_chat(channel_identifier)
            channel_title = chat.title
        except Exception as e:
            logger.error(f"Error accessing channel {channel_identifier}: {e}")
            await update.message.reply_text(
                "❌ <b>Cannot access this channel!</b>\n\n"
                "Please ensure:\n"
                "1. The bot is added to the channel as an admin\n"
                "2. The bot has permission to create invite links\n"
                "3. The channel ID/username is correct",
                parse_mode='HTML'
            )
            return
        
        link_id = generate_link_id(str(channel_identifier), user_id)
        bot_username = context.bot.username
        
        deep_link = f"https://t.me/{bot_username}?start={link_id}"
        
        await update.message.reply_text(
            f"🔗 <b>LINK GENERATED</b> 🔗\n\n"
            f"<b>Channel:</b> {channel_title}\n"
            f"<b>ID/Username:</b> <code>{channel_identifier}</code>\n"
            f"<b>Expires in:</b> {LINK_EXPIRY_MINUTES} minutes\n\n"
            f"<b>Direct Link:</b>\n<code>{deep_link}</code>\n\n"
            "Share this link with users!",
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 BACK TO MENU", callback_data="admin_back")]])
        )

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Exception while handling an update: {context.error}")

async def cleanup_task(context: ContextTypes.DEFAULT_TYPE):
    cleanup_expired_links()

def main():
    # Initialize database with retry logic
    max_retries = 5
    for attempt in range(max_retries):
        try:
            init_db()
            logger.info("✅ Database initialized successfully")
            break
        except Exception as e:
            logger.error(f"❌ Database initialization attempt {attempt + 1} failed: {e}")
            if attempt == max_retries - 1:
                logger.error("💥 All database initialization attempts failed")
                return
            time.sleep(5)

    if not BOT_TOKEN:
        logger.error("❌ BOT_TOKEN is missing")
        return

    application = Application.builder().token(BOT_TOKEN).build()
    
    # Add handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(button_handler))
    
    admin_filter = filters.User(user_id=ADMIN_ID)
    application.add_handler(MessageHandler(admin_filter & ~filters.COMMAND, handle_admin_message))
    
    application.add_error_handler(error_handler)
    
    # Job queues
    job_queue = application.job_queue
    if job_queue:
        job_queue.run_repeating(
            lambda context: asyncio.create_task(cleanup_task(context)), 
            interval=600, 
            first=10
        )
    else:
        logger.warning("JobQueue not available")

    # Webhook or polling
    if WEBHOOK_URL and BOT_TOKEN:
        keep_alive_thread = Thread(target=keep_alive, daemon=True)
        keep_alive_thread.start()
        logger.info("✅ Keep-alive service started")
        
        application.run_webhook(
            listen="0.0.0.0",
            port=PORT,
            url_path=BOT_TOKEN,
            webhook_url=WEBHOOK_URL + BOT_TOKEN
        )
    else:
        logger.info("🤖 Starting in Polling Mode...")
        application.run_polling()

if __name__ == '__main__':
    main()
