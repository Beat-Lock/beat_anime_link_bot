import os
import logging
import pg8000.dbapi 
import ssl          
import certifi      
import secrets
import urllib.parse 
import time
import asyncio
import sys 
import json 
from datetime import datetime, timedelta
from functools import wraps
from threading import Thread
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, MessageHandler, filters

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Bot configuration
BOT_TOKEN = os.getenv("BOT_TOKEN", "YOUR_TOKEN_HERE")
ADMIN_ID = 829342319  # <--- REPLACE with your actual Admin ID
LINK_EXPIRY_MINUTES = 5
DATABASE_URL = os.environ.get('DATABASE_URL') 

# --- NEW BROADCAST THROTTLING CONSTANTS ---
BROADCAST_CHUNK_SIZE = 1000 
BROADCAST_MIN_USERS = 5000   
BROADCAST_INTERVAL_MIN = 20  
# ------------------------------------------

# Webhook / polling config
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

# ========== DATABASE FUNCTIONS (PostgreSQL with Connection Pool) ==========

# Global variable for the connection pool
conn_pool = None 

def create_connection_factory():
    """Returns a function that creates a single pg8000 connection."""
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL environment variable is not set.")
    
    urllib.parse.uses_netloc.append('postgres') 
    urllib.parse.uses_netloc.append('postgresql') 
    url_parts = urllib.parse.urlparse(DATABASE_URL)
    
    # Prepare SSL context for Render
    ssl_context = ssl.create_default_context(cafile=certifi.where())
    ssl_context.verify_mode = ssl.CERT_REQUIRED
    
    def connect_function():
        return pg8000.dbapi.connect(
            database=url_parts.path[1:],
            user=url_parts.username,
            password=url_parts.password,
            host=url_parts.hostname,
            port=url_parts.port or 5432, 
            ssl_context=ssl_context 
        )
    return connect_function

def init_conn_pool():
    """Initializes the global database connection pool."""
    global conn_pool
    if conn_pool is None:
        try:
            connect_factory = create_connection_factory()
            # Max size set to 5, which is typical for small shared databases
            conn_pool = pg8000.dbapi.ThreadSafePool(connect_factory, min_size=1, max_size=5)
            logger.info("Database connection pool initialized successfully.")
        except Exception as e:
            logger.error(f"FATAL: Failed to initialize connection pool: {e}")
            raise 

def get_conn():
    """Acquires a connection from the global pool."""
    if conn_pool is None:
        raise ConnectionError("Database connection pool not initialized.")
    return conn_pool.get_connection()

def db_operation(sql, params=None, fetch=False, fetch_one=False):
    """Helper function to execute database queries using the connection pool."""
    conn = None
    try:
        conn = get_conn()
        cursor = conn.cursor()
        
        cursor.execute(sql, params if params else []) 
        
        if fetch_one:
            result = cursor.fetchone()
            return result
        if fetch:
            result = cursor.fetchall()
            return result
        conn.commit()
    except Exception as e:
        logger.error(f"DB Error on '{sql[:50]}...': {e}")
        if conn:
            conn.rollback()
        raise 
    finally:
        if conn:
            conn_pool.put_connection(conn) 

def init_db():
    """Initializes PostgreSQL tables (Drives persistence)."""
    init_conn_pool() # Initialize the pool before running init commands
    
    sql_users = '''
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            joined_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            is_banned BOOLEAN DEFAULT FALSE
        )
    '''
    sql_channels = '''
        CREATE TABLE IF NOT EXISTS force_sub_channels (
            channel_id SERIAL PRIMARY KEY,
            channel_username TEXT UNIQUE,
            channel_title TEXT,
            added_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            is_active BOOLEAN DEFAULT TRUE
        )
    '''
    sql_links = '''
        CREATE TABLE IF NOT EXISTS generated_links (
            link_id TEXT PRIMARY KEY,
            channel_username TEXT,
            user_id BIGINT,
            created_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        )
    '''
    
    conn = None
    try:
        conn = get_conn()
        cursor = conn.cursor()
        cursor.execute(sql_users)
        cursor.execute(sql_channels)
        cursor.execute(sql_links)
        conn.commit()
    except Exception as e:
        logger.error(f"DB Initialization Error: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn_pool.put_connection(conn)


def get_user_id_by_username(username):
    clean_username = username.lstrip('@')
    sql = 'SELECT user_id FROM users WHERE username ILIKE %s'
    result = db_operation(sql, (clean_username,), fetch_one=True)
    return result[0] if result else None
    
def resolve_target_user_id(arg):
    try:
        return int(arg)
    except ValueError:
        pass

    if arg:
        return get_user_id_by_username(arg)
    
    return None

def ban_user(user_id):
    sql = 'UPDATE users SET is_banned = TRUE WHERE user_id = %s'
    db_operation(sql, (user_id,))

def unban_user(user_id):
    sql = 'UPDATE users SET is_banned = FALSE WHERE user_id = %s'
    db_operation(sql, (user_id,))

def is_user_banned(user_id):
    sql = 'SELECT is_banned FROM users WHERE user_id = %s'
    result = db_operation(sql, (user_id,), fetch_one=True)
    return result[0] if result and result[0] is not None else False

def add_user(user_id, username, first_name, last_name):
    clean_username = username.lstrip('@') if username else None
    sql = '''
        INSERT INTO users (user_id, username, first_name, last_name)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (user_id) DO UPDATE SET 
            username = EXCLUDED.username, 
            first_name = EXCLUDED.first_name, 
            last_name = EXCLUDED.last_name
    '''
    db_operation(sql, (user_id, clean_username, first_name, last_name))

def get_user_count():
    sql = 'SELECT COUNT(*) FROM users'
    result = db_operation(sql, fetch_one=True)
    return result[0] if result else 0

def get_all_users(limit=None, offset=0):
    sql = 'SELECT user_id, username, first_name, last_name, joined_date, is_banned FROM users ORDER BY joined_date DESC'
    params = []
    
    if limit is not None:
        sql += ' LIMIT %s OFFSET %s'
        params = [limit, offset]

    return db_operation(sql, params, fetch=True)

def get_user_info_by_id(user_id):
    sql = 'SELECT user_id, username, first_name, last_name, joined_date, is_banned FROM users WHERE user_id = %s'
    return db_operation(sql, (user_id,), fetch_one=True)

def add_force_sub_channel(channel_username, channel_title):
    try:
        sql = '''
            INSERT INTO force_sub_channels (channel_username, channel_title, is_active)
            VALUES (%s, %s, TRUE)
            ON CONFLICT (channel_username) DO UPDATE SET 
                channel_title = EXCLUDED.channel_title,
                is_active = TRUE
        '''
        db_operation(sql, (channel_username, channel_title))
        return True
    except Exception:
        return False

def get_all_force_sub_channels(return_usernames_only=False):
    select_col = 'channel_username' if return_usernames_only else 'channel_username, channel_title'
    sql = f'SELECT {select_col} FROM force_sub_channels WHERE is_active = TRUE ORDER BY channel_title'
    channels = db_operation(sql, fetch=True)
    
    if return_usernames_only:
        return [row[0] for row in channels]
    return channels

def get_force_sub_channel_info(channel_username):
    sql = 'SELECT channel_username, channel_title FROM force_sub_channels WHERE channel_username = %s AND is_active = TRUE'
    return db_operation(sql, (channel_username,), fetch_one=True)

def delete_force_sub_channel(channel_username):
    sql = 'UPDATE force_sub_channels SET is_active = FALSE WHERE channel_username = %s'
    db_operation(sql, (channel_username,))

def generate_link_id(channel_username, user_id):
    link_id = secrets.token_urlsafe(16)
    sql = '''
        INSERT INTO generated_links (link_id, channel_username, user_id)
        VALUES (%s, %s, %s)
        ON CONFLICT (link_id) DO UPDATE SET created_time = EXCLUDED.created_time
    '''
    db_operation(sql, (link_id, channel_username, user_id))
    return link_id

def get_link_info(link_id):
    sql = 'SELECT channel_username, user_id, created_time FROM generated_links WHERE link_id = %s'
    return db_operation(sql, (link_id,), fetch_one=True)

def cleanup_old_links():
    """Removes links older than LINK_EXPIRY_MINUTES."""
    cutoff_time = datetime.now() - timedelta(minutes=LINK_EXPIRY_MINUTES)
    sql = 'DELETE FROM generated_links WHERE created_time < %s'
    try:
        db_operation(sql, (cutoff_time,))
        logger.info("Database cleanup: Old links removed.")
    except Exception as e:
        logger.error(f"Error during link cleanup: {e}")

# ========== FORCE SUBSCRIPTION LOGIC (with Ban Check) ==========

async def is_user_subscribed(user_id: int, bot) -> bool:
    """Check if user is member of all force-sub channels."""
    force_sub_channels = get_all_force_sub_channels(return_usernames_only=True)
    if not force_sub_channels:
        return True

    for ch in force_sub_channels:
        try:
            member = await bot.get_chat_member(chat_id=ch, user_id=user_id)
            if member.status in ['left', 'kicked']:
                return False
        except Exception as e:
            logger.error(f"Error checking membership in {ch} for user {user_id}: {e}")
            return False 
    return True

def force_sub_required(func):
    """Decorator for handlers to enforce force-subscribe, but bypass for admin."""
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user = update.effective_user
        if user is None:
            return await func(update, context, *args, **kwargs)
        
        force_sub_channels_info = get_all_force_sub_channels(return_usernames_only=False)
        
        if user.id == ADMIN_ID:
            return await func(update, context, *args, **kwargs)

        # --- BAN CHECK ---
        if is_user_banned(user.id):
            await delete_update_message(update, context)
            ban_text = "🚫 You have been banned from using this bot. Contact the administrator for details."
            if update.message:
                await update.message.reply_text(ban_text)
            elif update.callback_query:
                try:
                    await update.callback_query.edit_message_text(ban_text)
                except:
                    await context.bot.send_message(update.effective_chat.id, ban_text)
            return
        # --- END BAN CHECK ---

        if not force_sub_channels_info:
            return await func(update, context, *args, **kwargs)

        subscribed = await is_user_subscribed(user.id, context.bot)
        
        if not subscribed:
            await delete_update_message(update, context)
            
            keyboard = []
            
            for uname, title in force_sub_channels_info:
                keyboard.append([InlineKeyboardButton(f"{title}", url=f"https://t.me/{uname.lstrip('@')}")])
                
            keyboard.append([InlineKeyboardButton("Click to continue", callback_data="verify_subscription")])
            reply_markup = InlineKeyboardMarkup(keyboard)

            text = (
                "<b>Please join our world of anime:</b>\n\n"
                "After joining, click <b>Verify Subscription</b>."
            )

            if update.message:
                await update.message.reply_text(text, parse_mode='HTML', reply_markup=reply_markup)
            elif update.callback_query:
                await update.callback_query.edit_message_text(text, parse_mode='HTML', reply_markup=reply_markup)
            return
        
        return await func(update, context, *args, **kwargs)

    return wrapper

# ========== ADMIN COMMAND HANDLERS ==========

async def reload_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to restart the bot process gracefully."""
    if update.effective_user.id != ADMIN_ID:
        return

    await delete_update_message(update, context)
    user_states.pop(update.effective_user.id, None)
    await delete_bot_prompt(context, update.effective_chat.id)
    
    message_id_to_copy = None
    if context.args:
        try:
            if context.args[0].lower() == 'admin':
                message_id_to_copy = 'admin'
            else:
                message_id_to_copy = int(context.args[0])
        except ValueError:
            await update.message.reply_text(
                "❌ **Usage:** `/reload [optional message ID or 'admin']`\n**Example:** `/reload 1234` or `/reload admin`",
                parse_mode='Markdown'
            )
            return

    restart_info = {
        'chat_id': update.effective_chat.id,
        'admin_id': ADMIN_ID,
        'message_id_to_copy': message_id_to_copy 
    }
    try:
        with open('restart_message.json', 'w') as f:
            json.dump(restart_info, f)
    except Exception as e:
        logger.error(f"Failed to write restart file: {e}")

    await update.message.reply_text("🔄 **Bot is restarting...** Please wait.", parse_mode='Markdown')
    
    logger.info("Bot restart initiated by admin. Stopping application.")
    
    sys.exit(0) 

async def ban_user_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    await delete_update_message(update, context)
    user_states.pop(update.effective_user.id, None)
    await delete_bot_prompt(context, update.effective_chat.id)
    args = context.args
    if len(args) != 1:
        await update.message.reply_text("❌ **Usage:** `/banuser @username or ID`", parse_mode='Markdown'); return
    target_arg = args[0]
    target_user_id = resolve_target_user_id(target_arg)
    if target_user_id is None:
        await update.message.reply_text(f"❌ User **{target_arg}** not found in database.", parse_mode='Markdown'); return
    if target_user_id == ADMIN_ID:
        await update.message.reply_text("⚠️ Cannot ban the **Admin**.", parse_mode='Markdown'); return
    ban_user(target_user_id)
    await update.message.reply_text(f"🚫 User with ID **{target_user_id}** (Target: {target_arg}) has been **banned**.", parse_mode='Markdown')

async def unban_user_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    await delete_update_message(update, context)
    user_states.pop(update.effective_user.id, None)
    await delete_bot_prompt(context, update.effective_chat.id)
    args = context.args
    if len(args) != 1:
        await update.message.reply_text("❌ **Usage:** `/unbanuser @username or ID`", parse_mode='Markdown'); return
    target_arg = args[0]
    target_user_id = resolve_target_user_id(target_arg)
    if target_user_id is None:
        await update.message.reply_text(f"❌ User **{target_arg}** not found in database.", parse_mode='Markdown'); return
    unban_user(target_user_id)
    await update.message.reply_text(f"✅ User with ID **{target_user_id}** (Target: {target_arg}) has been **unbanned**.", parse_mode='Markdown')

async def add_channel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    await delete_update_message(update, context)
    await delete_bot_prompt(context, update.effective_chat.id)
    args = context.args
    if len(args) < 2:
        await update.message.reply_text("❌ **Usage:** `/addchannel @channelusername Channel Title`", parse_mode='Markdown'); return
    channel_username = args[0]
    channel_title = " ".join(args[1:])
    if not channel_username.startswith('@'):
        await update.message.reply_text("❌ Channel username must start with **@**.", parse_mode='Markdown'); return
    try:
        await context.bot.get_chat(channel_username)
    except Exception as e:
        logger.warning(f"Bot failed to get chat {channel_username}: {e}")
        await update.message.reply_text(f"⚠️ Bot cannot access channel **{channel_username}**. Make sure the bot is an **Admin** in that channel.", parse_mode='Markdown'); return
    if add_force_sub_channel(channel_username, channel_title):
        await update.message.reply_text(f"✅ Successfully added/updated channel:\n**Title:** {channel_title}\n**Username:** `{channel_username}`", parse_mode='Markdown')
    else:
        await update.message.reply_text("❌ Failed to add channel. Check logs for database error.", parse_mode='Markdown')

async def remove_channel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    await delete_update_message(update, context)
    await delete_bot_prompt(context, update.effective_chat.id)
    args = context.args
    if len(args) != 1:
        await update.message.reply_text("❌ **Usage:** `/removechannel @channelusername`", parse_mode='Markdown'); return
    channel_username = args[0]
    if not channel_username.startswith('@'):
        await update.message.reply_text("❌ Channel username must start with **@**.", parse_mode='Markdown'); return
    channel_info = get_force_sub_channel_info(channel_username)
    if not channel_info:
        await update.message.reply_text(f"⚠️ Channel **{channel_username}** is not active or does not exist in the list.", parse_mode='Markdown'); return
    delete_force_sub_channel(channel_username)
    await update.message.reply_text(f"🗑️ Successfully removed/deactivated channel **{channel_username}**.", parse_mode='Markdown')

@force_sub_required
async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    await delete_update_message(update, context)
    user_states.pop(update.effective_user.id, None)
    await delete_bot_prompt(context, update.effective_chat.id)
    user_count = get_user_count()
    channel_count = len(get_all_force_sub_channels()) 
    stats_text = (
        "📊 <b>BOT STATISTICS</b>\n\n"
        f"👤 Total Users: {user_count}\n"
        f"📢 Force Sub Channels: {channel_count}\n"
        f"🔗 Link Expiry: {LINK_EXPIRY_MINUTES} minutes"
    )
    keyboard = [[InlineKeyboardButton("🔙 BACK TO MENU", callback_data="admin_back")]]
    await update.message.reply_text(text=stats_text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))

# ========== BOT HANDLERS ==========

async def delete_update_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id == ADMIN_ID and user_states.get(user_id) == PENDING_BROADCAST:
        return 
    if update.message:
        try:
            await update.message.delete()
        except Exception as e:
            logger.warning(f"Could not delete message for user {update.effective_user.id}: {e}")

async def delete_bot_prompt(context: ContextTypes.DEFAULT_TYPE, chat_id):
    prompt_id = context.user_data.pop('bot_prompt_message_id', None)
    if prompt_id:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=prompt_id)
        except Exception as e:
            logger.warning(f"Could not delete bot prompt message {prompt_id}: {e}")
    return prompt_id

@force_sub_required
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if update.message:
        await delete_update_message(update, context)

    if update.callback_query and update.callback_query.message:
        try:
            await update.callback_query.message.delete()
        except Exception as e:
            logger.warning(f"Could not delete subscription prompt message: {e}")
    
    add_user(user.id, user.username, user.first_name, user.last_name)

    if context.args and len(context.args) > 0:
        link_id = context.args[0]
        await handle_channel_link_deep(update, context, link_id)
        return

    if user.id == ADMIN_ID:
        await delete_bot_prompt(context, update.effective_chat.id)
        user_states.pop(user.id, None)
        await send_admin_menu(update.effective_chat.id, context)
    else:
        keyboard = [
            [InlineKeyboardButton("ᴀɴɪᴍᴇ ᴄʜᴀɴɴᴇʟ", url=PUBLIC_ANIME_CHANNEL_URL)],
            [InlineKeyboardButton("ᴄᴏɴᴛᴀᴄT ᴀᴅᴍɪɴ", url=f"https://t.me/{ADMIN_CONTACT_USERNAME}")],
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
            logger.error(f"Error copying welcome message: {e}")
            fallback_text = "👋 <b>Welcome to the bot!</b>"
            await context.bot.send_message(update.effective_chat.id, fallback_text, parse_mode='HTML', reply_markup=reply_markup)


@force_sub_required
async def handle_admin_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id != ADMIN_ID or user_id not in user_states: return

    state = user_states[user_id]
    text = update.message.text
    
    await delete_bot_prompt(context, update.effective_chat.id)
    
    if state == PENDING_BROADCAST:
        user_states.pop(user_id, None)
        await broadcast_message_to_all_users(update, context, update.message)
        await send_admin_menu(update.effective_chat.id, context)
        return

    if text is None:
        await delete_update_message(update, context)
        msg = await update.message.reply_text("❌ Please send a text message.", parse_mode='HTML')
        context.user_data['bot_prompt_message_id'] = msg.message_id
        return

    if state == ADD_CHANNEL_USERNAME:
        await delete_update_message(update, context)
        if not text.startswith('@'):
            msg = await update.message.reply_text("❌ Please include @ in channel username.", parse_mode='HTML')
            context.user_data['bot_prompt_message_id'] = msg.message_id
            return
        
        context.user_data['channel_username'] = text
        user_states[user_id] = ADD_CHANNEL_TITLE
        msg = await update.message.reply_text(
            "📝 Send channel title now.", 
            parse_mode='HTML', 
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 CANCEL", callback_data="manage_force_sub")]])
        )
        context.user_data['bot_prompt_message_id'] = msg.message_id
        
    elif state == ADD_CHANNEL_TITLE:
        await delete_update_message(update, context)
        channel_username = context.user_data.pop('channel_username', None)
        channel_title = text
        user_states.pop(user_id, None)

        if add_force_sub_channel(channel_username, channel_title):
            await update.message.reply_text(
                f"✅ Channel added: {channel_title} ({channel_username})", 
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📢 MANAGE CHANNELS", callback_data="manage_force_sub")]])
            )
        else:
            await update.message.reply_text("❌ Failed to add channel. Check logs for database error.", parse_mode='HTML')

    elif state == GENERATE_LINK_CHANNEL_USERNAME:
        await delete_update_message(update, context)
        channel_identifier = text.strip()

        if not (channel_identifier.startswith('@') or channel_identifier.startswith('-100') or channel_identifier.lstrip('-').isdigit()):
            msg = await update.message.reply_text(
                "❌ Invalid format. Use @username or channel ID (-100...)",
                parse_mode='HTML'
            )
            context.user_data['bot_prompt_message_id'] = msg.message_id
            return

        user_states.pop(user_id, None)

        try:
            chat = await context.bot.get_chat(channel_identifier)
            channel_title = chat.title
        except Exception as e:
            logger.error(f"Error accessing channel {channel_identifier}: {e}")
            await update.message.reply_text(
                "❌ Cannot access channel. Make sure bot is admin in that channel.",
                parse_mode='HTML'
            )
            return

        link_id = generate_link_id(str(channel_identifier), user_id)
        botname = context.bot.username
        deep_link = f"https://t.me/{botname}?start={link_id}"

        await update.message.reply_text(
            f"🔗 Link generated:\nChannel: {channel_title}\n<code>{deep_link}</code>",
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 BACK TO MENU", callback_data="admin_back")]])
        )


@force_sub_required
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    data = query.data

    if data == "verify_subscription": return await start(update, context) 

    if user_id == ADMIN_ID and user_id in user_states:
        current = user_states[user_id]
        if current in [PENDING_BROADCAST, GENERATE_LINK_CHANNEL_USERNAME, ADD_CHANNEL_TITLE, ADD_CHANNEL_USERNAME] and data in ["admin_back", "manage_force_sub", "user_management"]:
            await delete_bot_prompt(context, query.message.chat.id)
            user_states.pop(user_id, None)

    if data == "close_message":
        try: await query.delete_message()
        except: pass; return

    # --- SINGLE USER MANAGEMENT LOGIC ---
    elif data.startswith("manage_user_"):
        if user_id != ADMIN_ID: await query.answer("You are not authorized", show_alert=True); return
        user_states.pop(user_id, None)
        await delete_bot_prompt(context, query.message.chat.id)
        try:
            target_user_id = int(data[12:])
            await send_single_user_management(query, context, target_user_id)
        except ValueError: await query.answer("Invalid User ID.", show_alert=True); return

    elif data.startswith("toggle_ban_"):
        if user_id != ADMIN_ID: await query.answer("You are not authorized", show_alert=True); return
        try:
            parts = data.split('_')
            target_user_id = int(parts[2].lstrip('f'))
            target_status = int(parts[3].lstrip('f')) 

            if target_user_id == ADMIN_ID:
                await query.answer("Cannot ban self!", show_alert=True)
                await send_single_user_management(query, context, target_user_id); return

            if target_status == 1: ban_user(target_user_id); action = "banned"
            else: unban_user(target_user_id); action = "unbanned"

            await send_single_user_management(query, context, target_user_id)
            await query.answer(f"User {target_user_id} successfully {action}.", show_alert=True)

        except Exception as e:
            logger.error(f"Error handling ban/unban: {e}")
            await query.answer("Error processing request.", show_alert=True)
    # --- END BAN/UNBAN LOGIC (Inline Button) ---

    elif data == "admin_broadcast_start":
        if user_id != ADMIN_ID: await query.edit_message_text("❌ Admin only", parse_mode='HTML'); return
        user_states[user_id] = PENDING_BROADCAST
        try: await query.delete_message()
        except: pass
        msg = await context.bot.send_message(chat_id=query.message.chat.id, text="✍️ **Send the message** you want to broadcast now. (Photos, videos, files are supported).", parse_mode='Markdown', reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 CANCEL", callback_data="admin_back")]]))
        context.user_data['bot_prompt_message_id'] = msg.message_id; return

    elif data == "admin_back":
        if user_id == ADMIN_ID: await send_admin_menu(query.message.chat.id, context, update.callback_query.message.message_id)

    elif data == "user_management":
        if user_id == ADMIN_ID: await send_user_management_menu(query.message.chat.id, context, update.callback_query.message.message_id)

    elif data == "manage_force_sub":
        if user_id == ADMIN_ID: await send_manage_channels_menu(query.message.chat.id, context, update.callback_query.message.message_id)

    elif data == "generate_link":
        if user_id != ADMIN_ID: await query.edit_message_text("❌ Admin only", parse_mode='HTML'); return
        user_states[user_id] = GENERATE_LINK_CHANNEL_USERNAME
        msg = await query.edit_message_text("🔗 Send the **channel @username or ID** you want to generate a deep link for:", parse_mode='Markdown', reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 CANCEL", callback_data="admin_back")]]))
        context.user_data['bot_prompt_message_id'] = msg.message_id; return
        
    elif data.startswith("remove_channel_"):
        if user_id != ADMIN_ID: return
        channel_username = data[15:]
        channel_info = get_force_sub_channel_info(channel_username)
        if channel_info:
            delete_force_sub_channel(channel_username)
            await query.answer(f"🗑️ Channel {channel_username} removed.", show_alert=True)
        else:
            await query.answer(f"⚠️ Channel {channel_username} not found or already inactive.", show_alert=True)
        await send_manage_channels_menu(query.message.chat.id, context, update.callback_query.message.message_id)

    elif data == "add_channel_prompt":
        if user_id != ADMIN_ID: return
        user_states[user_id] = ADD_CHANNEL_USERNAME
        msg = await query.edit_message_text("📝 Send **channel @username** now (e.g., `@MyChannel`).", parse_mode='Markdown', reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 CANCEL", callback_data="manage_force_sub")]]))
        context.user_data['bot_prompt_message_id'] = msg.message_id; return
        
    elif data == "about_bot":
        text = ("🤖 <b>Anime Links Bot</b>\n\nThis bot is designed to secure channels by enforcing subscriptions and generating expiring deep links for seamless content sharing.\n\nDeveloper: [Beat Anime Ocean](https://t.me/Beat_Anime_Ocean)")
        keyboard = [[InlineKeyboardButton("🔙 BACK", callback_data="back_to_start")]]
        await query.edit_message_text(text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))
        
    elif data == "back_to_start": return await start(update, context) 

# ========== DEEP LINK HANDLER (Updated Link Messages) ==========

async def handle_channel_link_deep(update: Update, context: ContextTypes.DEFAULT_TYPE, link_id: str):
    """Handles the deep link logic for channel redirects, showing custom error messages."""
    user = update.effective_user
    
    if not await is_user_subscribed(user.id, context.bot):
        return await start(update, context) 

    link_info = get_link_info(link_id)
    
    # 1. LINK NOT FOUND / "NO DATA" FIX
    if link_info is None:
        text = "❌ **Error:** Link not found. The link is invalid."
        keyboard = [[InlineKeyboardButton("🔙 GO BACK", callback_data="back_to_start")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await context.bot.send_message(user.id, text, parse_mode='Markdown', reply_markup=reply_markup)
        return

    channel_identifier, link_user_id, created_time = link_info
    
    expiry_time = created_time + timedelta(minutes=LINK_EXPIRY_MINUTES)
    
    # 2. LINK EXPIRED / "LINK LOSE" FIX
    if datetime.now(created_time.tzinfo) > expiry_time:
        text = f"⏳ **Error:** This link has expired after {LINK_EXPIRY_MINUTES} minutes. Please get a new link."
        keyboard = [[InlineKeyboardButton("🔙 GO BACK", callback_data="back_to_start")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await context.bot.send_message(user.id, text, parse_mode='Markdown', reply_markup=reply_markup)
        return

    # Check if the user is a member of the linked channel
    try:
        member = await context.bot.get_chat_member(chat_id=channel_identifier, user_id=user.id)
        if member.status in ['left', 'kicked']:
            try:
                chat = await context.bot.get_chat(channel_identifier)
                channel_title = chat.title
            except:
                channel_title = channel_identifier
                
            text = f"⚠️ You must be a member of **{channel_title}** to access the content. Please join the channel and try the link again."
            keyboard = [[InlineKeyboardButton(f"Join {channel_title}", url=f"https://t.me/{channel_identifier.lstrip('@')}")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await context.bot.send_message(user.id, text, parse_mode='Markdown', reply_markup=reply_markup)
            return

    except Exception as e:
        logger.error(f"Error checking membership in deep link channel {channel_identifier}: {e}")
        text = "❌ **Error:** Cannot verify channel access. Please contact the administrator."
        await context.bot.send_message(user.id, text, parse_mode='Markdown')
        return

    # Success: Send the final channel link
    try:
        chat = await context.bot.get_chat(channel_identifier)
        channel_title = chat.title
        
        text = f"✅ **Link Verified!**\n\nClick the button to go to **{channel_title}**."
        keyboard = [[InlineKeyboardButton(f"Go to {channel_title}", url=f"https://t.me/{channel_identifier.lstrip('@')}")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await context.bot.send_message(user.id, text, parse_mode='Markdown', reply_markup=reply_markup)
        
    except Exception as e:
        logger.error(f"Error handling deep link final step for {channel_identifier}: {e}")
        fallback_text = f"✅ **Success!** You have access to the channel: `{channel_identifier}`"
        await context.bot.send_message(user.id, fallback_text, parse_mode='Markdown')

# ========== ADMIN MENU GENERATORS ==========

async def send_single_user_management(query, context, target_user_id):
    user_info = get_user_info_by_id(target_user_id)
    if not user_info: await query.answer(f"User {target_user_id} not found in DB.", show_alert=True); return await send_user_management_menu(query.message.chat.id, context, query.message.message_id)
    _, username, first_name, last_name, joined_date, is_banned = user_info
    status_text = "🚫 BANNED" if is_banned else "✅ ACTIVE"
    action_button_text = "✅ UNBAN USER" if is_banned else "🚫 BAN USER"
    action_button_data = f"toggle_ban_{target_user_id}_{0 if is_banned else 1}"
    text = (
        f"👤 <b>USER MANAGEMENT: {target_user_id}</b>\n\n"
        f"**Status:** {status_text}\n"
        f"**Name:** {first_name} {last_name if last_name else ''}\n"
        f"**Username:** @{username}" if username else f"**Username:** N/A\n"
        f"**Joined:** {joined_date.strftime('%Y-%m-%d %H:%M:%S')}"
    )
    keyboard = [[InlineKeyboardButton(action_button_text, callback_data=action_button_data)], [InlineKeyboardButton("🔙 BACK TO USER LIST", callback_data="user_management")]]
    try: await context.bot.edit_message_text(chat_id=query.message.chat.id, message_id=query.message.message_id, text=text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception as e: logger.warning(f"Error sending single user management menu: {e}"); await context.bot.send_message(query.message.chat.id, text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))

async def send_user_management_menu(chat_id, context: ContextTypes.DEFAULT_TYPE, message_id=None):
    text = "👤 **USER MANAGEMENT**\n\n**Select an action:**"
    keyboard = [
        [InlineKeyboardButton("🔍 Find & Manage User by ID/Username", url=f"https://t.me/{context.bot.username}?start=admin_user_lookup")],
        [InlineKeyboardButton("🚫 Ban/Unban User (via command)", callback_data="admin_dummy")],
        [InlineKeyboardButton("🔙 BACK TO MENU", callback_data="admin_back")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    if message_id:
        try: await context.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text, parse_mode='Markdown', reply_markup=reply_markup); return
        except Exception as e: logger.warning(f"Failed to edit user management menu: {e}")
    await context.bot.send_message(chat_id, text, parse_mode='Markdown', reply_markup=reply_markup)

async def send_manage_channels_menu(chat_id, context: ContextTypes.DEFAULT_TYPE, message_id=None):
    active_channels = get_all_force_sub_channels(return_usernames_only=False)
    text = "📢 **MANAGE FORCE SUB CHANNELS**\n\n"
    if not active_channels: text += "No active force-subscription channels configured."
    for username, title in active_channels: text += f"\n• {title} (`{username}`)"
    removal_buttons = [[InlineKeyboardButton(f"🗑️ Remove {title}", callback_data=f"remove_channel_{username}")] for username, title in active_channels]
    keyboard = [[InlineKeyboardButton("➕ ADD NEW CHANNEL", callback_data="add_channel_prompt")]]
    keyboard.extend(removal_buttons)
    keyboard.append([InlineKeyboardButton("🔙 BACK TO MENU", callback_data="admin_back")])
    reply_markup = InlineKeyboardMarkup(keyboard)
    if message_id:
        try: await context.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text, parse_mode='Markdown', reply_markup=reply_markup); return
        except Exception as e: logger.warning(f"Failed to edit channel management menu: {e}")
    await context.bot.send_message(chat_id, text, parse_mode='Markdown', reply_markup=reply_markup)

async def send_admin_menu(chat_id, context: ContextTypes.DEFAULT_TYPE, message_id=None):
    stats_text = f"📊 Users: {get_user_count()} | Channels: {len(get_all_force_sub_channels())}"
    keyboard = [
        [InlineKeyboardButton("📢 BROADCAST MESSAGE", callback_data="admin_broadcast_start")],
        [InlineKeyboardButton("🔗 GENERATE DEEP LINK", callback_data="generate_link")],
        [InlineKeyboardButton("👤 USER MANAGEMENT", callback_data="user_management")],
        [InlineKeyboardButton("⚙️ MANAGE FORCE SUB CHANNELS", callback_data="manage_force_sub")],
        [InlineKeyboardButton(stats_text, callback_data="admin_dummy_stats")],
        [InlineKeyboardButton("🔄 RELOAD BOT (Command)", callback_data="admin_dummy")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    text = "👑 **Admin Panel**\n\nUse the buttons below to manage the bot."
    if message_id:
        try: await context.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text, parse_mode='Markdown', reply_markup=reply_markup); return
        except Exception as e: logger.warning(f"Failed to edit admin menu: {e}")
    msg = await context.bot.send_message(chat_id, text, parse_mode='Markdown', reply_markup=reply_markup)
    context.user_data['bot_prompt_message_id'] = msg.message_id


# ========== BROADCAST UTILITIES ==========

async def broadcast_message_throttled(context: ContextTypes.DEFAULT_TYPE, message_to_copy):
    all_users = get_all_users()
    total_users = len(all_users)
    successful = 0
    failed = 0
    confirmation_text = f"⏳ Broadcast started. Sending message to {total_users} users in chunks."
    await context.bot.send_message(ADMIN_ID, confirmation_text)

    if total_users >= BROADCAST_MIN_USERS:
        is_throttled = True
        chunk_size = BROADCAST_CHUNK_SIZE
        interval_seconds = BROADCAST_INTERVAL_MIN * 60
    else:
        is_throttled = False
        chunk_size = total_users
        interval_seconds = 0

    start_time = time.time()
    chunks = [all_users[i:i + chunk_size] for i in range(0, total_users, chunk_size)]
    
    for i, chunk in enumerate(chunks):
        chunk_start_time = time.time()
        for user_tuple in chunk:
            user_id = user_tuple[0]
            try:
                await context.bot.copy_message(chat_id=user_id, from_chat_id=message_to_copy.chat_id, message_id=message_to_copy.message_id, reply_markup=message_to_copy.reply_markup)
                successful += 1
            except Exception as e:
                logger.debug(f"Failed to send broadcast to user {user_id}: {e}")
                failed += 1

        chunk_duration = time.time() - chunk_start_time
        if is_throttled and i < len(chunks) - 1:
            wait_time = interval_seconds - chunk_duration
            if wait_time > 0:
                progress_message = (
                    f"✅ Chunk {i+1}/{len(chunks)} completed.\n"
                    f"Current Progress: {successful} successful, {failed} failed.\n"
                    f"Next chunk starting in approx. {BROADCAST_INTERVAL_MIN} minutes..."
                )
                await context.bot.send_message(ADMIN_ID, progress_message)
                await asyncio.sleep(wait_time)

    end_time = time.time()
    duration = end_time - start_time

    summary = (
        "📢 **BROADCAST COMPLETE**\n\n"
        f"👥 Total Users in DB: {total_users}\n"
        f"✅ Successful Sends: {successful}\n"
        f"❌ Failed Sends (Blocked/Deactivated): {failed}\n"
        f"⏱️ Total Duration: {int(duration // 60)}m {int(duration % 60)}s"
    )
    await context.bot.send_message(ADMIN_ID, summary, parse_mode='Markdown')

async def broadcast_message_to_all_users(update: Update, context: ContextTypes.DEFAULT_TYPE, message_to_copy):
    context.application.create_task(broadcast_message_throttled(context, message_to_copy))


# ========== ERROR & MAINTENANCE HANDLERS ==========

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("Exception while handling an update:", exc_info=context.error)

    if ADMIN_ID and context.bot:
        error_message = (
            f"❌ **An Error Occurred!**\n\n"
            f"**Error:** `{context.error}`\n"
            f"**Update:** `{update.update_id}`\n\n"
            f"Check bot logs for full traceback."
        )
        try:
            await context.bot.send_message(chat_id=ADMIN_ID, text=error_message, parse_mode='Markdown')
        except Exception as e:
            logger.error(f"Failed to send error message to admin: {e}")

async def cleanup_task(context: ContextTypes.DEFAULT_TYPE):
    cleanup_old_links()
    
def keep_alive():
    while True:
        time.sleep(840) 
        try:
            import requests
            requests.get(WEBHOOK_URL + "health", timeout=10)
        except Exception:
            pass
            
def handle_post_restart(application: Application):
    if os.path.exists('restart_message.json'):
        try:
            with open('restart_message.json', 'r') as f:
                restart_info = json.load(f)
            
            chat_id = restart_info.get('chat_id')
            message_id_to_copy = restart_info.get('message_id_to_copy')
            
            if chat_id:
                application.bot.send_message(chat_id, "✅ **Bot has restarted successfully!**", parse_mode='Markdown')
                
                if message_id_to_copy == 'admin':
                    asyncio.run(send_admin_menu(chat_id, application.context()))
                elif message_id_to_copy and isinstance(message_id_to_copy, int):
                    try: application.bot.copy_message(chat_id, chat_id, message_id_to_copy)
                    except Exception as e: logger.warning(f"Failed to copy message {message_id_to_copy} after restart: {e}")
                        
        except Exception as e: logger.error(f"Error handling post-restart: {e}")
        finally: os.remove('restart_message.json')


def main():
    # 1. Initialize DB and Connection Pool (Crucial for persistence and speed)
    try: init_db()
    except Exception as e: logger.error(f"FATAL: Database initialization failed. Stopping bot: {e}"); sys.exit(1) 

    if not BOT_TOKEN or BOT_TOKEN == "YOUR_TOKEN_HERE": logger.error("BOT_TOKEN not set!"); return

    application = Application.builder().token(BOT_TOKEN).build()
    
    # Check and handle post-restart actions
    handle_post_restart(application)
    
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(button_handler))
    
    admin_filter = filters.User(user_id=ADMIN_ID)
    application.add_handler(CommandHandler("reload", reload_command, filters=admin_filter)) 
    application.add_handler(CommandHandler("stats", stats_command, filters=admin_filter)) 
    application.add_handler(CommandHandler("addchannel", add_channel_command, filters=admin_filter))
    application.add_handler(CommandHandler("removechannel", remove_channel_command, filters=admin_filter))
    application.add_handler(CommandHandler("banuser", ban_user_command, filters=admin_filter))
    application.add_handler(CommandHandler("unbanuser", unban_user_command, filters=admin_filter))
    
    application.add_handler(MessageHandler(admin_filter & ~filters.COMMAND, handle_admin_message))
    
    application.add_error_handler(error_handler)

    if application.job_queue:
        application.job_queue.run_repeating(cleanup_task, interval=600, first=10)

    if WEBHOOK_URL and BOT_TOKEN:
        keep_alive_thread = Thread(target=keep_alive, daemon=True)
        keep_alive_thread.start()
        
        logger.info(f"Starting bot with webhook URL: {WEBHOOK_URL}")
        application.run_webhook(
            listen="0.0.0.0",
            port=PORT,
            url_path=BOT_TOKEN,
            webhook_url=WEBHOOK_URL + BOT_TOKEN
        )
    else:
        logger.warning("WEBHOOK_URL not set. Falling back to local polling.")
        application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main()
