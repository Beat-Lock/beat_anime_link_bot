import os
import logging
import pg8000.dbapi # Import the pg8000 DBAPI for connection
import urllib.parse as urlparse # For parsing the DATABASE_URL
import secrets
import requests
import time
import asyncio
import sys # ADDED FOR RESTART
import json # ADDED FOR RESTART
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

# --- NEW BROADCAST THROTTLING CONSTANTS ---
BROADCAST_CHUNK_SIZE = 1000  # Number of users to send in each batch
BROADCAST_MIN_USERS = 5000   # Minimum users required to activate throttling
BROADCAST_INTERVAL_MIN = 20  # Delay in minutes between chunks (20-30 min range used 20)
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

# ========== HELPER FUNCTION: AUTO-DELETE (Fixed for Broadcast) ==========

async def delete_update_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Safely attempts to delete the message associated with the incoming update (user input)."""
    user_id = update.effective_user.id
    
    # CRITICAL FIX: DO NOT delete the message if it's the one being broadcasted
    if user_id == ADMIN_ID and user_states.get(user_id) == PENDING_BROADCAST:
        return 
        
    if update.message:
        try:
            await update.message.delete()
        except Exception as e:
            # Ignore errors if the message is too old or already deleted
            logger.warning(f"Could not delete message for user {update.effective_user.id}: {e}")

async def delete_bot_prompt(context: ContextTypes.DEFAULT_TYPE, chat_id):
    """Safely attempts to delete the bot's stored prompt message."""
    prompt_id = context.user_data.pop('bot_prompt_message_id', None)
    if prompt_id:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=prompt_id)
        except Exception as e:
            logger.warning(f"Could not delete bot prompt message {prompt_id}: {e}")
    return prompt_id

# ========== DATABASE FUNCTIONS (POSTGRESQL) ==========

def connect_db():
    """Establishes a connection to the PostgreSQL database using DATABASE_URL."""
    DATABASE_URL = os.getenv("DATABASE_URL")
    if not DATABASE_URL:
        # Fallback for local development if needed, but erroring out is safer for production
        logger.error("DATABASE_URL environment variable is not set!")
        raise ConnectionError("Database URL not found. Set it in your Render environment variables.")
        
    url = urlparse.urlparse(DATABASE_URL)
    
    try:
        # pg8000 uses an empty dict for default SSL context, required by Render PostgreSQL
        conn = pg8000.dbapi.connect(
            database=url.path[1:],
            user=url.username,
            password=url.password,
            host=url.hostname,
            port=url.port,
            ssl_context={} 
        )
        return conn
    except Exception as e:
        logger.critical(f"Failed to connect to PostgreSQL: {e}")
        raise

def init_db():
    """Initializes the PostgreSQL database tables if they do not exist."""
    conn = connect_db()
    cursor = conn.cursor()
    
    # 1. users table: Changed to use BIGINT for user_id (Telegram IDs are large)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            joined_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_banned BOOLEAN DEFAULT FALSE
        )
    """)
    
    # 2. force_sub_channels table: Using channel_username as PRIMARY KEY
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS force_sub_channels (
            channel_username TEXT PRIMARY KEY,
            channel_title TEXT NOT NULL,
            added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_active BOOLEAN DEFAULT TRUE
        )
    """)
    
    # 3. generated_links table: Use TEXT for the primary key (link_id)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS generated_links (
            link_id TEXT PRIMARY KEY,
            channel_username TEXT,
            user_id BIGINT,
            created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    conn.commit()
    conn.close()

def get_user_id_by_username(username):
    """Looks up a user's ID by their @username (case-insensitive)."""
    conn = connect_db()
    cursor = conn.cursor()
    # Remove the '@' if present and convert to lowercase for case-insensitive lookup
    clean_username = username.lstrip('@').lower() 
    # Use LOWER() for case-insensitive search in PostgreSQL
    # IMPORTANT: Changed placeholder from '?' to '%s'
    cursor.execute('SELECT user_id FROM users WHERE LOWER(username) = %s', (clean_username,))
    result = cursor.fetchone()
    conn.close()
    return result[0] if result else None
    
def resolve_target_user_id(arg):
    """Tries to resolve an argument (ID or @username) into a numerical user ID."""
    # 1. Try to parse as integer (ID)
    try:
        return int(arg)
    except ValueError:
        pass

    # 2. Try to look up by username
    if arg:
        return get_user_id_by_username(arg)
    
    return None

def ban_user(user_id):
    conn = connect_db()
    cursor = conn.cursor()
    # IMPORTANT: Changed placeholder from '?' to '%s' and 1 to TRUE
    cursor.execute('UPDATE users SET is_banned = TRUE WHERE user_id = %s', (user_id,))
    conn.commit()
    conn.close()

def unban_user(user_id):
    conn = connect_db()
    cursor = conn.cursor()
    # IMPORTANT: Changed placeholder from '?' to '%s' and 0 to FALSE
    cursor.execute('UPDATE users SET is_banned = FALSE WHERE user_id = %s', (user_id,))
    conn.commit()
    conn.close()

def is_user_banned(user_id):
    conn = connect_db()
    cursor = conn.cursor()
    # IMPORTANT: Changed placeholder from '?' to '%s'
    cursor.execute('SELECT is_banned FROM users WHERE user_id = %s', (user_id,))
    result = cursor.fetchone()
    # Check if user exists and if is_banned column is set to TRUE
    return result[0] is True if result else False

def add_user(user_id, username, first_name, last_name):
    conn = connect_db()
    cursor = conn.cursor()
    # Ensure username is stored without the leading '@'
    clean_username = username.lstrip('@') if username else None
    
    # SQLite's INSERT OR REPLACE is replaced by PostgreSQL's UPSERT (ON CONFLICT DO UPDATE)
    # IMPORTANT: Changed placeholders from '?' to '%s'
    cursor.execute(
        """
        INSERT INTO users (user_id, username, first_name, last_name)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (user_id) DO UPDATE
        SET username = EXCLUDED.username, first_name = EXCLUDED.first_name, last_name = EXCLUDED.last_name;
        """,
        (user_id, clean_username, first_name, last_name)
    )
    conn.commit()
    conn.close()

def get_user_count():
    conn = connect_db()
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM users')
    count = cursor.fetchone()[0]
    conn.close()
    return count

def get_all_users(limit=None, offset=0):
    conn = connect_db()
    cursor = conn.cursor()
    if limit is None:
        # Fetching 6 columns: (uid, username, fname, lname, joined, is_banned)
        cursor.execute('SELECT user_id, username, first_name, last_name, joined_date, is_banned FROM users ORDER BY joined_date DESC')
    else:
        # IMPORTANT: Changed placeholders from '?' to '%s'
        cursor.execute('SELECT user_id, username, first_name, last_name, joined_date, is_banned FROM users ORDER BY joined_date DESC LIMIT %s OFFSET %s', (limit, offset))
    users = cursor.fetchall()
    conn.close()
    return users

def get_user_info_by_id(user_id):
    """Fetches a single user's details by ID."""
    conn = connect_db()
    cursor = conn.cursor()
    # Fetching 6 columns: (uid, username, fname, lname, joined, is_banned)
    # IMPORTANT: Changed placeholder from '?' to '%s'
    cursor.execute('SELECT user_id, username, first_name, last_name, joined_date, is_banned FROM users WHERE user_id = %s', (user_id,))
    user = cursor.fetchone()
    conn.close()
    return user

def add_force_sub_channel(channel_username, channel_title):
    conn = connect_db()
    cursor = conn.cursor()
    try:
        # Use PostgreSQL UPSERT logic (ON CONFLICT DO UPDATE)
        # This reliably updates if the channel exists, or inserts if new.
        # IMPORTANT: Changed placeholders from '?' to '%s' and 1 to TRUE
        cursor.execute(
            """
            INSERT INTO force_sub_channels (channel_username, channel_title, is_active)
            VALUES (%s, %s, TRUE)
            ON CONFLICT (channel_username) DO UPDATE
            SET channel_title = EXCLUDED.channel_title, is_active = TRUE;
            """,
            (channel_username, channel_title)
        )
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"DB Error adding channel: {e}")
        return False
    finally:
        conn.close()

def get_all_force_sub_channels(return_usernames_only=False):
    """
    Fetches all active force sub channels.
    If return_usernames_only is True, returns a list of usernames.
    Otherwise, returns a list of tuples: [(username, title), ...]
    """
    conn = connect_db()
    cursor = conn.cursor()
    if return_usernames_only:
        # IMPORTANT: Changed 1 to TRUE
        cursor.execute('SELECT channel_username FROM force_sub_channels WHERE is_active = TRUE ORDER BY channel_title')
        channels = [row[0] for row in cursor.fetchall()]
    else:
        # IMPORTANT: Changed 1 to TRUE
        cursor.execute('SELECT channel_username, channel_title FROM force_sub_channels WHERE is_active = TRUE ORDER BY channel_title')
        channels = cursor.fetchall()
    conn.close()
    return channels

def get_force_sub_channel_info(channel_username):
    conn = connect_db()
    cursor = conn.cursor()
    # IMPORTANT: Changed placeholder from '?' to '%s' and 1 to TRUE
    cursor.execute('SELECT channel_username, channel_title FROM force_sub_channels WHERE channel_username = %s AND is_active = TRUE', (channel_username,))
    channel = cursor.fetchone()
    conn.close()
    return channel

def delete_force_sub_channel(channel_username):
    conn = connect_db()
    cursor = conn.cursor()
    # Note: Sets channel to inactive (is_active = FALSE)
    # IMPORTANT: Changed placeholder from '?' to '%s' and 0 to FALSE
    cursor.execute('UPDATE force_sub_channels SET is_active = FALSE WHERE channel_username = %s', (channel_username,))
    conn.commit()
    conn.close()

def generate_link_id(channel_username, user_id):
    link_id = secrets.token_urlsafe(16)
    conn = connect_db()
    cursor = conn.cursor()
    # SQLite's INSERT OR REPLACE is replaced by PostgreSQL's UPSERT
    # IMPORTANT: Changed placeholders from '?' to '%s'
    cursor.execute(
        """
        INSERT INTO generated_links (link_id, channel_username, user_id)
        VALUES (%s, %s, %s)
        ON CONFLICT (link_id) DO UPDATE
        SET channel_username = EXCLUDED.channel_username, user_id = EXCLUDED.user_id;
        """,
        (link_id, channel_username, user_id)
    )
    conn.commit()
    conn.close()
    return link_id

def get_link_info(link_id):
    conn = connect_db()
    cursor = conn.cursor()
    # IMPORTANT: Changed placeholder from '?' to '%s'
    cursor.execute('''
        SELECT channel_username, user_id, created_time
        FROM generated_links WHERE link_id = %s
    ''', (link_id,))
    result = cursor.fetchone()
    conn.close()
    return result

# ========== CLEANUP TASK (UPDATED FOR POSTGRESQL) ==========

def cleanup_task(context: ContextTypes.DEFAULT_TYPE):
    """
    Job Queue task to cleanup expired generated links.
    (Runs every 10 minutes, set in main() job_queue.run_repeating)
    """
    logger.info("Running cleanup task...")
    conn = connect_db()
    cursor = conn.cursor()
    
    # Calculate cutoff time
    cutoff = datetime.now() - timedelta(minutes=LINK_EXPIRY_MINUTES)
    
    # PostgreSQL handles date comparison directly against TIMESTAMP column
    # IMPORTANT: Changed placeholder from '?' to '%s' and removed SQLite's datetime() function
    cursor.execute('DELETE FROM generated_links WHERE created_time < %s', (cutoff,))
    
    # Log the cleanup action
    deleted_rows = cursor.rowcount
    logger.info(f"Cleanup finished. Deleted {deleted_rows} expired links.")
    
    conn.commit()
    conn.close()

# ========== FORCE SUBSCRIPTION LOGIC (with Ban Check) ==========

# ... (The rest of the code remains the same as provided by the user, as the database functions are now self-contained)

async def is_user_subscribed(user_id: int, bot) -> bool:
    """Check if user is member of all force‑sub channels."""
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
            # Delete the message that triggered the check (command or message)
            await delete_update_message(update, context)
            
            # Build the keyboard and text using the friendly title
            keyboard = []
            channels_text_list = []
            
            for uname, title in force_sub_channels_info:
                keyboard.append([InlineKeyboardButton(f"{title}", url=f"https://t.me/{uname.lstrip('@')}")])
                channels_text_list.append(f"• {title} (<code>{uname}</code>)")
                
            keyboard.append([InlineKeyboardButton("Click to continue", callback_data="verify_subscription")])
            reply_markup = InlineKeyboardMarkup(keyboard)

            channels_text = "\n".join(channels_text_list)
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

# ========== ADMIN COMMAND HANDLERS (for Ban/Unban/Add/Remove/Reload) ==========

async def reload_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to restart the bot process gracefully. Can optionally take a message ID to send after restart."""
    if update.effective_user.id != ADMIN_ID:
        return

    # Cleanup user state and messages before restart
    await delete_update_message(update, context)
    user_states.pop(update.effective_user.id, None)
    await delete_bot_prompt(context, update.effective_chat.id)
    
    # Check for an optional message ID argument to send after reload
    message_id_to_copy = None
    if context.args:
        try:
            # Check for "admin" argument to skip message copy and show admin menu
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

    # 1. Store the chat ID for post-restart notification, and the message ID to copy
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

    # 2. Send the temporary message
    await update.message.reply_text("🔄 **Bot is restarting...** Please wait.", parse_mode='Markdown')
    
    logger.info("Bot restart initiated by admin. Stopping application.")
    
    # 3. FORCE an exit instead of a graceful stop (This is the change!)
    # await context.application.stop() # Commented out the graceful stop
    sys.exit(0) # ADDED: Use sys.exit(0) for an immediate exit
    # NOTE: The external process supervisor (Render/Heroku/etc) must be running
    # to detect this exit and automatically start the script again.

async def ban_user_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to ban a user by ID or username."""
    if update.effective_user.id != ADMIN_ID:
        return

    await delete_update_message(update, context)
    
    user_states.pop(update.effective_user.id, None)
    await delete_bot_prompt(context, update.effective_chat.id)

    args = context.args
    if len(args) != 1:
        await update.message.reply_text(
            "❌ **Usage:** `/banuser @username or ID`\n**Example:** `/banuser 123456789`",
            parse_mode='Markdown'
        )
        return

    target_arg = args[0]
    target_user_id = resolve_target_user_id(target_arg)

    if target_user_id is None:
        await update.message.reply_text(f"❌ User **{target_arg}** not found in database.", parse_mode='Markdown')
        return
        
    if target_user_id == ADMIN_ID:
        await update.message.reply_text("⚠️ Cannot ban the **Admin**.", parse_mode='Markdown')
        return

    ban_user(target_user_id)
    await update.message.reply_text(
        f"🚫 User with ID **{target_user_id}** (Target: {target_arg}) has been **banned**.",
        parse_mode='Markdown'
    )

async def unban_user_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to unban a user by ID or username."""
    if update.effective_user.id != ADMIN_ID:
        return

    await delete_update_message(update, context)

    user_states.pop(update.effective_user.id, None)
    await delete_bot_prompt(context, update.effective_chat.id)

    args = context.args
    if len(args) != 1:
        await update.message.reply_text(
            "❌ **Usage:** `/unbanuser @username or ID`\n**Example:** `/unbanuser @BannedUser`",
            parse_mode='Markdown'
        )
        return

    target_arg = args[0]
    target_user_id = resolve_target_user_id(target_arg)

    if target_user_id is None:
        await update.message.reply_text(f"❌ User **{target_arg}** not found in database.", parse_mode='Markdown')
        return
        
    unban_user(target_user_id)
    await update.message.reply_text(
        f"✅ User with ID **{target_user_id}** (Target: {target_arg}) has been **unbanned**.",
        parse_mode='Markdown'
    )

async def add_channel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to add a force-sub channel via /addchannel @username title."""
    if update.effective_user.id != ADMIN_ID:
        return

    await delete_update_message(update, context)
    await delete_bot_prompt(context, update.effective_chat.id)

    args = context.args
    if len(args) < 2:
        await update.message.reply_text(
            "❌ **Usage:** `/addchannel @channelusername Channel Title`\n**Example:** `/addchannel @BeatAnime Beat Anime Channel`",
            parse_mode='Markdown'
        )
        return

    channel_username = args[0]
    channel_title = " ".join(args[1:])

    if not channel_username.startswith('@'):
        await update.message.reply_text("❌ Channel username must start with **@**.", parse_mode='Markdown')
        return
        
    try:
        await context.bot.get_chat(channel_username)
    except Exception as e:
        logger.warning(f"Bot failed to get chat {channel_username}: {e}")
        await update.message.reply_text(
            f"⚠️ Bot cannot access channel **{channel_username}**. Make sure the bot is an **Admin** in that channel.",
            parse_mode='Markdown'
        )
        return

    if add_force_sub_channel(channel_username, channel_title):
        await update.message.reply_text(
            f"✅ Successfully added/updated channel:\n**Title:** {channel_title}\n**Username:** `{channel_username}`",
            parse_mode='Markdown'
        )
    else:
        await update.message.reply_text("❌ Failed to add channel. Check logs for database error.", parse_mode='Markdown')

async def remove_channel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to remove a force-sub channel via /removechannel @username."""
    if update.effective_user.id != ADMIN_ID:
        return
        
    await delete_update_message(update, context)
    await delete_bot_prompt(context, update.effective_chat.id)

    args = context.args
    if len(args) != 1:
        await update.message.reply_text(
            "❌ **Usage:** `/removechannel @channelusername`\n**Example:** `/removechannel @OldChannel`",
            parse_mode='Markdown'
        )
        return

    channel_username = args[0]

    if not channel_username.startswith('@'):
        await update.message.reply_text("❌ Channel username must start with **@**.", parse_mode='Markdown')
        return

    channel_info = get_force_sub_channel_info(channel_username)
    if not channel_info:
        await update.message.reply_text(
            f"⚠️ Channel **{channel_username}** is not active or does not exist in the list.",
            parse_mode='Markdown'
        )
        return

    delete_force_sub_channel(channel_username)
    await update.message.reply_text(
        f"🗑️ Successfully removed/deactivated channel **{channel_username}**.",
        parse_mode='Markdown'
    )

@force_sub_required
async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to show bot statistics."""
    if update.effective_user.id != ADMIN_ID:
        return
        
    # 1. Delete the incoming command message (User Input)
    await delete_update_message(update, context)
    
    # Clear any pending state/prompt
    user_states.pop(update.effective_user.id, None)
    await delete_bot_prompt(context, update.effective_chat.id)

    # 2. Get the stats
    user_count = get_user_count()
    channel_count = len(get_all_force_sub_channels()) 
    
    stats_text = (
        "📊 <b>BOT STATISTICS</b>\n\n"
        f"👤 Total Users: {user_count}\n"
        f"📢 Force Sub Channels: {channel_count}\n"
        f"🔗 Link Expiry: {LINK_EXPIRY_MINUTES} minutes"
    )
    
    keyboard = [
        [InlineKeyboardButton("🔙 BACK TO MENU", callback_data="admin_back")]
    ]
    
    # 3. Send the stats as a new message
    await update.message.reply_text(
        text=stats_text, 
        parse_mode='HTML', 
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

# ========== BOT HANDLERS ==========

@force_sub_required
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    # 1. Delete the incoming command message (User Input) - Only if not triggered by callback
    if update.message:
        await delete_update_message(update, context)

    # 2. DELETE THE BOT'S FORCE-SUB PROMPT MESSAGE (Bot Output)
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
    
    if user_id != ADMIN_ID or user_id not in user_states:
        return

    state = user_states[user_id]
    text = update.message.text

    # 1. Delete the bot's prompt message from the previous step (Bot Output)
    await delete_bot_prompt(context, update.effective_chat.id)

    if state == PENDING_BROADCAST:
        # NOTE: The message (update.message) is NOT deleted here, fixing the bug.
        user_states.pop(user_id, None)
        # This function now handles the synchronous or throttled scheduling
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
async def handle_channel_link_deep(update: Update, context: ContextTypes.DEFAULT_TYPE, link_id: str):
    user_id = update.effective_user.id

    # 1. Get link info from DB
    link_info = get_link_info(link_id)

    if not link_info:
        await context.bot.send_message(
            chat_id=user_id,
            text="❌ **Error:** The link you followed is invalid or has expired.",
            parse_mode='Markdown'
        )
        await start(update, context)
        return

    channel_identifier, created_user_id, created_time_db = link_info
    
    # 2. Check Expiry
    if not isinstance(created_time_db, datetime):
        # pg8000 returns datetime objects, but handle potential type issue just in case
        try:
            created_time = datetime.fromisoformat(str(created_time_db))
        except ValueError:
            logger.error(f"Could not parse created_time: {created_time_db}")
            created_time = datetime.now() - timedelta(hours=1) # Force expiry check fail
    else:
         created_time = created_time_db

    expiry_time = created_time + timedelta(minutes=LINK_EXPIRY_MINUTES)
    if datetime.now() > expiry_time:
        await context.bot.send_message(
            chat_id=user_id,
            text=f"⏳ **Error:** The link has expired. It was valid for {LINK_EXPIRY_MINUTES} minutes.",
            parse_mode='Markdown'
        )
        await start(update, context)
        return

    # 3. Check membership (final check)
    subscribed = await is_user_subscribed(user_id, context.bot)
    if not subscribed:
        # The force_sub_required decorator should technically catch this before here, 
        # but a final check is good. If it fails, restart the user.
        return await start(update, context)
        
    # 4. Success: Get the invite link
    try:
        # Get a temporary, short-lived invite link
        invite_link_object = await context.bot.create_chat_invite_link(
            chat_id=channel_identifier,
            member_limit=1, # Only one use
            expire_date=datetime.now() + timedelta(minutes=5) # Expires in 5 minutes
        )
        
        # 5. Send the invite link
        keyboard = [
            [InlineKeyboardButton("🔓 CLICK TO JOIN CHANNEL 🔓", url=invite_link_object.invite_link)]
        ]
        
        await context.bot.send_message(
            chat_id=user_id,
            text="✅ **Success!** Your channel link is ready. Click below to join the channel.",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    except Exception as e:
        logger.error(f"Error generating/sending invite link for {channel_identifier}: {e}")
        await context.bot.send_message(
            chat_id=user_id,
            text="❌ **Critical Error:** I couldn't generate the join link. Please contact the administrator.",
            parse_mode='Markdown'
        )
        
    # After successful link usage, redirect to start menu
    await start(update, context)


async def send_admin_menu(chat_id, context: ContextTypes.DEFAULT_TYPE):
    """Sends the main admin menu."""
    user_count = get_user_count()
    channel_count = len(get_all_force_sub_channels()) 
    
    text = (
        "👑 **ADMIN PANEL**\n\n"
        f"👤 Total Users: {user_count}\n"
        f"📢 Channels: {channel_count}"
    )

    keyboard = [
        [
            InlineKeyboardButton("📣 Broadcast Message", callback_data="admin_broadcast_start"),
            InlineKeyboardButton("🔗 Generate Link", callback_data="generate_links")
        ],
        [
            InlineKeyboardButton("📢 Manage Channels", callback_data="manage_force_sub"),
            InlineKeyboardButton("👤 Manage Users", callback_data="user_management")
        ],
        [InlineKeyboardButton("📊 Stats", callback_data="admin_stats")],
        [InlineKeyboardButton("❌ CLOSE", callback_data="close_message")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    try:
        msg = await context.bot.send_message(
            chat_id=chat_id, 
            text=text, 
            parse_mode='Markdown', 
            reply_markup=reply_markup
        )
        context.user_data['bot_prompt_message_id'] = msg.message_id
    except Exception as e:
        logger.error(f"Error sending admin menu: {e}")

async def send_admin_stats(query, context: ContextTypes.DEFAULT_TYPE):
    user_count = get_user_count()
    channel_count = len(get_all_force_sub_channels()) 
    
    stats_text = (
        "📊 <b>BOT STATISTICS</b>\n\n"
        f"👤 Total Users: {user_count}\n"
        f"📢 Force Sub Channels: {channel_count}\n"
        f"🔗 Link Expiry: {LINK_EXPIRY_MINUTES} minutes"
    )
    
    keyboard = [
        [InlineKeyboardButton("🔙 BACK TO MENU", callback_data="admin_back")]
    ]
    
    await query.edit_message_text(
        text=stats_text, 
        parse_mode='HTML', 
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def show_force_sub_management(query, context: ContextTypes.DEFAULT_TYPE):
    channels = get_all_force_sub_channels(return_usernames_only=False)
    
    text = "📢 **FORCE SUBSCRIBE CHANNELS**\n\n"
    if channels:
        for uname, title in channels:
            text += f"• **{title}** (`{uname}`)\n"
    else:
        text += "No active channels configured.\n"

    keyboard = [
        [InlineKeyboardButton("➕ ADD CHANNEL", callback_data="add_channel_start")],
        [InlineKeyboardButton("➖ REMOVE CHANNEL", callback_data="remove_channel_start")],
        [InlineKeyboardButton("🔙 BACK TO MENU", callback_data="admin_back")]
    ]
    await query.edit_message_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))

async def send_user_management(query, context: ContextTypes.DEFAULT_TYPE, offset: int = 0, limit: int = 10):
    users_data = get_all_users(limit=limit, offset=offset)
    total_users = get_user_count()
    
    text = f"👥 **USER MANAGEMENT**\n(Showing {offset + 1} to {min(offset + limit, total_users)} of {total_users} users)\n\n"
    
    if not users_data:
        text += "No users found."
    else:
        for user in users_data:
            user_id, username, first_name, last_name, joined_date, is_banned = user
            name = first_name if first_name else "N/A"
            name += f" {last_name}" if last_name else ""
            status = "🚫 BANNED" if is_banned else "✅ Active"
            text += f"• `{user_id}` ({status})\n"
            text += f"  Name: {name}\n"
            text += f"  @{username if username else 'N/A'}\n"
            
            keyboard = [[
                InlineKeyboardButton("👁️ VIEW/MANAGE", callback_data=f"manage_user_{user_id}")
            ]]
            # This complex message structure must be sent separately or the loop logic changed. 
            # For simplicity, stick to a list of users for the main view.
            
    # Pagination
    keyboard = []
    nav_buttons = []
    if offset > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"user_page_{max(0, offset - limit)}"))
    if offset + limit < total_users:
        nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"user_page_{offset + limit}"))
    
    if nav_buttons:
        keyboard.append(nav_buttons)
        
    keyboard.append([InlineKeyboardButton("🔙 BACK TO MENU", callback_data="admin_back")])
    
    await query.edit_message_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))

async def send_single_user_management(query, context: ContextTypes.DEFAULT_TYPE, target_user_id: int):
    user = get_user_info_by_id(target_user_id)
    
    if not user:
        await query.edit_message_text(f"❌ User with ID **{target_user_id}** not found.", parse_mode='Markdown')
        return
        
    user_id, username, first_name, last_name, joined_date, is_banned = user
    name = f"{first_name} {last_name}".strip()
    status_text = "🚫 BANNED" if is_banned else "✅ ACTIVE"
    toggle_status = 0 if is_banned else 1
    toggle_text = "✅ UNBAN" if is_banned else "🚫 BAN"
    
    text = (
        f"👤 **USER DETAILS**\n\n"
        f"**ID:** `{user_id}`\n"
        f"**Name:** {name}\n"
        f"**Username:** @{username if username else 'N/A'}\n"
        f"**Joined:** {joined_date.strftime('%Y-%m-%d %H:%M')}\n"
        f"**Status:** {status_text}"
    )
    
    keyboard = [
        [InlineKeyboardButton(toggle_text, callback_data=f"toggle_ban_{user_id}_{toggle_status}")],
        [InlineKeyboardButton("🔙 BACK TO USER LIST", callback_data="user_management")]
    ]
    
    await query.edit_message_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))

async def broadcast_message_to_all_users(update: Update, context: ContextTypes.DEFAULT_TYPE, message):
    """Schedules the broadcast task."""
    user_count = get_user_count()
    
    if user_count < BROADCAST_MIN_USERS:
        # Synchronous broadcast (for smaller user counts)
        await run_synchronous_broadcast(update, context, message)
    else:
        # Throttled/Scheduled broadcast (for larger user counts)
        await schedule_throttled_broadcast(update, context, message, user_count)

async def run_synchronous_broadcast(update, context, message):
    """Sends the message to all users instantly (used for small user counts)."""
    logger.info("Running synchronous broadcast.")
    all_users = get_all_users()
    sent_count = 0
    failed_count = 0

    await update.message.reply_text("📣 **Broadcast started!** (Synchronous mode)", parse_mode='Markdown')

    for user_data in all_users:
        user_id = user_data[0]
        try:
            # Copy the original message content
            await message.copy(chat_id=user_id)
            sent_count += 1
        except Exception as e:
            failed_count += 1
            # Optionally ban the user if error is due to user block/not found
            # if 'blocked by the user' or 'chat not found' in str(e).lower():
            #     ban_user(user_id)
            
        # Small delay to prevent hitting Telegram API limits
        await asyncio.sleep(0.05) 

    await context.bot.send_message(
        chat_id=ADMIN_ID,
        text=f"✅ **Broadcast Finished!** (Synchronous)\nSent to: {sent_count}\nFailed: {failed_count}",
        parse_mode='Markdown'
    )
    logger.info(f"Synchronous broadcast finished. Sent: {sent_count}, Failed: {failed_count}")


async def schedule_throttled_broadcast(update, context, message, total_users: int):
    """Sets up the initial job for throttled broadcast."""
    # 1. Store the message content in a persistent way (not ideal in SQLite, but necessary here)
    # Since we are using PostgreSQL, we can use a simpler method by storing the broadcast content.
    
    # We will use the ADMIN_ID's user state storage for pending broadcast message/offset.
    # We must first convert the message to a serializable format (message ID and chat ID)
    
    # Use context.bot_data for temporary persistent storage across the application,
    # though technically context.bot_data is not persistent across deploys unless linked to DB.
    # Given the bot is now running on persistent DB, let's use a dedicated table for large broadcast
    
    # For simplicity without adding a new table, we will use a JSON file as the bot is now persistent
    # via the file system for this one file.
    
    broadcast_data = {
        'chat_id': message.chat_id,
        'message_id': message.message_id,
        'total_users': total_users,
        'offset': 0,
        'sent_count': 0,
        'failed_count': 0,
        'is_active': True,
        'start_time': datetime.now().isoformat()
    }

    try:
        with open('broadcast_status.json', 'w') as f:
            json.dump(broadcast_data, f)
        
        # 2. Schedule the job queue to start the process
        if context.application.job_queue:
            # Remove any previous broadcast jobs
            current_jobs = context.application.job_queue.get_jobs_by_name("throttled_broadcast")
            for job in current_jobs:
                job.schedule_removal()
                
            # Schedule the first chunk immediately
            context.application.job_queue.run_once(
                throttled_broadcast_job, 
                0, 
                name="throttled_broadcast", 
                data=None # Data is handled via file
            )
            
            await update.message.reply_text(
                f"📣 **Throttled Broadcast Scheduled!**\n\n"
                f"Total Users: {total_users}\n"
                f"Chunk Size: {BROADCAST_CHUNK_SIZE}\n"
                f"Delay: {BROADCAST_INTERVAL_MIN} min/chunk\n"
                f"The first chunk is starting now...",
                parse_mode='Markdown'
            )
            logger.info("Throttled broadcast scheduled.")
            
        else:
            await update.message.reply_text("❌ Job queue not available. Cannot run throttled broadcast.", parse_mode='Markdown')
            
    except Exception as e:
        logger.error(f"Failed to schedule broadcast: {e}")
        await update.message.reply_text("❌ Failed to start broadcast scheduling due to internal error.", parse_mode='Markdown')


async def throttled_broadcast_job(context: ContextTypes.DEFAULT_TYPE):
    """Job function that sends one chunk of the broadcast and schedules the next."""
    job = context.job
    try:
        with open('broadcast_status.json', 'r') as f:
            broadcast_data = json.load(f)
    except FileNotFoundError:
        logger.warning("Broadcast status file not found. Aborting job.")
        return
    except json.JSONDecodeError:
        logger.error("Failed to decode broadcast status JSON. Aborting job.")
        return

    if not broadcast_data.get('is_active'):
        logger.info("Broadcast manually stopped. Removing job.")
        job.schedule_removal()
        return

    offset = broadcast_data['offset']
    total_users = broadcast_data['total_users']
    
    if offset >= total_users:
        # All users processed. Finish the broadcast.
        await finish_throttled_broadcast(context.bot, broadcast_data)
        job.schedule_removal()
        return

    logger.info(f"Running broadcast chunk: offset={offset}, chunk_size={BROADCAST_CHUNK_SIZE}")
    
    # Get the next chunk of users
    users_chunk = get_all_users(limit=BROADCAST_CHUNK_SIZE, offset=offset)
    
    chunk_sent = 0
    chunk_failed = 0
    
    message_chat_id = broadcast_data['chat_id']
    message_id = broadcast_data['message_id']

    # Send message to the chunk
    for user_data in users_chunk:
        user_id = user_data[0]
        try:
            # Copy the original message content
            await context.bot.copy_message(chat_id=user_id, from_chat_id=message_chat_id, message_id=message_id)
            chunk_sent += 1
        except Exception as e:
            chunk_failed += 1
            # In a real environment, logging the failure reason is crucial
            # logger.warning(f"Failed to send to user {user_id}: {e}")
            
        # Small delay to prevent hitting Telegram API limits
        await asyncio.sleep(0.05) 

    # Update broadcast status
    broadcast_data['offset'] += len(users_chunk)
    broadcast_data['sent_count'] += chunk_sent
    broadcast_data['failed_count'] += chunk_failed
    
    # Save updated status
    with open('broadcast_status.json', 'w') as f:
        json.dump(broadcast_data, f)
        
    # Send status update to admin after each chunk
    await context.bot.send_message(
        chat_id=ADMIN_ID,
        text=(
            f"📢 **BROADCAST STATUS UPDATE**\n"
            f"Processed: {broadcast_data['offset']} / {total_users}\n"
            f"Sent: {broadcast_data['sent_count']}\n"
            f"Failed: {broadcast_data['failed_count']}\n"
            f"Next chunk in {BROADCAST_INTERVAL_MIN} minutes..."
        ),
        parse_mode='Markdown'
    )

    # Schedule the next job if there are more users
    if broadcast_data['offset'] < total_users:
        interval_seconds = BROADCAST_INTERVAL_MIN * 60
        context.application.job_queue.run_once(
            throttled_broadcast_job, 
            interval_seconds, 
            name="throttled_broadcast",
            data=None
        )
    else:
        # Final cleanup if the last job finishes the offset
        await finish_throttled_broadcast(context.bot, broadcast_data)
        job.schedule_removal()

async def finish_throttled_broadcast(bot, final_data):
    """Sends the final completion message for the throttled broadcast."""
    try:
        # Delete the temporary status file
        os.remove('broadcast_status.json')
    except:
        pass
        
    await bot.send_message(
        chat_id=ADMIN_ID,
        text=(
            f"✅ **BROADCAST COMPLETED!**\n"
            f"Total Sent: {final_data['sent_count']}\n"
            f"Total Failed: {final_data['failed_count']}\n"
            f"Total Processed: {final_data['total_users']}"
        ),
        parse_mode='Markdown'
    )
    logger.info("Throttled broadcast successfully completed and job removed.")


@force_sub_required
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    data = query.data

    if data == "verify_subscription":
        return await start(update, context)
        
    # Admin state cleanup
    if user_id == ADMIN_ID and user_id in user_states:
        current = user_states[user_id]
        if current in [PENDING_BROADCAST, GENERATE_LINK_CHANNEL_USERNAME, ADD_CHANNEL_TITLE, ADD_CHANNEL_USERNAME] and data in ["admin_back", "manage_force_sub", "user_management"]:
            await delete_bot_prompt(context, query.message.chat_id)
            user_states.pop(user_id, None)

    if data == "close_message":
        try:
            await query.delete_message()
        except Exception as e:
            logger.warning(f"Could not delete message: {e}")
        return

    # --- SINGLE USER MANAGEMENT LOGIC ---
    elif data.startswith("manage_user_"):
        if user_id != ADMIN_ID:
            await query.answer("You are not authorized", show_alert=True)
            return
        user_states.pop(user_id, None)
        await delete_bot_prompt(context, query.message.chat_id)
        try:
            target_user_id = int(data[12:])
            await send_single_user_management(query, context, target_user_id)
        except ValueError:
            await query.answer("Invalid User ID.", show_alert=True)
            return

    # --- BAN/UNBAN LOGIC (Inline Button) ---
    elif data.startswith("toggle_ban_"):
        if user_id != ADMIN_ID:
            await query.answer("You are not authorized", show_alert=True)
            return
        try:
            parts = data.split('_')
            target_user_id = int(parts[2].lstrip('f'))
            target_status = int(parts[3].lstrip('f')) # 1 to ban, 0 to unban

            if target_user_id == ADMIN_ID:
                await query.answer("Cannot ban self!", show_alert=True)
                await send_single_user_management(query, context, target_user_id)
                return

            if target_status == 1:
                ban_user(target_user_id)
                action = "banned"
            else:
                unban_user(target_user_id)
                action = "unbanned"
                
            await send_single_user_management(query, context, target_user_id)
            await query.answer(f"User {target_user_id} successfully {action}.", show_alert=True)
        except Exception as e:
            logger.error(f"Error handling ban/unban: {e}")
            await query.answer("Error processing request.", show_alert=True)
    # --- END BAN/UNBAN LOGIC (Inline Button) ---

    elif data == "admin_broadcast_start":
        if user_id != ADMIN_ID:
            await query.edit_message_text("❌ Admin only", parse_mode='HTML')
            return
        user_states[user_id] = PENDING_BROADCAST
        try:
            await query.delete_message()
        except:
            pass
        msg = await context.bot.send_message(
            chat_id=query.message.chat_id,
            text="📣 Send the message (text, photo, video, etc.) to broadcast now.",
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 CANCEL", callback_data="admin_back")]])
        )
        context.user_data['bot_prompt_message_id'] = msg.message_id
        return

    elif data == "admin_stats":
        if user_id != ADMIN_ID:
            await query.edit_message_text("❌ Admin only", parse_mode='HTML')
            return
        await send_admin_stats(query, context)

    elif data == "user_management":
        if user_id != ADMIN_ID:
            await query.edit_message_text("❌ Admin only", parse_mode='HTML')
            return
        user_states.pop(user_id, None)
        await delete_bot_prompt(context, query.message.chat_id)
        await send_user_management(query, context, offset=0)

    elif data.startswith("user_page_"):
        if user_id != ADMIN_ID:
            await query.edit_message_text("❌ Admin only", parse_mode='HTML')
            return
        try:
            offset = int(data[10:])
        except:
            offset = 0
        await send_user_management(query, context, offset=offset)

    elif data == "manage_force_sub":
        if user_id != ADMIN_ID:
            await query.edit_message_text("❌ Admin only", parse_mode='HTML')
            return
        await show_force_sub_management(query, context)

    elif data == "generate_links":
        if user_id != ADMIN_ID:
            await query.edit_message_text("❌ Admin only", parse_mode='HTML')
            return
        user_states[user_id] = GENERATE_LINK_CHANNEL_USERNAME
        try:
            await query.delete_message()
        except:
            pass
        msg = await context.bot.send_message(
            chat_id=query.message.chat_id,
            text="🔗 **SEND CHANNEL USERNAME OR ID** to generate a link.\n\n"
                 "Example: `@BeatAnime` or `-100123456789`",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 CANCEL", callback_data="admin_back")]])
        )
        context.user_data['bot_prompt_message_id'] = msg.message_id
        return

    elif data == "add_channel_start":
        if user_id != ADMIN_ID:
            await query.edit_message_text("❌ Admin only", parse_mode='HTML')
            return
        user_states[user_id] = ADD_CHANNEL_USERNAME
        try:
            await query.delete_message()
        except:
            pass
        msg = await context.bot.send_message(
            chat_id=query.message.chat_id,
            text="📝 Send channel username (must include **@**). The bot must be an admin there.",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 CANCEL", callback_data="manage_force_sub")]])
        )
        context.user_data['bot_prompt_message_id'] = msg.message_id
        return

    elif data == "remove_channel_start":
        if user_id != ADMIN_ID:
            await query.edit_message_text("❌ Admin only", parse_mode='HTML')
            return
        
        channels = get_all_force_sub_channels(return_usernames_only=False)
        if not channels:
            await query.answer("No channels to remove.", show_alert=True)
            return

        text = "❌ **SELECT CHANNEL TO REMOVE:**"
        keyboard = []
        for uname, title in channels:
            # Use a unique callback data to indicate removal action
            keyboard.append([InlineKeyboardButton(f"🗑️ {title} ({uname})", callback_data=f"remove_c_{uname}")])
            
        keyboard.append([InlineKeyboardButton("🔙 BACK TO MENU", callback_data="manage_force_sub")])
        await query.edit_message_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))

    elif data.startswith("remove_c_"):
        if user_id != ADMIN_ID:
            await query.answer("You are not authorized", show_alert=True)
            return
        
        channel_username = data[9:]
        channel_info = get_force_sub_channel_info(channel_username)
        
        if channel_info:
            delete_force_sub_channel(channel_username)
            await query.answer(f"Channel {channel_username} deactivated.", show_alert=True)
            await show_force_sub_management(query, context) # Refresh the list
        else:
            await query.answer("Channel not found/already removed.", show_alert=True)

    elif data == "admin_back":
        if user_id != ADMIN_ID:
            return
        await send_admin_menu(query.message.chat_id, context)

    elif data == "about_bot":
        text = (
            "🤖 **Anime Link Bot**\n\n"
            "This bot is designed to create temporary, protected deep links to private Telegram channels "
            "while enforcing mandatory subscription to one or more public channels.\n\n"
            "Developed for the **Beat Anime** community."
        )
        keyboard = [
            [InlineKeyboardButton("ᴀɴɪᴍᴇ ᴄʜᴀɴɴᴇʟ", url=PUBLIC_ANIME_CHANNEL_URL)],
            [InlineKeyboardButton("ᴄᴏɴᴛᴀᴄT ᴀᴅᴍɪɴ", url=f"https://t.me/{ADMIN_CONTACT_USERNAME}")],
            [InlineKeyboardButton("ʀᴇǫᴜᴇsᴛ ᴀɴɪᴍᴇ ᴄʜᴀɴɴᴇʟ", url=REQUEST_CHANNEL_URL)],
            [InlineKeyboardButton("🔙 BACK", callback_data="start_menu")]
        ]
        await query.edit_message_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))

    elif data == "start_menu":
        await start(update, context) # Re-run start to show welcome message
        
# ========== ERROR HANDLING ==========

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Log the error and send a message to the admin."""
    logger.error("Exception while handling an update:", exc_info=context.error)

    # Specific error handling (optional, but good for debugging)
    if isinstance(context.error, Exception) and update.effective_chat:
        error_message = f"An error occurred: {context.error}"
        logger.error(error_message)
        
        # Notify admin
        if ADMIN_ID and ADMIN_ID != 0:
            try:
                await context.bot.send_message(
                    chat_id=ADMIN_ID, 
                    text=f"**BOT ERROR**:\n\n{error_message}\n\nUpdate: `{update}`",
                    parse_mode='Markdown'
                )
            except Exception as admin_e:
                logger.error(f"Failed to send error message to admin: {admin_e}")

# ========== MAIN FUNCTION ==========

def keep_alive():
    """Simple thread to keep the web service awake on Render/Heroku."""
    while True:
        time.sleep(840)
        try:
            # Pings a safe external URL to prevent idle shutdown
            requests.get("https://www.google.com/robots.txt", timeout=10)
        except:
            pass

def main():
    # Initialize the database (this will now connect to PostgreSQL)
    try:
        init_db()
        logger.info("Database initialization complete (PostgreSQL).")
    except Exception as e:
        logger.critical(f"FATAL: Could not initialize database: {e}")
        sys.exit(1)


    if not BOT_TOKEN or BOT_TOKEN == "YOUR_TOKEN_HERE":
        logger.error("BOT_TOKEN not set!")
        return

    application = Application.builder().token(BOT_TOKEN).build()
    
    # Check for restart message file
    try:
        with open('restart_message.json', 'r') as f:
            restart_info = json.load(f)
        os.remove('restart_message.json')
        
        # Send post-restart message via a job queue run_once
        if application.job_queue:
            async def post_restart_notification(context: ContextTypes.DEFAULT_TYPE):
                chat_id = restart_info.get('chat_id')
                message_id_to_copy = restart_info.get('message_id_to_copy')
                
                # Try to delete the 'Bot is restarting...' message
                try:
                    # Assuming the message ID to delete is the last message sent before sys.exit(0)
                    # We don't have the message ID here, so we'll just send the success message.
                    pass 
                except:
                    pass
                
                # Send the final message
                if message_id_to_copy == 'admin':
                    await send_admin_menu(chat_id, context)
                elif message_id_to_copy:
                    try:
                        await context.bot.copy_message(chat_id=chat_id, from_chat_id=chat_id, message_id=message_id_to_copy)
                    except:
                        await context.bot.send_message(chat_id=chat_id, text="✅ **Bot has successfully restarted!**", parse_mode='Markdown')
                else:
                    await context.bot.send_message(chat_id=chat_id, text="✅ **Bot has successfully restarted!**", parse_mode='Markdown')
            
            application.job_queue.run_once(post_restart_notification, 1) # Run 1 second after start
            
    except FileNotFoundError:
        pass # No restart pending
    except Exception as e:
        logger.error(f"Error handling post-restart notification: {e}")

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(button_handler))
    
    # Admin-only command handlers
    admin_filter = filters.User(user_id=ADMIN_ID)
    application.add_handler(CommandHandler("reload", reload_command, filters=admin_filter)) # <--- /reload COMMAND
    application.add_handler(CommandHandler("stats", stats_command, filters=admin_filter)) 
    application.add_handler(CommandHandler("addchannel", add_channel_command, filters=admin_filter))
    application.add_handler(CommandHandler("removechannel", remove_channel_command, filters=admin_filter))
    application.add_handler(CommandHandler("banuser", ban_user_command, filters=admin_filter))
    application.add_handler(CommandHandler("unbanuser", unban_user_command, filters=admin_filter))
    
    # Admin-only message handler (for states like PENDING_BROADCAST)
    application.add_handler(MessageHandler(admin_filter & ~filters.COMMAND, handle_admin_message))
    
    application.add_error_handler(error_handler)

    if application.job_queue:
        application.job_queue.run_repeating(cleanup_task, interval=600, first=10)

    if WEBHOOK_URL and BOT_TOKEN:
        keep_alive_thread = Thread(target=keep_alive, daemon=True)
        keep_alive_thread.start()
        logger.info(f"Setting webhook to {WEBHOOK_URL}webhook")
        application.run_webhook(
            listen="0.0.0.0",
            port=PORT,
            url_path="",
            webhook_url=WEBHOOK_URL
        )
    else:
        logger.info("Running in long-polling mode.")
        application.run_polling(drop_pending_updates=True)

if __name__ == '__main__':
    main()
