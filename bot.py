import os
import logging
import pg8000.dbapi # 🚨 PostgreSQL Library
import ssl          # For secure DB connection (required by some hosts like Render/Heroku)
import certifi      # For SSL root certificates
import secrets
import urllib.parse 
import requests     # For Keep-Alive feature
import time
import asyncio
import sys 
import json 
from datetime import datetime, timedelta
from functools import wraps
from threading import Thread
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, 
    CommandHandler, 
    CallbackQueryHandler, 
    ContextTypes, 
    MessageHandler, 
    filters
)
from telegram.error import Forbidden, BadRequest

# --- Configuration and Setup ---

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Bot configuration
BOT_TOKEN = os.getenv("BOT_TOKEN", "YOUR_TOKEN_HERE") # <<-- REPLACE ME
ADMIN_ID = 829342319  # <<-- REPLACE with your actual Admin ID
LINK_EXPIRY_MINUTES = 5
DATABASE_URL = os.environ.get('DATABASE_URL') # MUST be set as an environment variable

# --- BROADCAST THROTTLING CONSTANTS ---
BROADCAST_CHUNK_SIZE = 1000  
BROADCAST_MIN_USERS = 5000   
BROADCAST_INTERVAL_MIN = 20 
# ------------------------------------

# Webhook / polling config
PORT = int(os.environ.get('PORT', 8080))
WEBHOOK_URL = os.environ.get('RENDER_EXTERNAL_URL', '').rstrip('/')
KEEP_ALIVE_URL = os.environ.get('KEEP_ALIVE_URL')

# --- Global State ---
# Used for multi-step admin processes like broadcasting
BOT_STATE = {} # Key: admin_id, Value: {"state": "PENDING_BROADCAST", "message": "..."}

# --- Database Connection and Utility Functions (PostgreSQL) ---

def get_db_connection():
    """Establishes and returns a PostgreSQL database connection."""
    if not DATABASE_URL:
        logger.error("DATABASE_URL is not set.")
        return None
    
    url = urllib.parse.urlparse(DATABASE_URL)
    conn_params = {
        'user': url.username,
        'password': url.password,
        'host': url.hostname,
        'port': url.port or 5432,
        'database': url.path[1:],
    }
    
    # Use SSL context for secure connections (e.g., Render, Heroku)
    if 'sslmode=require' in DATABASE_URL or 'sslmode=verify-full' in DATABASE_URL or url.hostname and ('render.com' in url.hostname or 'heroku.com' in url.hostname):
        conn_params['ssl_context'] = ssl.create_default_context(cafile=certifi.where())
    
    try:
        conn = pg8000.dbapi.connect(**conn_params)
        return conn
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        return None

def db_init():
    """Initializes the database schema if it doesn't exist."""
    conn = get_db_connection()
    if not conn: return
    
    try:
        with conn.cursor() as cursor:
            # Users table: Stores user data and ban status
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    join_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_banned BOOLEAN DEFAULT FALSE,
                    username VARCHAR(32)
                );
            """)
            # Required channels table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS required_channels (
                    channel_id BIGINT PRIMARY KEY,
                    title VARCHAR(255)
                );
            """)
            conn.commit()
            logger.info("Database initialized successfully.")
    except Exception as e:
        logger.error(f"Error during DB initialization: {e}")
    finally:
        if conn: conn.close()

async def update_user_activity(user_id: int, username: str | None):
    """Updates user's last activity and username (UPSERT operation)."""
    conn = get_db_connection()
    if not conn: return

    try:
        with conn.cursor() as cursor:
            # PostgreSQL's ON CONFLICT DO UPDATE
            cursor.execute("""
                INSERT INTO users (user_id, last_activity, username) 
                VALUES (%s, CURRENT_TIMESTAMP, %s)
                ON CONFLICT (user_id) DO UPDATE 
                SET last_activity = CURRENT_TIMESTAMP, username = %s;
            """, (user_id, username, username))
            conn.commit()
    except Exception as e:
        logger.error(f"Error updating user activity for {user_id}: {e}")
    finally:
        if conn: conn.close()

def is_user_banned(user_id: int) -> bool:
    """Checks if a user is banned."""
    conn = get_db_connection()
    if not conn: return False
    
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT is_banned FROM users WHERE user_id = %s;", (user_id,))
            result = cursor.fetchone()
            return result and result[0]
    except Exception as e:
        logger.error(f"Error checking ban status for {user_id}: {e}")
        return False
    finally:
        if conn: conn.close()
        
# --- Decorators ---

def restricted(func):
    """Decorator that restricts access to the bot admin."""
    @wraps(func)
    async def wrapped(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        if update.effective_user.id != ADMIN_ID:
            logger.warning(f"Unauthorized access attempt by {update.effective_user.id} to {func.__name__}")
            await update.message.reply_text("You are not authorized to use this command.")
            return
        return await func(update, context, *args, **kwargs)
    return wrapped

# --- Membership Check ---

async def check_membership(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> tuple[bool, str]:
    """Checks if a user is a member of all required channels."""
    conn = get_db_connection()
    if not conn: return False, "Database connection error."
    
    required_channels = []
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT channel_id FROM required_channels;")
            required_channels = [row[0] for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"Error fetching channels: {e}")
        return False, "Error fetching required channels from DB."
    finally:
        if conn: conn.close()

    if not required_channels:
        return True, ""

    missing_channel_ids = []
    for channel_id in required_channels:
        try:
            member = await context.bot.get_chat_member(channel_id, user_id)
            if member.status in ['left', 'kicked', 'banned']:
                missing_channel_ids.append(channel_id)
        except BadRequest:
            logger.warning(f"Could not check membership for channel {channel_id}. Bot is not an admin.")
            continue 

    if missing_channel_ids:
        join_links = "\n".join([f"• Channel ID: `{cid}`" for cid in missing_channel_ids])
        error_message = (
            "⚠️ **ACCESS DENIED** ⚠️\n\n"
            "You must join the following channels to use this bot:\n"
            f"{join_links}\n\n"
            "After joining, please press the **'✅ Check Membership'** button again."
        )
        return False, error_message
    
    return True, ""

# --- Core Bot Command Handlers ---

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /start command, including deep links."""
    user = update.effective_user
    user_id = user.id
    username = user.username
    
    await update_user_activity(user_id, username)
    
    if context.args:
        payload = context.args[0]
        if payload.startswith("admin_user_lookup_"):
            if user_id == ADMIN_ID:
                target_user_id = payload.replace("admin_user_lookup_", "")
                await handle_admin_user_lookup(update, context, target_user_id)
                return
            else:
                await update.message.reply_text("This link is for bot administrators only. Use the normal /start command to begin.")
                return

    if is_user_banned(user_id):
        await update.message.reply_text("You have been banned from using this bot.")
        return

    is_member, error_message = await check_membership(user_id, context)
    
    if is_member:
        welcome_message = (
            f"Hello, {user.first_name}!\n\n"
            "Welcome to the bot. You now have full access to its features."
            "\nType /help for a list of commands."
        )
        await update.message.reply_text(welcome_message)
    else:
        keyboard = [[InlineKeyboardButton("✅ Check Membership", callback_data="check_membership")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(error_message, reply_markup=reply_markup, parse_mode='Markdown')

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Sends a help message."""
    user_id = update.effective_user.id
    if is_user_banned(user_id):
        await update.message.reply_text("You have been banned from using this bot.")
        return
        
    is_member, _ = await check_membership(user_id, context)
    if not is_member:
        await start_command(update, context)
        return
        
    help_text = (
        "🤖 **Bot Help**\n\n"
        "Available commands:\n"
        "• /start - Start interaction or re-check access.\n"
        "• /help - Show this help message.\n"
    )
    if user_id == ADMIN_ID:
        help_text += (
            "\n\n👑 **Admin Commands**:\n"
            "• /stats - Get bot statistics.\n"
            "• /userinfo `<user_id>` - Get admin lookup link for a user.\n"
            "• /broadcast - Start a message broadcast process.\n"
            "• /addchannel `<id>` - Add a required channel.\n"
            "• /removechannel `<id>` - Remove a required channel.\n"
            "• /banuser `<id>` - Ban a user (Legacy/Fallback).\n"
            "• /unbanuser `<id>` - Unban a user (Legacy/Fallback).\n"
            "• /reload - Restart the bot.\n"
        )
    await update.message.reply_text(help_text, parse_mode='Markdown')

# --- Admin Functionality ---

@restricted
async def reload_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Restarts the bot."""
    await update.message.reply_text("🤖 Restarting bot...")
    
    try:
        with open("restart_command.json", "w") as f:
            json.dump({"command": "/reload", "user_id": update.effective_user.id, "time": time.time()}, f)
        
        # Use os.execl to replace the current process with a new instance
        os.execl(sys.executable, sys.executable, *sys.argv)
    except Exception as e:
        await update.message.reply_text(f"❌ Failed to restart: {e}")

@restricted
async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Provides bot statistics to the admin."""
    conn = get_db_connection()
    if not conn:
        await update.message.reply_text("Error: Could not connect to the database to fetch stats.")
        return

    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(user_id) FROM users;")
            total_users = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(user_id) FROM users WHERE is_banned = TRUE;")
            banned_users = cursor.fetchone()[0]

            cursor.execute("SELECT channel_id, title FROM required_channels;")
            required_channels = cursor.fetchall()
            
            cursor.execute("SELECT MAX(last_activity) FROM users;")
            last_activity = cursor.fetchone()[0]

            stats_text = (
                "📊 **Bot Statistics**\n\n"
                f"• Total Users: `{total_users}`\n"
                f"• Banned Users: `{banned_users}`\n"
                f"• Last User Activity: `{last_activity.strftime('%Y-%m-%d %H:%M:%S') if last_activity else 'N/A'}`\n\n"
                f"**Required Channels ({len(required_channels)})**:\n"
            )
            for cid, title in required_channels:
                 stats_text += f"• `{title}` (ID: `{cid}`)\n"
            if not required_channels:
                 stats_text += "• None\n"

            await update.message.reply_text(stats_text, parse_mode='Markdown')
            
    except Exception as e:
        logger.error(f"Error fetching stats: {e}")
        await update.message.reply_text(f"An error occurred while fetching stats: {e}")
    finally:
        if conn: conn.close()

@restricted
async def user_info_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Generates an admin lookup deep link for a user (Fixes NameError)."""
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Usage: `/userinfo <user_id>` (e.g., `/userinfo 123456789`)", parse_mode='Markdown')
        return

    target_user_id = int(context.args[0])
    
    # 🚨 FIX: Define the variable correctly here
    user_lookup_url = (
        f"https://t.me/{context.bot.get_me().username}"
        f"?start=admin_user_lookup_{target_user_id}"
    )

    message_text = (
        f"👤 **Admin Lookup Link for User ID:** `{target_user_id}`\n\n"
        "Click the link below to load their details and management options:\n"
        f"🔗 [User Lookup Link]({user_lookup_url})"
    )
    
    await update.message.reply_text(
        message_text,
        parse_mode='Markdown',
        disable_web_page_preview=True
    )

async def handle_admin_user_lookup(update: Update, context: ContextTypes.DEFAULT_TYPE, target_user_id_str: str) -> None:
    """Handles the deep link payload to show admin user info."""
    
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("This is an internal admin function and cannot be used directly.")
        return

    try:
        target_user_id = int(target_user_id_str)
    except ValueError:
        await update.message.reply_text("Invalid User ID in the lookup link.")
        return
        
    conn = get_db_connection()
    if not conn:
        await update.message.reply_text("Error: Could not connect to the database for lookup.")
        return

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT user_id, join_date, last_activity, is_banned, username FROM users WHERE user_id = %s;", 
                (target_user_id,)
            )
            user_data = cursor.fetchone()
            
            if not user_data:
                await update.message.reply_text(f"User ID `{target_user_id}` not found in the database.")
                return

            user_id, join_date, last_activity, is_banned, username = user_data
            
            status = "✅ Active" if not is_banned else "🛑 **BANNED**"
            username_display = f"@{username}" if username else "N/A"
            
            info_text = (
                f"👤 **User Management (Admin View)**\n\n"
                f"• **ID:** `{user_id}`\n"
                f"• **Username:** {username_display}\n"
                f"• **Status:** {status}\n"
                f"• **Join Date:** `{join_date.strftime('%Y-%m-%d %H:%M:%S')}`\n"
                f"• **Last Activity:** `{last_activity.strftime('%Y-%m-%d %H:%M:%S')}`"
            )
            
            # Setup action buttons
            if is_banned:
                button = InlineKeyboardButton("🔓 Unban User", callback_data=f"unban:{user_id}")
            else:
                button = InlineKeyboardButton("🚫 Ban User", callback_data=f"ban:{user_id}")
                
            keyboard = [[button]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await update.message.reply_text(
                info_text, 
                reply_markup=reply_markup, 
                parse_mode='Markdown'
            )

    except Exception as e:
        logger.error(f"Error handling admin user lookup for {target_user_id_str}: {e}")
        await update.message.reply_text(f"An error occurred during user lookup: {e}")
    finally:
        if conn: conn.close()

@restricted
async def add_channel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Adds a required channel ID for membership checks."""
    if not context.args or not context.args[0].strip().lstrip('-').isdigit():
        await update.message.reply_text("Usage: `/addchannel <channel_id>` (Use the full negative ID, e.g., `-1001234567890`)", parse_mode='Markdown')
        return

    channel_id = int(context.args[0])
    
    conn = get_db_connection()
    if not conn:
        await update.message.reply_text("Database connection error.")
        return

    try:
        # Get channel title for display
        chat = await context.bot.get_chat(channel_id)
        title = chat.title

        with conn.cursor() as cursor:
            # UPSERT for channels
            cursor.execute("INSERT INTO required_channels (channel_id, title) VALUES (%s, %s) ON CONFLICT (channel_id) DO UPDATE SET title = EXCLUDED.title;", (channel_id, title))
            conn.commit()
        
        await update.message.reply_text(f"✅ Channel `{title}` (ID: `{channel_id}`) added to required list.")
    except BadRequest as e:
        await update.message.reply_text(f"❌ Failed to get chat info for ID `{channel_id}`. Ensure the bot is an administrator in the channel: {e}")
    except Exception as e:
        logger.error(f"Error adding channel: {e}")
        await update.message.reply_text(f"An unexpected database error occurred: {e}")
    finally:
        if conn: conn.close()

@restricted
async def remove_channel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Removes a required channel ID."""
    if not context.args or not context.args[0].strip().lstrip('-').isdigit():
        await update.message.reply_text("Usage: `/removechannel <channel_id>` (Use the full negative ID).", parse_mode='Markdown')
        return

    channel_id = int(context.args[0])
    
    conn = get_db_connection()
    if not conn:
        await update.message.reply_text("Database connection error.")
        return

    try:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM required_channels WHERE channel_id = %s;", (channel_id,))
            if cursor.rowcount > 0:
                await update.message.reply_text(f"✅ Channel ID `{channel_id}` removed from required list.")
            else:
                await update.message.reply_text(f"⚠️ Channel ID `{channel_id}` was not found in the required list.")
            conn.commit()
    except Exception as e:
        logger.error(f"Error removing channel: {e}")
        await update.message.reply_text(f"An unexpected database error occurred: {e}")
    finally:
        if conn: conn.close()

@restricted
async def ban_user_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Bans a user (Legacy/Fallback command)."""
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Usage: `/banuser <user_id>`", parse_mode='Markdown')
        return
    
    target_user_id = int(context.args[0])
    
    conn = get_db_connection()
    if not conn:
        await update.message.reply_text("Database connection error. Cannot perform action.")
        return

    try:
        with conn.cursor() as cursor:
            cursor.execute("UPDATE users SET is_banned = TRUE WHERE user_id = %s;", (target_user_id,))
            conn.commit()
            if cursor.rowcount > 0:
                await update.message.reply_text(f"✅ User ID `{target_user_id}` has been banned.")
            else:
                await update.message.reply_text(f"⚠️ User ID `{target_user_id}` not found in database or already banned.")
    except Exception as e:
        logger.error(f"Error banning user: {e}")
        await update.message.reply_text(f"An error occurred: {e}")
    finally:
        if conn: conn.close()

@restricted
async def unban_user_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Unbans a user (Legacy/Fallback command)."""
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Usage: `/unbanuser <user_id>`", parse_mode='Markdown')
        return
    
    target_user_id = int(context.args[0])

    conn = get_db_connection()
    if not conn:
        await update.message.reply_text("Database connection error. Cannot perform action.")
        return

    try:
        with conn.cursor() as cursor:
            cursor.execute("UPDATE users SET is_banned = FALSE WHERE user_id = %s;", (target_user_id,))
            conn.commit()
            if cursor.rowcount > 0:
                await update.message.reply_text(f"✅ User ID `{target_user_id}` has been unbanned.")
            else:
                await update.message.reply_text(f"⚠️ User ID `{target_user_id}` not found in database or already unbanned.")
    except Exception as e:
        logger.error(f"Error unbanning user: {e}")
        await update.message.reply_text(f"An error occurred: {e}")
    finally:
        if conn: conn.close()

# --- Broadcast Functions ---

@restricted
async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Initializes the broadcast state."""
    BOT_STATE[ADMIN_ID] = {"state": "PENDING_BROADCAST"}
    await update.message.reply_text("Please reply with the message you want to broadcast (text and media supported).")

@restricted
async def handle_admin_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles messages from the admin when in a special state."""
    admin_state = BOT_STATE.get(ADMIN_ID, {})

    if admin_state.get("state") == "PENDING_BROADCAST":
        # Store the message to be broadcasted
        BOT_STATE[ADMIN_ID]["message"] = update.message.to_dict()
        
        keyboard = [[InlineKeyboardButton("✅ Confirm Broadcast", callback_data="confirm_broadcast")]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await update.message.reply_text(
            "⚠️ **CONFIRM BROADCAST** ⚠️\n\n"
            "The message above will be sent to all active, non-banned users. Confirm?",
            reply_markup=reply_markup
        )
        BOT_STATE[ADMIN_ID]["state"] = "CONFIRM_BROADCAST"
    else:
        await update.message.reply_text("Command not recognized. Type /help for a list of commands.")

async def start_broadcast(context: ContextTypes.DEFAULT_TYPE, message_data: dict, user_ids: list[int]):
    """A background task to send the broadcast message."""
    bot = context.bot
    success_count = 0
    fail_count = 0
    total_users = len(user_ids)
    
    await bot.send_message(ADMIN_ID, f"📢 Starting broadcast to {total_users} users...")
    
    # Check if throttling is needed
    if total_users >= BROADCAST_MIN_USERS:
        is_throttled = True
        chunk_size = BROADCAST_CHUNK_SIZE
        interval_minutes = BROADCAST_INTERVAL_MIN
    else:
        is_throttled = False
        chunk_size = total_users
        interval_minutes = 0

    chunks = [user_ids[i:i + chunk_size] for i in range(0, total_users, chunk_size)]

    for chunk_index, chunk in enumerate(chunks):
        logger.info(f"Processing chunk {chunk_index + 1}/{len(chunks)} ({len(chunk)} users)")
        
        for user_id in chunk:
            try:
                # Reconstruct the message based on its type
                if message_data.get('text'):
                    await bot.send_message(user_id, text=message_data['text'])
                elif message_data.get('photo'):
                    photo = message_data['photo'][-1]
                    caption = message_data.get('caption')
                    await bot.send_photo(user_id, photo=photo['file_id'], caption=caption)
                elif message_data.get('video'):
                    video = message_data['video']
                    caption = message_data.get('caption')
                    await bot.send_video(user_id, video=video['file_id'], caption=caption)
                # Add more message types as needed
                
                success_count += 1
                await asyncio.sleep(0.05)

            except Forbidden:
                # Bot was blocked by the user
                logger.info(f"User {user_id} blocked the bot.")
                fail_count += 1
            except BadRequest as e:
                logger.error(f"Error sending message to {user_id}: {e}")
                fail_count += 1
            except Exception as e:
                logger.error(f"Unexpected error during broadcast to {user_id}: {e}")
                fail_count += 1

        if is_throttled and chunk_index < len(chunks) - 1:
            await bot.send_message(ADMIN_ID, f"⏸ Chunk {chunk_index + 1} finished. Waiting {interval_minutes} minutes before next chunk...")
            await asyncio.sleep(interval_minutes * 60)

    await bot.send_message(
        ADMIN_ID, 
        f"✅ **Broadcast Finished!**\n\n"
        f"• Total Users: {total_users}\n"
        f"• Successfully Sent: {success_count}\n"
        f"• Failed (Blocked/Error): {fail_count}"
    )
    if ADMIN_ID in BOT_STATE:
        del BOT_STATE[ADMIN_ID]

# --- Cleanup and Maintenance ---

def keep_alive(keep_alive_url: str = KEEP_ALIVE_URL):
    """Sends a request to the server's keep-alive URL."""
    if not keep_alive_url:
        logger.warning("KEEP_ALIVE_URL not set. Keep-alive task is disabled.")
        return
        
    while True:
        try:
            response = requests.get(keep_alive_url, timeout=10)
            logger.info(f"Keep-alive ping to {keep_alive_url} successful. Status: {response.status_code}")
        except Exception as e:
            logger.error(f"Keep-alive ping failed: {e}")
        
        time.sleep(600) 

async def cleanup_task(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Scheduled task for regular maintenance."""
    logger.info("Running scheduled cleanup task.")


# --- Callback Handler ---

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles all inline button queries."""
    query = update.callback_query
    await query.answer()
    
    data = query.data
    user_id = query.from_user.id
    
    if data == "check_membership":
        await start_command(update, context)
        
    elif data.startswith(('ban:', 'unban:')):
        await admin_button_handler(update, context)
        
    elif data == "confirm_broadcast" and user_id == ADMIN_ID:
        admin_state = BOT_STATE.get(ADMIN_ID, {})
        if admin_state.get("state") == "CONFIRM_BROADCAST" and "message" in admin_state:
            conn = get_db_connection()
            if not conn:
                await query.edit_message_text("Database connection error. Cannot start broadcast.")
                return

            try:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT user_id FROM users WHERE is_banned = FALSE AND user_id != %s;", (ADMIN_ID,))
                    user_ids = [row[0] for row in cursor.fetchall()]
            finally:
                if conn: conn.close()

            if not user_ids:
                await query.edit_message_text("No eligible users found for broadcast.")
                del BOT_STATE[ADMIN_ID]
                return
                
            message_data = admin_state["message"]
            context.application.create_task(start_broadcast(context, message_data, user_ids))

            await query.edit_message_text("📢 Broadcast initiation confirmed. Sending task started in background.")
        else:
            await query.edit_message_text("Broadcast state is invalid. Please start over with /broadcast.")

async def admin_button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles admin inline button clicks (ban/unban)."""
    query = update.callback_query
    
    if query.from_user.id != ADMIN_ID:
        await query.edit_message_text("You are not authorized for this action.", parse_mode='Markdown')
        return

    data = query.data
    action, target_user_id_str = data.split(':', 1)
    
    try:
        target_user_id = int(target_user_id_str)
    except ValueError:
        await query.edit_message_text("Invalid User ID.")
        return

    conn = get_db_connection()
    if not conn:
        await query.edit_message_text("Database connection error. Cannot perform action.")
        return
        
    success = False
    new_status = ""
    try:
        with conn.cursor() as cursor:
            if action == 'ban':
                cursor.execute("UPDATE users SET is_banned = TRUE WHERE user_id = %s;", (target_user_id,))
                new_status = "BANNED"
            elif action == 'unban':
                cursor.execute("UPDATE users SET is_banned = FALSE WHERE user_id = %s;", (target_user_id,))
                new_status = "UNBANNED"
            conn.commit()
            success = True
            
    except Exception as e:
        logger.error(f"Error performing {action} for {target_user_id}: {e}")
        await query.edit_message_text(f"An error occurred: {e}")
        return
    finally:
        if conn: conn.close()

    if success:
        await query.edit_message_text(
            f"✅ User `{target_user_id}` is now **{new_status}**.\n\n"
            "Use the original lookup link (/userinfo) to view the updated status and buttons.", 
            parse_mode='Markdown'
        )

# --- Error Handling ---

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Log the error and send a traceback to the admin if possible."""
    logger.error("Exception while handling an update:", exc_info=context.error)
    
    if update and hasattr(update, 'effective_chat') and update.effective_chat:
        try:
            error_message = f"An error occurred: {context.error}"
            
            if update.effective_chat.id != ADMIN_ID:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text="🤖 An unexpected error occurred. The developers have been notified."
                )
            
            await context.bot.send_message(
                chat_id=ADMIN_ID,
                text=f"An unhandled error occurred:\n\n`{error_message}`",
                parse_mode='Markdown'
            )
        except Exception as e:
            logger.error(f"Failed to send error message to admin: {e}")

# --- Main function ---

def main() -> None:
    """Start the bot."""
    db_init()
    
    application = Application.builder().token(BOT_TOKEN).build()
    
    # General handlers
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CallbackQueryHandler(button_handler))
    
    # Admin-only command handlers
    admin_filter = filters.User(user_id=ADMIN_ID)
    application.add_handler(CommandHandler("reload", reload_command, filters=admin_filter))
    application.add_handler(CommandHandler("stats", stats_command, filters=admin_filter)) 
    application.add_handler(CommandHandler("userinfo", user_info_command, filters=admin_filter))
    application.add_handler(CommandHandler("broadcast", broadcast_command, filters=admin_filter))
    application.add_handler(CommandHandler("addchannel", add_channel_command, filters=admin_filter))
    application.add_handler(CommandHandler("removechannel", remove_channel_command, filters=admin_filter))
    application.add_handler(CommandHandler("banuser", ban_user_command, filters=admin_filter))
    application.add_handler(CommandHandler("unbanuser", unban_user_command, filters=admin_filter))
    
    # Admin-only message handler (for states like PENDING_BROADCAST)
    application.add_handler(MessageHandler(admin_filter & ~filters.COMMAND, handle_admin_message))
    
    application.add_error_handler(error_handler)

    if application.job_queue:
        application.job_queue.run_repeating(cleanup_task, interval=600, first=10)

    # --- Deployment Logic ---
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
        logger.info("Starting bot with polling...")
        application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    # Check for a pending restart command
    if os.path.exists("restart_command.json"):
        try:
            with open("restart_command.json", "r") as f:
                json.load(f) # Just load to confirm it's valid
            os.remove("restart_command.json")
        except Exception as e:
            logger.error(f"Error reading or removing restart file: {e}")
            
    main()
