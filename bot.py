import os
import logging
import sqlite3
import secrets
import requests
import time
import asyncio
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

# ========== DATABASE FUNCTIONS ==========

def init_db():
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            joined_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_banned BOOLEAN DEFAULT 0
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS force_sub_channels (
            channel_id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_username TEXT UNIQUE,
            channel_title TEXT,
            added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_active BOOLEAN DEFAULT 1
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS generated_links (
            link_id TEXT PRIMARY KEY,
            channel_username TEXT,
            user_id INTEGER,
            created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

def get_user_id_by_username(username):
    """Looks up a user's ID by their @username (case-insensitive)."""
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    # Remove the '@' if present and convert to lowercase for case-insensitive lookup
    clean_username = username.lstrip('@').lower() 
    # Use COLLATE NOCASE for case-insensitive search if available, or just LOWER()
    cursor.execute('SELECT user_id FROM users WHERE LOWER(username) = ?', (clean_username,))
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
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    cursor.execute('UPDATE users SET is_banned = 1 WHERE user_id = ?', (user_id,))
    conn.commit()
    conn.close()

def unban_user(user_id):
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    cursor.execute('UPDATE users SET is_banned = 0 WHERE user_id = ?', (user_id,))
    conn.commit()
    conn.close()

def is_user_banned(user_id):
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    cursor.execute('SELECT is_banned FROM users WHERE user_id = ?', (user_id,))
    result = cursor.fetchone()
    # Check if user exists and if is_banned column is set to 1
    return result[0] == 1 if result else False

def add_user(user_id, username, first_name, last_name):
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    # Ensure username is stored without the leading '@'
    clean_username = username.lstrip('@') if username else None
    cursor.execute('''
        INSERT OR REPLACE INTO users (user_id, username, first_name, last_name)
        VALUES (?, ?, ?, ?)
    ''', (user_id, clean_username, first_name, last_name))
    conn.commit()
    conn.close()

def get_user_count():
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM users')
    count = cursor.fetchone()[0]
    conn.close()
    return count

def get_all_users(limit=None, offset=0):
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    if limit is None:
        # Fetching 6 columns: (uid, username, fname, lname, joined, is_banned)
        cursor.execute('SELECT user_id, username, first_name, last_name, joined_date, is_banned FROM users ORDER BY joined_date DESC')
    else:
        # Fetching 6 columns: (uid, username, fname, lname, joined, is_banned)
        cursor.execute('SELECT user_id, username, first_name, last_name, joined_date, is_banned FROM users ORDER BY joined_date DESC LIMIT ? OFFSET ?', (limit, offset))
    users = cursor.fetchall()
    conn.close()
    return users

def get_user_info_by_id(user_id):
    """Fetches a single user's details by ID."""
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    # Fetching 6 columns: (uid, username, fname, lname, joined, is_banned)
    cursor.execute('SELECT user_id, username, first_name, last_name, joined_date, is_banned FROM users WHERE user_id = ?', (user_id,))
    user = cursor.fetchone()
    conn.close()
    return user

def add_force_sub_channel(channel_username, channel_title):
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    try:
        # Re-activate channel if it was previously set to inactive, or insert if new.
        cursor.execute('UPDATE force_sub_channels SET is_active = 1, channel_title = ? WHERE channel_username = ?', (channel_title, channel_username))
        if cursor.rowcount == 0:
            cursor.execute('''
                INSERT INTO force_sub_channels (channel_username, channel_title, is_active)
                VALUES (?, ?, 1)
            ''', (channel_username, channel_title))
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
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    if return_usernames_only:
        cursor.execute('SELECT channel_username FROM force_sub_channels WHERE is_active = 1 ORDER BY channel_title')
        channels = [row[0] for row in cursor.fetchall()]
    else:
        cursor.execute('SELECT channel_username, channel_title FROM force_sub_channels WHERE is_active = 1 ORDER BY channel_title')
        channels = cursor.fetchall()
    conn.close()
    return channels

def get_force_sub_channel_info(channel_username):
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    cursor.execute('SELECT channel_username, channel_title FROM force_sub_channels WHERE channel_username = ? AND is_active = 1', (channel_username,))
    channel = cursor.fetchone()
    conn.close()
    return channel

def delete_force_sub_channel(channel_username):
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    # Note: Sets channel to inactive (is_active = 0)
    cursor.execute('UPDATE force_sub_channels SET is_active = 0 WHERE channel_username = ?', (channel_username,))
    conn.commit()
    conn.close()

def generate_link_id(channel_username, user_id):
    link_id = secrets.token_urlsafe(16)
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    cursor.execute('''
        INSERT OR REPLACE INTO generated_links (link_id, channel_username, user_id)
        VALUES (?, ?, ?)
    ''', (link_id, channel_username, user_id))
    conn.commit()
    conn.close()
    return link_id

def get_link_info(link_id):
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    cursor.execute('''
        SELECT channel_username, user_id, created_time
        FROM generated_links WHERE link_id = ?
    ''', (link_id,))
    result = cursor.fetchone()
    conn.close()
    return result
    
# ========== FORCE SUBSCRIPTION LOGIC (with Ban Check) ==========

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
                keyboard.append([InlineKeyboardButton(f"📢 Join {title}", url=f"https://t.me/{uname.lstrip('@')}")])
                channels_text_list.append(f"• {title} (<code>{uname}</code>)")
                
            keyboard.append([InlineKeyboardButton("✅ Verify Subscription", callback_data="verify_subscription")])
            reply_markup = InlineKeyboardMarkup(keyboard)

            channels_text = "\n".join(channels_text_list)
            text = (
                "📢 <b>Please join our force‑subscription channel(s) first:</b>\n\n"
                f"{channels_text}\n\n"
                "After joining, click <b>Verify Subscription</b>."
            )

            if update.message:
                await update.message.reply_text(text, parse_mode='HTML', reply_markup=reply_markup)
            elif update.callback_query:
                await update.callback_query.edit_message_text(text, parse_mode='HTML', reply_markup=reply_markup)
            return
        
        return await func(update, context, *args, **kwargs)

    return wrapper

# ========== ADMIN COMMAND HANDLERS (for Ban/Unban/Add/Remove) ==========

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
                "❌ Invalid format. Use @username or channel ID (-100...)", parse_mode='HTML'
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
        
        try: await query.delete_message()
        except: pass

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
        
        try: await query.delete_message()
        except: pass
        
        msg = await context.bot.send_message(
            chat_id=query.message.chat_id,
            text="🔗 Send channel username or ID to generate deep link.",
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 CANCEL", callback_data="admin_back")]])
        )
        context.user_data['bot_prompt_message_id'] = msg.message_id
        
    elif data == "add_channel_start":
        if user_id != ADMIN_ID:
            await query.edit_message_text("❌ Admin only", parse_mode='HTML')
            return
        user_states[user_id] = ADD_CHANNEL_USERNAME
        
        try: await query.delete_message()
        except: pass
        
        msg = await context.bot.send_message(
            chat_id=query.message.chat_id,
            text="📢 Send @username of channel to add to force-sub list.",
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 CANCEL", callback_data="manage_force_sub")]])
        )
        context.user_data['bot_prompt_message_id'] = msg.message_id

    elif data.startswith("channel_"):
        if user_id != ADMIN_ID:
            await query.edit_message_text("❌ Admin only", parse_mode='HTML')
            return
        await show_channel_details(query, context, data[8:])

    elif data.startswith("delete_"):
        if user_id != ADMIN_ID:
            await query.edit_message_text("❌ Admin only", parse_mode='HTML')
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
                f"🗑️ Confirm deletion of {channel_info[1]} ({channel_info[0]})?",
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup(keyboard)
            )

    elif data.startswith("confirm_delete_"):
        if user_id != ADMIN_ID:
            await query.edit_message_text("❌ Admin only", parse_mode='HTML')
            return
        channel_username_clean = data[15:]
        channel_username = '@' + channel_username_clean
        delete_force_sub_channel(channel_username)
        await query.edit_message_text(
            f"✅ Channel {channel_username} removed from force‑sub list.",
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📢 Manage Channels", callback_data="manage_force_sub")]])
        )
        
    elif data == "delete_channel_prompt":
        if user_id != ADMIN_ID:
            await query.edit_message_text("❌ Admin only", parse_mode='HTML')
            return
        channels = get_all_force_sub_channels()
        if not channels:
            await query.answer("No channels to delete!", show_alert=True)
            return

        text = "🗑️ Choose a channel to delete (set inactive):"
        keyboard = []
        for uname, title in channels:
            keyboard.append([InlineKeyboardButton(title, callback_data=f"delete_{uname.lstrip('@')}")])
        
        keyboard.append([InlineKeyboardButton("🔙 BACK", callback_data="manage_force_sub")])
        await query.edit_message_text(text=text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))

    elif data in ["admin_back", "user_back", "channels_back"]:
        if user_id == ADMIN_ID:
            await send_admin_menu(query.message.chat_id, context, query)
        else:
            # Non-admin back to main menu
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
            except:
                pass
            try:
                await context.bot.copy_message(
                    chat_id=query.message.chat_id,
                    from_chat_id=WELCOME_SOURCE_CHANNEL,
                    message_id=WELCOME_SOURCE_MESSAGE_ID,
                    reply_markup=reply_markup
                )
            except Exception as e:
                logger.error(f"Error copying back message: {e}")
                fallback = "🏠 <b>Main Menu</b>"
                await context.bot.send_message(query.message.chat_id, fallback, parse_mode='HTML', reply_markup=reply_markup)

    elif data == "about_bot":
        about_text = (
            "<b>About Us</b>\n\n"
            "Developed by @Beat_Anime_Ocean"
        )
        keyboard = [[InlineKeyboardButton("🔙 BACK", callback_data="user_back")]]
        try:
            await query.delete_message()
        except:
            pass
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text=about_text,
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

async def handle_channel_link_deep(update: Update, context: ContextTypes.DEFAULT_TYPE, link_id):
    link_info = get_link_info(link_id)
    if not link_info:
        await update.message.reply_text("❌ This link is invalid or not registered.", parse_mode='HTML')
        return

    channel_identifier, creator_id, created_time = link_info

    try:
        if isinstance(channel_identifier, str) and channel_identifier.lstrip('-').isdigit():
            channel_identifier = int(channel_identifier)
            
        created_dt = datetime.fromisoformat(created_time)
        if datetime.now() > created_dt + timedelta(minutes=LINK_EXPIRY_MINUTES):
            await update.message.reply_text("❌ This link has expired.", parse_mode='HTML')
            return

        chat = await context.bot.get_chat(channel_identifier)
        invite_link = await context.bot.create_chat_invite_link(
            chat.id,
            expire_date=datetime.now().timestamp() + LINK_EXPIRY_MINUTES * 60
        )

        success_message = (
            f"<b>Channel:</b> {chat.title}\n"
            f"<b>Expires in:</b> {LINK_EXPIRY_MINUTES} minutes\n"
            f"<b>Usage:</b> Multiple use within that period\n\n"
            f"Click below:"
        )
        keyboard = [[InlineKeyboardButton("🔗 Request to Join", url=invite_link.invite_link)]]
        await update.message.reply_text(
            success_message,
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    except Exception as e:
        logger.error(f"Error generating invite link: {e}")
        await update.message.reply_text("❌ Error creating invite link. Contact admin.", parse_mode='HTML')

# --- BROADCAST JOB FUNCTION (FOR THROTTLING) ---
async def broadcast_worker_job(context: ContextTypes.DEFAULT_TYPE):
    """JobQueue worker to send a message to a single chunk of users."""
    job_data = context.job.data
    offset = job_data['offset']
    chunk_size = job_data['chunk_size']
    message_chat_id = job_data['message_chat_id']
    message_id = job_data['message_id']
    is_last_chunk = job_data['is_last_chunk']
    admin_chat_id = job_data['admin_chat_id']

    users_chunk = get_all_users(limit=chunk_size, offset=offset)
    sent_count = 0
    fail_count = 0

    for user in users_chunk:
        target_user_id = user[0]
        try:
            # This relies on the message not being deleted by the admin's input handler!
            await context.bot.copy_message(
                chat_id=target_user_id,
                from_chat_id=message_chat_id,
                message_id=message_id
            )
            sent_count += 1
        except Exception as e:
            logger.warning(f"Failed send to {target_user_id} (Offset: {offset}): {e}")
            fail_count += 1
        await asyncio.sleep(0.05) 

    logger.info(f"Broadcast chunk from offset {offset} finished. Sent {sent_count} messages, Failed {fail_count}.")
    
    await context.bot.send_message(
        chat_id=admin_chat_id,
        text=f"✅ **Broadcast Progress:**\nChunk {offset // chunk_size + 1} sent to {sent_count} users (Offset: {offset}). Failed: {fail_count}.",
        parse_mode='Markdown'
    )

    if is_last_chunk:
        total_users = get_user_count() 
        await context.bot.send_message(
            chat_id=admin_chat_id,
            text=f"🎉 **BROADCAST COMPLETE!**\nTotal users attempted: {total_users}.",
            parse_mode='Markdown'
        )
# ------------------------------------------

# --- BROADCAST SCHEDULER FUNCTION ---
async def broadcast_message_to_all_users(update: Update, context: ContextTypes.DEFAULT_TYPE, message_to_copy):
    admin_chat_id = update.effective_chat.id
    total_users = get_user_count()

    if total_users < BROADCAST_MIN_USERS:
        await update.message.reply_text(f"🔄 Broadcasting to {total_users} users (below threshold, no block delay)...", parse_mode='HTML')
        sent = 0
        all_users = get_all_users(limit=None, offset=0)
        for u in all_users:
            target = u[0]
            try:
                await context.bot.copy_message(chat_id=target, from_chat_id=message_to_copy.chat_id, message_id=message_to_copy.message_id)
                sent += 1
            except Exception as e:
                logger.warning(f"Failed send to {target}: {e}")
            await asyncio.sleep(0.05)
        await context.bot.send_message(chat_id=admin_chat_id, text=f"✅ **Broadcast Complete!**\nTotal attempted: {total_users}.\nSuccessfully sent: {sent}.", parse_mode='Markdown')
        try: await update.message.delete()
        except: pass
        return

    # --- THROTTLED BROADCAST LOGIC ---
    await update.message.reply_text(
        f"⏳ **Throttled Broadcast Started!**\n"
        f"Total users: {total_users}.\n"
        f"Sending in chunks of {BROADCAST_CHUNK_SIZE} every {BROADCAST_INTERVAL_MIN} minutes.",
        parse_mode='Markdown'
    )

    offset = 0
    current_delay = 0 
    chunks_sent = 0
    total_chunks = (total_users + BROADCAST_CHUNK_SIZE - 1) // BROADCAST_CHUNK_SIZE

    while offset < total_users:
        is_last_chunk = (offset + BROADCAST_CHUNK_SIZE) >= total_users
        
        job_data = {
            'offset': offset,
            'chunk_size': BROADCAST_CHUNK_SIZE,
            'message_chat_id': message_to_copy.chat_id,
            'message_id': message_to_copy.message_id,
            'is_last_chunk': is_last_chunk,
            'admin_chat_id': admin_chat_id,
        }
        
        context.job_queue.run_once(
            broadcast_worker_job, 
            when=current_delay, 
            data=job_data,
            name=f"broadcast_chunk_{chunks_sent}"
        )

        offset += BROADCAST_CHUNK_SIZE
        current_delay += BROADCAST_INTERVAL_MIN * 60 
        chunks_sent += 1

    await update.message.reply_text(
        f"Scheduled **{total_chunks}** broadcast chunks, running over **{current_delay // 60} minutes**.\n"
        f"You will receive a notification after each chunk is sent.",
        parse_mode='Markdown'
    )
    
    try: await update.message.delete()
    except: pass
# ----------------------------------------------------

async def send_admin_menu(chat_id, context, query=None):
    if query:
        try:
            await query.delete_message()
        except:
            pass
            
    context.user_data.pop('bot_prompt_message_id', None)
    user_states.pop(chat_id, None) 
    
    keyboard = [
        [InlineKeyboardButton("📊 BOT STATS", callback_data="admin_stats")],
        [InlineKeyboardButton("📢 MANAGE FORCE SUB CHANNELS", callback_data="manage_force_sub")],
        [InlineKeyboardButton("🔗 GENERATE CHANNEL LINKS", callback_data="generate_links")],
        [InlineKeyboardButton("📣 START MEDIA BROADCAST", callback_data="admin_broadcast_start")],
        [InlineKeyboardButton("👤 USER MANAGEMENT", callback_data="user_management")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    text = "👨‍💼 <b>ADMIN PANEL</b>\n\nChoose an option:"
    await context.bot.send_message(chat_id=chat_id, text=text, parse_mode='HTML', reply_markup=reply_markup)

async def send_admin_stats(query, context):
    try:
        await query.delete_message()
    except:
        pass
    user_count = get_user_count()
    channel_count = len(get_all_force_sub_channels()) 
    stats_text = (
        "📊 <b>BOT STATISTICS</b>\n\n"
        f"👤 Total Users: {user_count}\n"
        f"📢 Force Sub Channels: {channel_count}\n"
        f"🔗 Link Expiry: {LINK_EXPIRY_MINUTES} minutes"
    )
    keyboard = [
        [InlineKeyboardButton("🔄 REFRESH", callback_data="admin_stats")],
        [InlineKeyboardButton("🔙 BACK", callback_data="admin_back")]
    ]
    await context.bot.send_message(chat_id=query.message.chat_id, text=stats_text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))

async def show_force_sub_management(query, context):
    channels = get_all_force_sub_channels(return_usernames_only=False)
    channels_text = "📢 <b>FORCE SUBSCRIPTION CHANNELS</b>\n\n"
    if not channels:
        channels_text += "No channels configured."
    else:
        channels_text += "<b>Configured Channels:</b>\n"
        for uname, title in channels:
            channels_text += f"• {title} (<code>{uname}</code>)\n"
    
    keyboard = [[InlineKeyboardButton("➕ ADD NEW CHANNEL", callback_data="add_channel_start")]]
    
    if channels:
        buttons = [InlineKeyboardButton(title, callback_data=f"channel_{uname.lstrip('@')}") for uname, title in channels]
        grouped = [buttons[i:i+2] for i in range(0, len(buttons), 2)]
        keyboard.extend(grouped)
        
        keyboard.append([InlineKeyboardButton("🗑️ DELETE CHANNEL", callback_data="delete_channel_prompt")]) 
    
    keyboard.append([InlineKeyboardButton("🔙 BACK TO MENU", callback_data="admin_back")])
    try:
        await query.edit_message_text(text=channels_text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))
    except:
        await context.bot.send_message(chat_id=query.message.chat_id, text=channels_text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))

async def show_channel_details(query, context, channel_username_clean):
    channel_username = '@' + channel_username_clean
    channel_info = get_force_sub_channel_info(channel_username)
    if not channel_info:
        await query.edit_message_text(
            "❌ Channel not found.",
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 BACK", callback_data="manage_force_sub")]])
        )
        return
    uname, title = channel_info
    details = (
        f"📢 <b>CHANNEL DETAILS</b>\n\n"
        f"<b>Title:</b> {title}\n"
        f"<b>Username:</b> <code>{uname}</code>\n"
        f"<i>Choose an action:</i>"
    )
    keyboard = [
        [InlineKeyboardButton("🔗 GENERATE TEMP LINK", callback_data=f"genlink_{channel_username_clean}")],
        [InlineKeyboardButton("🗑️ DELETE CHANNEL", callback_data=f"delete_{channel_username_clean}")],
        [InlineKeyboardButton("🔙 BACK", callback_data="manage_force_sub")]
    ]
    await query.edit_message_text(text=details, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))

async def send_single_user_management(query, context, target_user_id):
    """Shows details and ban/unban buttons for a single user."""
    user_info = get_user_info_by_id(target_user_id)
    
    if not user_info:
        await query.edit_message_text(
            f"❌ User ID **{target_user_id}** not found.",
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 BACK TO USER LIST", callback_data="user_management")]])
        )
        return

    uid, username, fname, lname, joined, is_banned = user_info
    
    uname_display = f"@{username}" if username else "N/A"
    name = f"{fname or ''} {lname or ''}".strip() or "N/A"
    status = "🚫 **BANNED**" if is_banned else "✅ **Active**"
    
    text = (
        f"👤 <b>USER DETAILS</b>\n\n"
        f"**Name:** {name}\n"
        f"**ID:** <code>{uid}</code>\n"
        f"**Username:** <code>{uname_display}</code>\n"
        f"**Joined:** {joined}\n"
        f"**Status:** {status}\n\n"
    )

    action_button_text = "✅ UNBAN USER" if is_banned else "🚫 BAN USER"
    action_status = 1 - is_banned # 1 to ban, 0 to unban
    
    keyboard = [
        [InlineKeyboardButton(
            action_button_text, 
            callback_data=f"toggle_ban_f{uid}_f{action_status}" 
        )],
        [InlineKeyboardButton("🔙 BACK TO USER LIST", callback_data="user_management")]
    ]
    
    await query.edit_message_text(text=text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))

async def send_user_management(query, context, offset=0):
    if query.from_user.id != ADMIN_ID:
        await query.answer("You are not authorized", show_alert=True)
        return
    
    total = get_user_count()
    users = get_all_users(limit=10, offset=offset) 
    has_next = total > offset + 10
    has_prev = offset > 0
    
    text = f"👤 <b>USER MANAGEMENT</b>\n\n"
    text += f"Showing {offset+1}-{min(offset+10, total)} of {total} total users.\n\n"
    
    management_keyboard = []

    for (uid, username, fname, lname, joined, is_banned) in users:
        uname_display = f"@{username}" if username else f"ID: {uid}"
        name = f"{fname or ''} {lname or ''}".strip() or "N/A"
        
        status_icon = '🚫' if is_banned else '✅'
        text += f"{status_icon} **{name}** (<code>{uname_display}</code>)\n"

        management_keyboard.append([
            InlineKeyboardButton(
                f"👤 MANAGE: {name}",
                callback_data=f"manage_user_{uid}"
            )
        ])
        
    final_keyboard = management_keyboard
    
    nav = []
    if has_prev:
        nav.append(InlineKeyboardButton("⬅️ PREV", callback_data=f"user_page_{offset-10}"))
    if has_next:
        nav.append(InlineKeyboardButton("NEXT ➡️", callback_data=f"user_page_{offset+10}"))
    if nav:
        final_keyboard.append(nav)
        
    final_keyboard.append([InlineKeyboardButton("🔙 BACK TO MENU", callback_data="admin_back")])

    await query.edit_message_text(text=text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(final_keyboard))

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Exception in update: {context.error}")

async def cleanup_task(context: ContextTypes.DEFAULT_TYPE):
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    cutoff = datetime.now() - timedelta(days=7)
    cursor.execute('DELETE FROM generated_links WHERE datetime(created_time) < ?', (cutoff.isoformat(),))
    conn.commit()
    conn.close()

def keep_alive():
    while True:
        time.sleep(840)
        try:
            requests.get("https://www.google.com/robots.txt", timeout=10)
        except:
            pass

def main():
    init_db()
    if not BOT_TOKEN or BOT_TOKEN == "YOUR_TOKEN_HERE":
        logger.error("BOT_TOKEN not set!")
        return

    application = Application.builder().token(BOT_TOKEN).build()
    
    # --- HANDLERS ---
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(button_handler))
    
    # Admin-only command handlers
    admin_filter = filters.User(user_id=ADMIN_ID)
    application.add_handler(CommandHandler("stats", stats_command, filters=admin_filter)) # <--- NEW /stats COMMAND
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
        application.run_webhook(
            listen="0.0.0.0",
            port=PORT,
            url_path=BOT_TOKEN,
            webhook_url=WEBHOOK_URL + BOT_TOKEN
        )
    else:
        application.run_polling()

if __name__ == "__main__":
    main()
