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
ADMIN_ID = 829342319
LINK_EXPIRY_MINUTES = 5

# --- NEW BROADCAST THROTTLING CONSTANTS ---
BROADCAST_CHUNK_SIZE = 1000  # Number of users to send in each batch
BROADCAST_MIN_USERS = 5000   # Minimum users required to activate throttling
BROADCAST_INTERVAL_MIN = 20  # Delay in minutes between chunks (20-30 min range used 20)
# ------------------------------------------

# Force‑subscribe channels (users must join these)
FORCE_SUB_CHANNELS = [
    "@YourChannel1",
    "@YourChannel2",
    # add more if needed
]

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

# ========== HELPER FUNCTION: AUTO-DELETE (Modified to exclude broadcast message) ==========

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
            joined_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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

def add_user(user_id, username, first_name, last_name):
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    cursor.execute('''
        INSERT OR REPLACE INTO users (user_id, username, first_name, last_name)
        VALUES (?, ?, ?, ?)
    ''', (user_id, username, first_name, last_name))
    conn.commit()
    conn.close()

def get_all_users(limit=None, offset=0):
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    # Note: We only need user_id (u[0]) for broadcasting
    if limit is None:
        cursor.execute('SELECT user_id, username, first_name, last_name, joined_date FROM users ORDER BY joined_date DESC')
    else:
        cursor.execute('SELECT user_id, username, first_name, last_name, joined_date FROM users ORDER BY joined_date DESC LIMIT ? OFFSET ?', (limit, offset))
    users = cursor.fetchall()
    conn.close()
    return users

def get_user_count():
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM users')
    count = cursor.fetchone()[0]
    conn.close()
    return count

def add_force_sub_channel(channel_username, channel_title):
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT OR IGNORE INTO force_sub_channels (channel_username, channel_title)
            VALUES (?, ?)
        ''', (channel_username, channel_title))
        conn.commit()
        return cursor.rowcount > 0
    except Exception as e:
        logger.error(f"DB Error adding channel: {e}")
        return False
    finally:
        conn.close()

def get_all_force_sub_channels():
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
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

# ========== FORCE SUBSCRIPTION LOGIC ==========

async def is_user_subscribed(user_id: int, bot) -> bool:
    """Check if user is member of all force‑sub channels."""
    for ch in FORCE_SUB_CHANNELS:
        try:
            member = await bot.get_chat_member(chat_id=ch, user_id=user_id)
            # If the user has left or was kicked, treat as not subscribed
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

        # ✅ Bypass force-sub for admin
        if user.id == ADMIN_ID:
            return await func(update, context, *args, **kwargs)

        subscribed = await is_user_subscribed(user.id, context.bot)
        if not subscribed:
            # Ask them to join and verify
            keyboard = []
            for ch in FORCE_SUB_CHANNELS:
                keyboard.append([InlineKeyboardButton(f"📢 Join {ch}", url=f"https://t.me/{ch.lstrip('@')}")])
            keyboard.append([InlineKeyboardButton("✅ Verify Subscription", callback_data="verify_subscription")])
            reply_markup = InlineKeyboardMarkup(keyboard)

            channels_text = "\n".join([f"• {ch}" for ch in FORCE_SUB_CHANNELS])
            text = (
                "📢 <b>Please join our force‑subscription channel(s) first:</b>\n\n"
                f"{channels_text}\n\n"
                "After joining, click <b>Verify Subscription</b>."
            )

            if update.message:
                await update.message.reply_text(text, parse_mode='HTML', reply_markup=reply_markup)
            elif update.callback_query:
                await update.callback_query.edit_message_text(text, parse_mode='HTML', reply_markup=reply_markup)
            return  # block further handler execution

        # If subscribed (or admin), proceed
        return await func(update, context, *args, **kwargs)

    return wrapper

# ========== BOT HANDLERS ==========

@force_sub_required
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    add_user(user.id, user.username, user.first_name, user.last_name)

    if context.args and len(context.args) > 0:
        link_id = context.args[0]
        await handle_channel_link_deep(update, context, link_id)
        return

    if user.id == ADMIN_ID:
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
            logger.error(f"Error copying welcome message: {e}")
            fallback_text = "👋 <b>Welcome to the bot!</b>"
            await update.message.reply_text(fallback_text, parse_mode='HTML', reply_markup=reply_markup)

@force_sub_required
async def handle_admin_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    # --- NO DELETION HERE FOR BROADCAST FIX ---
    # The message should not be deleted if it is the one being broadcasted.
    # We will delete admin's input for other states (like ADD_CHANNEL_USERNAME) 
    # inside their state handlers if needed, but not here universally.
    # ------------------------------------------

    if user_id not in user_states:
        return

    state = user_states[user_id]

    if state == PENDING_BROADCAST:
        user_states.pop(user_id, None)
        # The message is NOT deleted here, fixing the broadcast failure
        await broadcast_message_to_all_users(update, context, update.message) 
        await send_admin_menu(update.effective_chat.id, context)
        return

    text = update.message.text
    if text is None:
        # Delete user's non-text message
        await delete_update_message(update, context)
        await update.message.reply_text("❌ Please send text message.", parse_mode='HTML')
        return

    if state == ADD_CHANNEL_USERNAME:
        await delete_update_message(update, context)
        if not text.startswith('@'):
            await update.message.reply_text("❌ Please include @ in channel username.", parse_mode='HTML')
            return
        context.user_data['channel_username'] = text
        user_states[user_id] = ADD_CHANNEL_TITLE
        await update.message.reply_text(
            "📝 Send channel title now.",
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 CANCEL", callback_data="manage_force_sub")]])
        )
    elif state == ADD_CHANNEL_TITLE:
        await delete_update_message(update, context)
        channel_username = context.user_data.get('channel_username')
        channel_title = text
        if add_force_sub_channel(channel_username, channel_title):
            user_states.pop(user_id, None)
            context.user_data.pop('channel_username', None)
            await update.message.reply_text(
                f"✅ Channel added: {channel_title} ({channel_username})",
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📢 MANAGE CHANNELS", callback_data="manage_force_sub")]])
            )
        else:
            await update.message.reply_text("❌ Could not add. It may already exist.", parse_mode='HTML')
    elif state == GENERATE_LINK_CHANNEL_USERNAME:
        await delete_update_message(update, context)
        channel_identifier = text.strip()
        if not (channel_identifier.startswith('@') or channel_identifier.startswith('-100') or channel_identifier.lstrip('-').isdigit()):
            await update.message.reply_text(
                "❌ Invalid format. Use @username or channel ID (-100...)", parse_mode='HTML'
            )
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

    if user_id in user_states:
        current = user_states[user_id]
        if current in [PENDING_BROADCAST, GENERATE_LINK_CHANNEL_USERNAME, ADD_CHANNEL_TITLE, ADD_CHANNEL_USERNAME] and data in ["admin_back", "admin_stats", "manage_force_sub", "generate_links", "user_management"]:
            user_states.pop(user_id, None)

    if data == "close_message":
        try:
            await query.delete_message()
        except Exception as e:
            logger.warning(f"Could not delete message: {e}")
        return

    if data == "admin_broadcast_start":
        if user_id != ADMIN_ID:
            await query.edit_message_text("❌ Admin only", parse_mode='HTML')
            return
        user_states[user_id] = PENDING_BROADCAST
        try:
            await query.delete_message()
        except:
            pass
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text="📣 Send the message (text, photo, video, etc.) to broadcast now.",
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 CANCEL", callback_data="admin_back")]])
        )
        return

    if data.startswith("verify_deep_"):
        link_id = data[12:]
        return await handle_channel_link_deep(update, context, link_id)

    if data == "admin_stats":
        if user_id != ADMIN_ID:
            await query.edit_message_text("❌ Admin only", parse_mode='HTML')
            return
        await send_admin_stats(query, context)

    elif data == "user_management":
        if user_id != ADMIN_ID:
            await query.edit_message_text("❌ Admin only", parse_mode='HTML')
            return
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
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text="🔗 Send channel username or ID to generate deep link.",
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 CANCEL", callback_data="admin_back")]])
        )
    elif data == "add_channel_start":
        if user_id != ADMIN_ID:
            await query.edit_message_text("❌ Admin only", parse_mode='HTML')
            return
        user_states[user_id] = ADD_CHANNEL_USERNAME
        try:
            await query.delete_message()
        except:
            pass
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text="📢 Send @username of channel to add to force-sub list.",
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 CANCEL", callback_data="manage_force_sub")]])
        )

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

    elif data in ["admin_back", "user_back", "channels_back"]:
        if user_id == ADMIN_ID:
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

@force_sub_required
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

# --- NEW BROADCAST JOB FUNCTION (FOR THROTTLING) ---
async def broadcast_worker_job(context: ContextTypes.DEFAULT_TYPE):
    """JobQueue worker to send a message to a single chunk of users."""
    job_data = context.job.data
    offset = job_data['offset']
    chunk_size = job_data['chunk_size']
    message_chat_id = job_data['message_chat_id']
    message_id = job_data['message_id']
    is_last_chunk = job_data['is_last_chunk']
    admin_chat_id = job_data['admin_chat_id']

    # Fetch users for the specific chunk
    # Note: get_all_users now correctly uses limit and offset
    users_chunk = get_all_users(limit=chunk_size, offset=offset) 
    sent_count = 0
    fail_count = 0

    for user in users_chunk:
        target_user_id = user[0]
        try:
            # We use copy_message to ensure media/formatting is preserved correctly
            await context.bot.copy_message(
                chat_id=target_user_id,
                from_chat_id=message_chat_id,
                message_id=message_id
            )
            sent_count += 1
        except Exception as e:
            # Likely cause: user blocked the bot or chat is invalid
            logger.warning(f"Failed send to {target_user_id} (Offset: {offset}): {e}")
            fail_count += 1
        # Small delay between messages to respect Telegram's API limits (30 messages per second per bot)
        await asyncio.sleep(0.05) 

    logger.info(f"Broadcast chunk from offset {offset} finished. Sent {sent_count} messages, Failed {fail_count}.")
    
    # Send intermediate progress report to admin
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

# --- UPDATED BROADCAST SCHEDULER FUNCTION (WITH THROTTLING LOGIC) ---
async def broadcast_message_to_all_users(update: Update, context: ContextTypes.DEFAULT_TYPE, message_to_copy):
    admin_chat_id = update.effective_chat.id
    total_users = get_user_count()

    # If user count is below the minimum threshold, perform a fast broadcast
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
            await asyncio.sleep(0.05) # Still respects per-second rate limits
        await context.bot.send_message(chat_id=admin_chat_id, text=f"✅ **Broadcast Complete!**\nTotal attempted: {total_users}.\nSuccessfully sent: {sent}.", parse_mode='Markdown')
        
        # Finally, delete the admin's original message after a successful fast broadcast
        try:
            await update.message.delete()
        except:
            pass
        return

    # --- THROTTLED BROADCAST LOGIC (for > 5000 users) ---
    await update.message.reply_text(
        f"⏳ **Throttled Broadcast Started!**\n"
        f"Total users: {total_users}.\n"
        f"Sending in chunks of {BROADCAST_CHUNK_SIZE} every {BROADCAST_INTERVAL_MIN} minutes.",
        parse_mode='Markdown'
    )

    offset = 0
    current_delay = 0 # delay in seconds
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
        
        # Schedule the job for the calculated delay
        context.job_queue.run_once(
            broadcast_worker_job, 
            when=current_delay, 
            data=job_data,
            name=f"broadcast_chunk_{chunks_sent}"
        )

        offset += BROADCAST_CHUNK_SIZE
        current_delay += BROADCAST_INTERVAL_MIN * 60 # Add 20 minutes delay for next chunk
        chunks_sent += 1

    await update.message.reply_text(
        f"Scheduled **{total_chunks}** broadcast chunks, running over **{current_delay // 60} minutes**.\n"
        f"You will receive a notification after each chunk is sent.",
        parse_mode='Markdown'
    )
    
    # Finally, delete the admin's original message after a successful scheduling
    try:
        await update.message.delete()
    except:
        pass
# ----------------------------------------------------

async def send_admin_menu(chat_id, context, query=None):
    if query:
        try:
            await query.delete_message()
        except:
            pass
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
    channels = get_all_force_sub_channels()
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
        await query.delete_message()
    except:
        pass
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

async def send_user_management(query, context, offset=0):
    if query.from_user.id != ADMIN_ID:
        await query.answer("You are not authorized", show_alert=True)
        return
    total = get_user_count()
    users = get_all_users(limit=10, offset=offset)
    has_next = total > offset + 10
    has_prev = offset > 0
    text = f"👤 <b>USER MANAGEMENT</b>\n\nShowing {offset+1}-{min(offset+10, total)} of {total}\n\n"
    for (uid, username, fname, lname, joined) in users:
        name = f"{fname or ''} {lname or ''}".strip() or "N/A"
        uname = f"@{username}" if username else f"ID: {uid}"
        text += f"<b>{name}</b> (<code>{uname}</code>)\nJoined: {joined}\n\n"
    keyboard = []
    nav = []
    if has_prev:
        nav.append(InlineKeyboardButton("⬅️ PREV", callback_data=f"user_page_{offset-10}"))
    if has_next:
        nav.append(InlineKeyboardButton("NEXT ➡️", callback_data=f"user_page_{offset+10}"))
    if nav:
        keyboard.append(nav)
    keyboard.append([InlineKeyboardButton("🔙 BACK", callback_data="admin_back")])
    await query.edit_message_text(text=text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))


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
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(button_handler))
    admin_filter = filters.User(user_id=ADMIN_ID)
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
