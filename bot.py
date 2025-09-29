import os
import logging
import sqlite3
import secrets
import re
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, MessageHandler, filters
import asyncio
import os 

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Bot configuration (UPDATED with your new token)
BOT_TOKEN = '7877393813:AAEqVD-Ar6M4O3yg6h2ZuNUN_PPY4NRVr10'
ADMIN_ID = 829342319 # Replace with your actual Telegram User ID
LINK_EXPIRY_MINUTES = 5  # Links expire after 5 minutes

# Global variables for webhook configuration
PORT = int(os.environ.get('PORT', 8080))
# RENDER_EXTERNAL_URL is set automatically by Render
WEBHOOK_URL = os.environ.get('RENDER_EXTERNAL_URL', '').rstrip('/') + '/'

# =================================================================
# ⚙️ CUSTOMIZATION CONSTANTS 
# =================================================================

# Channel ID for the welcome message source (The private channel where you copied the welcome post from)
WELCOME_SOURCE_CHANNEL = -1002530952988
# Message ID of the welcome post inside that channel
WELCOME_SOURCE_MESSAGE_ID = 32  

PUBLIC_ANIME_CHANNEL_URL = "https://t.me/BeatAnime"
REQUEST_CHANNEL_URL = "https://t.me/Beat_Hindi_Dubbed"

ADMIN_CONTACT_USERNAME = "Beat_Anime_Ocean" 

# =================================================================

# User states for conversation
ADD_CHANNEL_USERNAME, ADD_CHANNEL_TITLE, GENERATE_LINK_CHANNEL_USERNAME, PENDING_BROADCAST = range(4)
user_states = {}

# --- CRITICAL FIX: MarkdownV2 Escaping Function ---
def escape_markdown_v2(text):
    """Helper function to escape characters reserved in MarkdownV2."""
    # List of characters to escape: _, *, [, ], (, ), ~, `, >, #, +, -, =, |, {, }, ., !
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    # Escape '\' itself first, then escape other characters
    text = text.replace('\\', '\\\\')
    return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', text)

# --- CLEAN DATABASE INITIALIZATION (FINAL VERSION) ---
def init_db():
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    
    # Users table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            joined_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Force subscription channels table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS force_sub_channels (
            channel_id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_username TEXT UNIQUE,
            channel_title TEXT,
            added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_active BOOLEAN DEFAULT 1
        )
    ''')
    
    # Generated links table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS generated_links (
            link_id TEXT PRIMARY KEY,
            channel_username TEXT,
            user_id INTEGER,
            created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_used BOOLEAN DEFAULT 0
        )
    ''')
    
    conn.commit()
    conn.close()

# --- END DATABASE INITIALIZATION ---


def add_user(user_id, username, first_name, last_name):
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    cursor.execute('''
        INSERT OR REPLACE INTO users (user_id, username, first_name, last_name)
        VALUES (?, ?, ?, ?)
    ''', (user_id, username, first_name, last_name))
    conn.commit()
    conn.close()

def get_all_users(limit=20, offset=0):
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
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

def get_force_sub_channel_count():
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM force_sub_channels WHERE is_active = 1')
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
        return cursor.rowcount > 0 # Return true if a new row was inserted
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
        INSERT INTO generated_links (link_id, channel_username, user_id)
        VALUES (?, ?, ?)
    ''', (link_id, channel_username, user_id))
    conn.commit()
    conn.close()
    return link_id

def get_link_info(link_id):
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    cursor.execute('''
        SELECT channel_username, user_id, created_time, is_used 
        FROM generated_links WHERE link_id = ?
    ''', (link_id,))
    result = cursor.fetchone()
    conn.close()
    return result

def mark_link_used(link_id):
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    cursor.execute('UPDATE generated_links SET is_used = 1 WHERE link_id = ?', (link_id,))
    conn.commit()
    conn.close()

def cleanup_expired_links():
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    expiry_time = datetime.now() - timedelta(minutes=LINK_EXPIRY_MINUTES)
    cursor.execute('DELETE FROM generated_links WHERE created_time < ?', (expiry_time,))
    conn.commit()
    conn.close()

async def check_force_subscription(user_id, context):
    channels = get_all_force_sub_channels()
    not_joined_channels = []
    
    for channel_username, channel_title in channels:
        try:
            member = await context.bot.get_chat_member(channel_username, user_id)
            if member.status in ['left', 'kicked']:
                not_joined_channels.append((channel_username, channel_title))
        except Exception as e:
            logger.error(f"Error checking subscription for {channel_username}: {e}")
    
    return not_joined_channels

def is_admin(user_id):
    return user_id == ADMIN_ID

# --- HELPER FUNCTIONS FOR ADMIN MENU NAVIGATION ---

# FIX: Added query=None and message deletion logic
async def send_admin_menu(chat_id, context, query=None):
    """Sends the admin main menu as a new message."""
    
    if query:
        try:
            await query.delete_message()
        except Exception:
            pass
            
    keyboard = [
        [InlineKeyboardButton("📊 BOT STATS", callback_data="admin_stats")],
        [InlineKeyboardButton("📺 MANAGE FORCE SUB CHANNELS", callback_data="manage_force_sub")],
        [InlineKeyboardButton("🔗 GENERATE CHANNEL LINKS", callback_data="generate_links")],
        [InlineKeyboardButton("📢 START MEDIA BROADCAST", callback_data="admin_broadcast_start")],
        [InlineKeyboardButton("👥 USER MANAGEMENT", callback_data="user_management")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    # Using raw f-string to ensure correct MarkdownV2 escaping
    text = r"👑 **ADMIN PANEL** 👑\n\nWelcome back, Admin\! Choose an option below\:"
    
    await context.bot.send_message(
        chat_id=chat_id,
        text=text,
        parse_mode='MarkdownV2',
        reply_markup=reply_markup
    )

# FIX: Added message deletion logic
async def send_admin_stats(query, context):
    """Calculates and sends the bot stats as a new message."""
    
    try:
        await query.delete_message()
    except Exception:
        pass
        
    user_count = get_user_count()
    channel_count = get_force_sub_channel_count()
    
    # Escape all dynamic values that might contain special characters
    safe_user_count = escape_markdown_v2(str(user_count))
    safe_channel_count = escape_markdown_v2(str(channel_count))
    safe_expiry = escape_markdown_v2(str(LINK_EXPIRY_MINUTES))
    safe_datetime = escape_markdown_v2(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    
    # Using raw f-string to ensure correct MarkdownV2 escaping
    stats_text = rf"""
📊 **BOT STATISTICS** 📊

👥 **Total Users:** {safe_user_count}
📺 **Force Sub Channels:** {safe_channel_count}
🔗 **Link Expiry:** {safe_expiry} minutes

*Last Cleanup:* {safe_datetime}
    """
    
    keyboard = [[InlineKeyboardButton("🔄 REFRESH", callback_data="admin_stats")],
                [InlineKeyboardButton("🔙 BACK", callback_data="admin_back")]]
    
    await context.bot.send_message(
        chat_id=query.message.chat_id, 
        text=stats_text, 
        parse_mode='MarkdownV2', 
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def show_force_sub_management(query, context):
    """Displays the list of force sub channels with options to add/delete."""
    channels = get_all_force_sub_channels()
    
    channels_text = "📺 **FORCE SUBSCRIPTION CHANNELS** 📺\n\n"
    
    if not channels:
        channels_text += r"No channels configured currently\."
    else:
        channels_text += r"Configured Channels:\n"
        for channel_username, channel_title in channels:
            safe_title = escape_markdown_v2(channel_title)
            safe_username = escape_markdown_v2(channel_username)
            channels_text += rf"• {safe_title} (`{safe_username}`)\n"

    keyboard = [
        [InlineKeyboardButton("➕ ADD NEW CHANNEL", callback_data="add_channel_start")]
    ]
    
    # If channels exist, add specific management buttons
    if channels:
        # Corrected List Comprehension: Unpacks channel_username and channel_title directly
        channel_buttons = [
            InlineKeyboardButton(channel_title, callback_data=f"channel_{channel_username}") 
            for channel_username, channel_title in channels
        ]
        
        # Group channel buttons into rows of 2 for better display
        grouped_buttons = [channel_buttons[i:i + 2] for i in range(0, len(channel_buttons), 2)]
        
        # Insert the channel buttons 
        for row in grouped_buttons:
            keyboard.append(row)

        # Now add the delete button, since we have channels
        keyboard.append([InlineKeyboardButton("🗑️ DELETE CHANNEL", callback_data="delete_channel_prompt")])
        keyboard.append([InlineKeyboardButton("ℹ️ CHANNEL DETAILS", callback_data="channel_details_prompt")]) # Kept for backward compatibility, though not used in full flow

    keyboard.append([InlineKeyboardButton("🔙 BACK TO MENU", callback_data="admin_back")])
    
    # FIX: Delete and Send New Message
    try:
        await query.delete_message()
    except Exception:
        pass
        
    await context.bot.send_message(
        chat_id=query.message.chat_id,
        text=channels_text,
        parse_mode='MarkdownV2',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def show_channel_details(query, context, channel_username):
    """Displays detailed options for a specific force sub channel."""
    channel_info = get_force_sub_channel_info(channel_username)
    
    if not channel_info:
        await query.edit_message_text(r"❌ Channel not found\.", parse_mode='MarkdownV2', reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 MANAGE CHANNELS", callback_data="manage_force_sub")]]))
        return
        
    channel_username, channel_title = channel_info
    
    safe_title = escape_markdown_v2(channel_title)
    safe_username = escape_markdown_v2(channel_username)
    
    details_text = rf"""
📺 **CHANNEL DETAILS** 📺

**Title:** {safe_title}
**Username:** {safe_username}
**Status:** *Active Force Sub*

_Choose an action below\._
    """
    
    keyboard = [
        [InlineKeyboardButton("🔗 GENERATE TEMP LINK", callback_data=f"genlink_{channel_username}")],
        [InlineKeyboardButton("🗑️ DELETE CHANNEL", callback_data=f"delete_{channel_username}")],
        [InlineKeyboardButton("🔙 BACK TO MANAGEMENT", callback_data="manage_force_sub")]
    ]
    
    await query.edit_message_text(
        text=details_text,
        parse_mode='MarkdownV2',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

# FIX: Added message deletion logic for initial click
async def send_user_management(query, context, offset=0):
    """Displays a paginated list of users."""
    
    # NEW FIX: Delete old message before sending new one, ONLY on initial click
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
    for user_id, username, first_name, last_name, joined_date in users:
        display_name = f"{first_name or ''} {last_name or ''}".strip() or "N/A"
        display_username = f"@{username}" if username else f"ID: {user_id}"
        
        # Escape user-provided data before insertion
        safe_display_name = escape_markdown_v2(display_name)
        safe_display_username = escape_markdown_v2(display_username)
        safe_joined = escape_markdown_v2(datetime.fromisoformat(joined_date).strftime('%Y-%m-%d %H:%M'))
        
        user_list_text += f"**{safe_display_name}** (`{safe_display_username}`)\n"
        user_list_text += f"Joined\\: {safe_joined}\n\n"
    
    if not user_list_text:
        user_list_text = r"No users found in the database\."

    # Escape all numeric values
    safe_user_count = escape_markdown_v2(str(user_count))
    safe_start = escape_markdown_v2(str(offset + 1))
    safe_end = escape_markdown_v2(str(min(offset + 10, user_count)))
    
    # Build message with ALL colons properly escaped
    stats_text = (
        "👥 **USER MANAGEMENT** 👥\n\n" +
        f"**Total Users\\:** {safe_user_count}\n" +
        f"**Showing\\:** {safe_start}\\-{safe_end} of {safe_user_count}\n\n" +
        user_list_text
    )
    
    # Build keyboard
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
    
    # Check if this is a refresh/pagination update or initial click
    if query.data.startswith("user_page_"):
        await query.edit_message_text(
            text=stats_text,
            parse_mode='MarkdownV2',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    else:
         # Initial click from Admin Menu (Now deletion is handled above)
        await context.bot.send_message(
            chat_id=query.message.chat_id, 
            text=stats_text, 
            parse_mode='MarkdownV2', 
            reply_markup=InlineKeyboardMarkup(keyboard)
        )


# --- END HELPER FUNCTIONS ---


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    add_user(user.id, user.username, user.first_name, user.last_name)
    
    if context.args and len(context.args) > 0:
        link_id = context.args[0]
        await handle_channel_link_deep(update, context, link_id)
        return
    
    # --- Force Subscription Check ---
    if not is_admin(user.id):
        not_joined_channels = await check_force_subscription(user.id, context)
        
        if not_joined_channels:
            keyboard = []
            for channel_username, channel_title in not_joined_channels:
                # Use escaped channel title for button text if needed, but not strictly required
                keyboard.append([InlineKeyboardButton(f"📢 JOIN {channel_title}", url=f"https://t.me/{channel_username[1:]}")])
            
            keyboard.append([InlineKeyboardButton("✅ VERIFY SUBSCRIPTION", callback_data="verify_subscription")])
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            # Escape channel info for the message body
            channels_text = "\n".join([f"• {escape_markdown_v2(title)} (`{escape_markdown_v2(username)}`)" for username, title in not_joined_channels])
            
            # Using raw f-string to ensure correct MarkdownV2 escaping
            await update.message.reply_text(
                rf"📢 **Please join our channels to use this bot\!**\n\n"
                rf"**Required Channels:**\n{channels_text}\n\n"
                r"Join all channels above and then click Verify Subscription\.",
                parse_mode='MarkdownV2',
                reply_markup=reply_markup
            )
            return
    
    # --- Main Menu Display ---
    if is_admin(user.id):
        await send_admin_menu(update.effective_chat.id, context)
    else:
        # DYNAMIC WELCOME MESSAGE LOGIC with 2-COLUMN LAYOUT
        keyboard = [
            [
                InlineKeyboardButton("ANIME CHANNEL", url=PUBLIC_ANIME_CHANNEL_URL),
                InlineKeyboardButton("REQUEST ANIME CHANNEL", url=REQUEST_CHANNEL_URL)
            ],
            [
                InlineKeyboardButton("CONTACT ADMIN", url=f"https://t.me/{ADMIN_CONTACT_USERNAME}"),
                InlineKeyboardButton("ABOUT ME", callback_data="about_bot")
            ],
            [
                InlineKeyboardButton("CLOSE", callback_data="close_message")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        try:
            # Copy the entire message (media + text) from the source channel
            await context.bot.copy_message(
                chat_id=update.effective_chat.id,
                from_chat_id=WELCOME_SOURCE_CHANNEL,
                message_id=WELCOME_SOURCE_MESSAGE_ID,
                reply_markup=reply_markup # Attach the buttons to the copied message
            )
        except Exception as e:
            logger.error(f"Error copying welcome message from channel: {e}")
            # Fallback text MUST use careful MarkdownV2 escaping
            fallback_text = r"👋 *WELCOME TO THE ADVANCED LINKS SHARING BOT\.*\n\nUSE THIS BOT TO SAFELY SHARE CONTENT WITHOUT RISKING COPYRIGHT TAKEDOWNS\.\nEXPLORE THE OPTIONS BELOW TO GET STARTED\!"
            await update.message.reply_text(fallback_text, parse_mode='MarkdownV2', reply_markup=reply_markup)


async def handle_channel_link_deep(update: Update, context: ContextTypes.DEFAULT_TYPE, link_id):
    link_info = get_link_info(link_id)
    
    if not link_info:
        await update.message.reply_text(r"❌ This link has expired or is invalid\.", parse_mode='MarkdownV2')
        return
    
    channel_username, creator_id, created_time, is_used = link_info
    
    if is_used:
        await update.message.reply_text(r"❌ This link has already been used\.", parse_mode='MarkdownV2')
        return
    
    link_age = datetime.now() - datetime.fromisoformat(created_time)
    if link_age.total_seconds() > LINK_EXPIRY_MINUTES * 60:
        await update.message.reply_text(r"❌ This link has expired\.", parse_mode='MarkdownV2')
        return
    
    user = update.effective_user
    
    not_joined_channels = await check_force_subscription(user.id, context)
    if not_joined_channels:
        keyboard = []
        for chan_user, chan_title in not_joined_channels:
            keyboard.append([InlineKeyboardButton(f"📢 JOIN {chan_title}", url=f"https://t.me/{chan_user[1:]}")])
        
        keyboard.append([InlineKeyboardButton("✅ VERIFY SUBSCRIPTION", callback_data=f"verify_deep_{link_id}")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        channels_text = "\n".join([f"• {escape_markdown_v2(title)}" for _, title in not_joined_channels])
        
        # Using raw f-string to ensure correct MarkdownV2 escaping
        await update.message.reply_text(
            rf"📢 **Please join our channels to get access\!**\n\n"
            rf"**Required Channels:**\n{channels_text}\n\n"
            r"Join all channels above and then click Verify Subscription\.",
            parse_mode='MarkdownV2',
            reply_markup=reply_markup
        )
        return
    
    try:
        chat = await context.bot.get_chat(channel_username)
        invite_link = await context.bot.create_chat_invite_link(
            chat.id, 
            member_limit=1,
            expire_date=datetime.now().timestamp() + 300
        )
        
        mark_link_used(link_id)
        
        # Escape all dynamic values
        safe_chat_title = escape_markdown_v2(chat.title)
        safe_expiry = escape_markdown_v2(str(LINK_EXPIRY_MINUTES))
        
        # MarkdownV2 success message (using raw f-string)
        success_message = (
            rf"🎉 *Access Granted\!* 🎉\n\n"
            rf"*Channel:* {safe_chat_title}\n"
            rf"*Expires in:* {safe_expiry} minutes\n"
            rf"*Usage:* Single use\n\n"
            r"_Enjoy the content\! 🍿_"
        )
        
        # FIX: Use the invite link in a button
        keyboard = [[InlineKeyboardButton("🔓 Request to Join", url=invite_link.invite_link)]]
        
        await update.message.reply_text(
            success_message,
            parse_mode='MarkdownV2',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
    except Exception as e:
        logger.error(f"Error generating invite link for {channel_username}: {e}")
        await update.message.reply_text(r"❌ Error generating access link\. Make sure the bot is an *Admin* in the target channel and has the right to create invite links\.", parse_mode='MarkdownV2')

async def broadcast_message_to_all_users(update: Update, context: ContextTypes.DEFAULT_TYPE, message_to_copy):
    """Internal function to handle the actual broadcast by copying the message."""
    users = get_all_users(limit=None, offset=0) # Get all users without limit/offset
    success_count = 0
    total_users = len(users)
    
    # Send a confirmation message immediately (ensure total_users is safe)
    safe_total_users = escape_markdown_v2(str(total_users))
    await update.message.reply_text(rf"🚀 Starting broadcast to {safe_total_users} users\. Please wait\.", parse_mode='MarkdownV2')

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
    
    # Final confirmation message (ensure counts are safe)
    safe_success_count = escape_markdown_v2(str(success_count))
    safe_total_users = escape_markdown_v2(str(total_users))
    
    # Using raw f-string to ensure correct MarkdownV2 escaping
    await context.bot.send_message(
        chat_id=update.effective_chat.id, 
        text=rf"✅ **Broadcast complete\!**\n\n📊 Sent to {safe_success_count}/{safe_total_users} users\.",
        parse_mode='MarkdownV2'
    )

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    data = query.data
    
    # Admin state cleanup
    if user_id in user_states:
        current_state = user_states.get(user_id)
        if current_state in [PENDING_BROADCAST, GENERATE_LINK_CHANNEL_USERNAME, ADD_CHANNEL_TITLE, ADD_CHANNEL_USERNAME] and data in ["admin_back", "admin_stats", "manage_force_sub", "generate_links", "user_management"]:
            del user_states[user_id]
            
    if data == "close_message":
        try:
            await query.delete_message()
        except Exception as e:
            logger.warning(f"Could not delete message on 'close_message': {e}")
        return

    # --- ADMIN BROADCAST START ---
    if data == "admin_broadcast_start":
        if not is_admin(user_id):
            await query.edit_message_text(r"❌ Admin only\.", parse_mode='MarkdownV2')
            return
        
        user_states[user_id] = PENDING_BROADCAST
        
        keyboard = [[InlineKeyboardButton("🔙 CANCEL", callback_data="admin_back")]]
        
        # FIX: Delete old message and send a new one
        try:
            await query.delete_message()
        except Exception:
            pass
            
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text=r"📢 **MEDIA BROADCAST MODE**" + "\n\n" +
                 r"Please **forward** the message \(image, video, file, sticker, or text with stylish caption\) you wish to broadcast *now*\." + "\n\n" +
                 r"**Note:** Any message you send next will be copied to all users\.",
            parse_mode='MarkdownV2',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return
    
    if data == "verify_subscription":
        not_joined_channels = await check_force_subscription(user_id, context)
        
        if not_joined_channels:
            channels_text = "\n".join([f"• {escape_markdown_v2(title)}" for _, title in not_joined_channels])
            # Using raw f-string to ensure correct MarkdownV2 escaping
            await query.edit_message_text(
                rf"❌ **You haven't joined all required channels\!**\n\n"
                rf"**Still missing:**\n{channels_text}\n\n"
                r"Please join all channels and try again\.",
                parse_mode='MarkdownV2'
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
                [
                    InlineKeyboardButton("ANIME CHANNEL", url=PUBLIC_ANIME_CHANNEL_URL),
                    InlineKeyboardButton("REQUEST ANIME CHANNEL", url=REQUEST_CHANNEL_URL)
                ],
                [
                    InlineKeyboardButton("CONTACT ADMIN", url=f"https://t.me/{ADMIN_CONTACT_USERNAME}"),
                    InlineKeyboardButton("ABOUT ME", callback_data="about_bot")
                ],
                [
                    InlineKeyboardButton("CLOSE", callback_data="close_message")
                ]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            try:
                await query.delete_message() # Delete the verification message
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
                logger.error(f"Error copying verified welcome message: {e}")
                # Using raw f-string to ensure correct MarkdownV2 escaping
                fallback_text = r"✅ **Subscription verified\!**\n\nWelcome to the bot\! Explore the options below\:"
                await context.bot.send_message(query.message.chat_id, fallback_text, parse_mode='MarkdownV2', reply_markup=reply_markup)
        
        
    elif data.startswith("verify_deep_"):
        link_id = data[12:]
        not_joined_channels = await check_force_subscription(user_id, context)
        
        if not_joined_channels:
            channels_text = "\n".join([f"• {escape_markdown_v2(title)}" for _, title in not_joined_channels])
            # Using raw f-string to ensure correct MarkdownV2 escaping
            await query.edit_message_text(
                rf"❌ **You haven't joined all required channels\!**\n\n"
                rf"**Still missing:**\n{channels_text}\n\n"
                r"Please join all channels and try again\.",
                parse_mode='MarkdownV2'
            )
            return
        
        link_info = get_link_info(link_id)
        if not link_info:
            await query.edit_message_text(r"❌ Link expired or invalid\.", parse_mode='MarkdownV2')
            return
        
        channel_username = link_info[0]
        
        try:
            chat = await context.bot.get_chat(channel_username)
            invite_link = await context.bot.create_chat_invite_link(
                chat.id, 
                member_limit=1,
                expire_date=datetime.now().timestamp() + 300
            )
            
            mark_link_used(link_id)
            
            # Escape all dynamic values
            safe_chat_title = escape_markdown_v2(chat.title)
            safe_expiry = escape_markdown_v2(str(LINK_EXPIRY_MINUTES))
            
            # MarkdownV2 success message (using raw f-string)
            success_message = (
                rf"🎉 *Access Granted\!* 🎉\n\n"
                rf"*Channel:* {safe_chat_title}\n"
                rf"*Expires in:* {safe_expiry} minutes\n"
                rf"*Usage:* Single use\n\n"
                r"_Enjoy the content\! 🍿_"
            )
            
            # FIX: Use the link to generate a button
            keyboard = [[InlineKeyboardButton("🔓 Request to Join", url=invite_link.invite_link)]]

            try:
                await query.delete_message() 
            except Exception:
                pass
                
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=success_message,
                parse_mode='MarkdownV2',
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            
        except Exception as e:
            logger.error(f"Error verifying deep link: {e}")
            await query.edit_message_text(r"❌ Error generating access link\. Make sure the bot is an *Admin* in the target channel and has the right to create invite links\.", parse_mode='MarkdownV2')
    
    # --- BOT STATS ---
    elif data == "admin_stats":
        if not is_admin(user_id):
            await query.edit_message_text(r"❌ Admin only\.", parse_mode='MarkdownV2')
            return
        
        await send_admin_stats(query, context)
        return
    
    # --- USER MANAGEMENT (Initial click/Refresh) ---
    elif data == "user_management":
        if not is_admin(user_id):
            await query.edit_message_text(r"❌ Admin only\.", parse_mode='MarkdownV2')
            return
        
        await send_user_management(query, context, offset=0)
        return
    
    # --- USER MANAGEMENT (Pagination) ---
    elif data.startswith("user_page_"):
        if not is_admin(user_id):
            await query.edit_message_text(r"❌ Admin only\.", parse_mode='MarkdownV2')
            return
        
        try:
            offset = int(data[10:])
        except ValueError:
            offset = 0
            
        await send_user_management(query, context, offset=offset)
        return
    
    # --- MANAGE FORCE SUB CHANNELS ---
    elif data == "manage_force_sub":
        if not is_admin(user_id):
            await query.edit_message_text(r"❌ Admin only\.", parse_mode='MarkdownV2')
            return
        await show_force_sub_management(query, context)
    
    # --- GENERATE LINKS FLOW START ---
    elif data == "generate_links":
        if not is_admin(user_id):
            await query.edit_message_text(r"❌ Admin only\.", parse_mode='MarkdownV2')
            return
        
        user_states[user_id] = GENERATE_LINK_CHANNEL_USERNAME
        
        keyboard = [[InlineKeyboardButton("🔙 CANCEL", callback_data="admin_back")]]
        
        # FIX: Delete and Send New Message
        try:
            await query.delete_message()
        except Exception:
            pass
            
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text=r"🔗 **GENERATE CHANNEL LINKS**" + "\n\n" +
                 r"Please send the **username** \(starting with @\) of the channel " +
                 r"you want to generate a one\-time, expirable link for\.",
            parse_mode='MarkdownV2',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    # --- ADD CHANNEL FLOW START ---
    elif data == "add_channel_start":
        if not is_admin(user_id):
            await query.edit_message_text(r"❌ Admin only\.", parse_mode='MarkdownV2')
            return
        
        user_states[user_id] = ADD_CHANNEL_USERNAME
        
        # FIX: Delete and Send New Message
        try:
            await query.delete_message()
        except Exception:
            pass
            
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text=r"📺 **ADD FORCE SUBSCRIPTION CHANNEL**" + "\n\n" +
                 r"Please send me the channel username \(starting with @\)\:" + "\n\n" +
                 r"Example\: `@Beat_Anime_Ocean`",
            parse_mode='MarkdownV2',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 CANCEL", callback_data="manage_force_sub")]])
        )
    
    elif data.startswith("channel_"):
        if not is_admin(user_id):
            await query.edit_message_text(r"❌ Admin only\.", parse_mode='MarkdownV2')
            return
        await show_channel_details(query, context, data[8:])
    
    elif data.startswith("delete_"):
        if not is_admin(user_id):
            await query.edit_message_text(r"❌ Admin only\.", parse_mode='MarkdownV2')
            return
        channel_username = data[7:]
        channel_info = get_force_sub_channel_info(channel_username)
        
        if channel_info:
            keyboard = [
                [InlineKeyboardButton("✅ YES, DELETE", callback_data=f"confirm_delete_{channel_username}")],
                [InlineKeyboardButton("❌ NO, CANCEL", callback_data=f"channel_{channel_username}")]
            ]
            # Escape channel info for the message body
            safe_channel_title = escape_markdown_v2(channel_info[1])
            safe_channel_username = escape_markdown_v2(channel_info[0])
            
            # Using raw f-string to ensure correct MarkdownV2 escaping
            await query.edit_message_text(
                rf"🗑️ **CONFIRM DELETION**\n\n"
                rf"Are you sure you want to delete this force sub channel\?\n\n"
                rf"**Channel:** {safe_channel_title}\n"
                rf"**Username:** {safe_channel_username}\n\n"
                r"This action cannot be undone\!",
                parse_mode='MarkdownV2',
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
    
    elif data.startswith("confirm_delete_"):
        if not is_admin(user_id):
            await query.edit_message_text(r"❌ Admin only\.", parse_mode='MarkdownV2')
            return
        channel_username = data[15:]
        delete_force_sub_channel(channel_username)
        
        safe_channel_username = escape_markdown_v2(channel_username)
        
        # Using raw f-string to ensure correct MarkdownV2 escaping
        await query.edit_message_text(
            rf"✅ **CHANNEL DELETED**\n\n"
            rf"Force sub channel `{safe_channel_username}` has been deleted successfully\.",
            parse_mode='MarkdownV2',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📺 MANAGE CHANNELS", callback_data="manage_force_sub")]])
        )
    
    # --- BACK BUTTONS ---
    elif data in ["admin_back", "user_back", "channels_back"]:
        if is_admin(user_id):
            await send_admin_menu(query.message.chat_id, context, query)
        else:
            keyboard = [
                [
                    InlineKeyboardButton("ANIME CHANNEL", url=PUBLIC_ANIME_CHANNEL_URL),
                    InlineKeyboardButton("REQUEST ANIME CHANNEL", url=REQUEST_CHANNEL_URL)
                ],
                [
                    InlineKeyboardButton("CONTACT ADMIN", url=f"https://t.me/{ADMIN_CONTACT_USERNAME}"),
                    InlineKeyboardButton("ABOUT ME", callback_data="about_bot")
                ],
                [
                    InlineKeyboardButton("CLOSE", callback_data="close_message")
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
                fallback_text = r"🌟 **MAIN MENU** 🌟\n\nChoose an option\:"
                await context.bot.send_message(query.message.chat_id, fallback_text, parse_mode='MarkdownV2', reply_markup=reply_markup)

    
    elif data == "about_bot":
        # Using raw string (r""") to prevent Python SyntaxWarning and ensure MarkdownV2 escapes are correct
        about_me_text = r"""
*About Us\.*

▣**Made for: @Beat\_Anime\_Ocean**
▣**Owned by: @Beat\_Anime\_Ocean**
▣**Developer: @Beat\_Anime\_Ocean**

_Adios \!\!_
"""
        keyboard = [[InlineKeyboardButton("🔙 BACK", callback_data="user_back")]] 
        
        try:
            await query.delete_message()
        except Exception:
            logger.warning("Could not delete message during 'about_bot' switch, proceeding to send new message.")

        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text=about_me_text,
            parse_mode='MarkdownV2',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

async def handle_admin_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    if user_id not in user_states:
        return 

    state = user_states[user_id]
    
    # Handle incoming media/text for broadcast first
    if state == PENDING_BROADCAST:
        if user_id in user_states:
            del user_states[user_id] # Clear state
            await broadcast_message_to_all_users(update, context, update.message)
            await send_admin_menu(update.effective_chat.id, context)
            return
            
    # Handle text inputs for other flows
    text = update.message.text
    if text is None:
        await update.message.reply_text(r"❌ Please send a text message as requested (e.g., a username or title)\.", parse_mode='MarkdownV2')
        return

    if state == ADD_CHANNEL_USERNAME:
        if not text.startswith('@'):
            await update.message.reply_text(r"❌ Please provide a valid channel username starting with @\. Try again\:", parse_mode='MarkdownV2')
            return
        
        context.user_data['channel_username'] = text
        user_states[user_id] = ADD_CHANNEL_TITLE
        
        await update.message.reply_text(
            r"📝 **STEP 2\: Channel Title**" + "\n\n" +
            r"Now please send me the display title for this channel\:" + "\n\n" +
            r"Example\: `Anime Ocean Channel`",
            parse_mode='MarkdownV2',
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
            
            # Escape channel info for the final message
            safe_channel_username = escape_markdown_v2(channel_username)
            safe_channel_title = escape_markdown_v2(channel_title)
            
            # Using raw f-string to ensure correct MarkdownV2 escaping
            await update.message.reply_text(
                rf"✅ **FORCE SUB CHANNEL ADDED SUCCESSFULLY\!**\n\n"
                rf"**Username:** {safe_channel_username}\n"
                rf"**Title:** {safe_channel_title}\n\n"
                r"Channel has been added to force subscription list\!",
                parse_mode='MarkdownV2',
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📺 MANAGE CHANNELS", callback_data="manage_force_sub")]])
            )
        else:
            await update.message.reply_text(r"❌ Error adding channel\. It might already exist or there was a database error\.", parse_mode='MarkdownV2')
            
    elif state == GENERATE_LINK_CHANNEL_USERNAME:
        channel_username = text.strip()
        
        if not channel_username.startswith('@'):
            await update.message.reply_text(r"❌ Please provide a valid channel username starting with @\. Try again\:", parse_mode='MarkdownV2')
            return
            
        if user_id in user_states:
            del user_states[user_id]
        
        link_id = generate_link_id(channel_username, user_id)
        bot_username = context.bot.username
        
        # Escape channel username for display
        safe_channel_username = escape_markdown_v2(channel_username)
        deep_link = f"https://t.me/{bot_username}?start={link_id}"
        
        # **CRITICAL FIX:** Escape the deep_link because link_id contains MarkdownV2 reserved characters
        safe_deep_link = escape_markdown_v2(deep_link)
        safe_expiry = escape_markdown_v2(str(LINK_EXPIRY_MINUTES))
        
        # Using raw f-string to ensure correct MarkdownV2 escaping
        await update.message.reply_text(
            rf"🔗 **LINK GENERATED** 🔗\n\n"
            rf"**Channel:** {safe_channel_username}\n"
            rf"**Expires in:** {safe_expiry} minutes\n\n"
            rf"**Direct Link:**\n`{safe_deep_link}`\n\n"
            r"Share this link with users\!",
            parse_mode='MarkdownV2',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 BACK TO MENU", callback_data="admin_back")]])
        )

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Exception while handling an update: {context.error}")

async def cleanup_task(context: ContextTypes.DEFAULT_TYPE):
    cleanup_expired_links()

def main():
    init_db()
    
    # Check if BOT_TOKEN is set
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN is missing. Please update it in bot.py.")
        return

    application = Application.builder().token(BOT_TOKEN).build()
    
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(button_handler))
    
    admin_filter = filters.User(user_id=ADMIN_ID)
    application.add_handler(MessageHandler(admin_filter & ~filters.COMMAND, handle_admin_message))
    
    application.add_error_handler(error_handler)
    
    job_queue = application.job_queue
    if job_queue: 
        job_queue.run_repeating(cleanup_task, interval=600, first=10)
    else:
        logger.warning("JobQueue is not available. Background cleanup task for expired links is disabled. Ensure 'python-telegram-bot[job-queue]' is installed.")

    if WEBHOOK_URL and BOT_TOKEN:
        print(f"🤖 Starting Webhook listener on port {PORT}. Webhook URL: {WEBHOOK_URL + BOT_TOKEN}")
        application.run_webhook(
            listen="0.0.0.0",
            port=PORT,
            url_path=BOT_TOKEN,
            webhook_url=WEBHOOK_URL + BOT_TOKEN
        )
    else:
        print("🤖 RENDER_EXTERNAL_URL not found. Starting in Polling Mode...")
        application.run_polling()

if __name__ == '__main__':
    if 'PORT' not in os.environ:
        os.environ['PORT'] = str(8080)
    
    main()
