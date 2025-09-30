import os
import logging
import sqlite3
import secrets
import re
import requests
import time
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, MessageHandler, filters
import asyncio
from threading import Thread

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Bot configuration
BOT_TOKEN = '7877393813:AAEqVD-Ar6M4O3yg6h2ZuNUN_PPY4NRVr10'
ADMIN_ID = 829342319
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
INIT, ADMIN_MENU, GENERATE_LINK_CHANNEL_USERNAME, BROADCAST_MESSAGE, FORCE_SUB_ADD_USERNAME, FORCE_SUB_ADD_TITLE, FORCE_SUB_REMOVE = range(7)
user_states = {}
user_broadcast_text = {} # Stores text for broadcast
# Database functions
def init_db():
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            is_active BOOLEAN
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS force_subscribe_channels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE,
            title TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS deep_links (
            link_id TEXT PRIMARY KEY,
            channel_identifier TEXT,
            creator_id INTEGER,
            expiry_time TIMESTAMP,
            is_used BOOLEAN
        )
    """)
    conn.commit()
    conn.close()

def add_user(user_id):
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    cursor.execute("INSERT OR IGNORE INTO users VALUES (?, ?)", (user_id, True))
    conn.commit()
    conn.close()

def get_all_active_users():
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    cursor.execute("SELECT user_id FROM users WHERE is_active = ?", (True,))
    users = [row[0] for row in cursor.fetchall()]
    conn.close()
    return users

# Link generation functions
def generate_link_id(channel_identifier, creator_id):
    link_id = secrets.token_urlsafe(16)
    expiry_time = datetime.now() + timedelta(minutes=LINK_EXPIRY_MINUTES)
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO deep_links (link_id, channel_identifier, creator_id, expiry_time, is_used) VALUES (?, ?, ?, ?, ?)",
        (link_id, channel_identifier, creator_id, expiry_time.isoformat(), False)
    )
    conn.commit()
    conn.close()
    return link_id

def get_link_data(link_id):
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    cursor.execute("SELECT channel_identifier, expiry_time, is_used FROM deep_links WHERE link_id = ?", (link_id,))
    data = cursor.fetchone()
    conn.close()
    if data:
        channel_id, expiry_time_str, is_used = data
        expiry_time = datetime.fromisoformat(expiry_time_str)
        return channel_id, expiry_time, is_used
    return None, None, None

def mark_link_used(link_id):
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    cursor.execute("UPDATE deep_links SET is_used = ? WHERE link_id = ?", (True, link_id))
    conn.commit()
    conn.close()

def cleanup_deep_links():
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    # Delete links that have expired and been used or are very old and unused
    cutoff_time = datetime.now() - timedelta(days=7) # Delete unused links older than 7 days
    cursor.execute("DELETE FROM deep_links WHERE is_used = ? AND expiry_time < ?", (True, datetime.now().isoformat()))
    cursor.execute("DELETE FROM deep_links WHERE is_used = ? AND expiry_time < ?", (False, cutoff_time.isoformat()))
    conn.commit()
    conn.close()

# Force Subscribe functions
def add_force_sub_channel(username, title):
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO force_subscribe_channels (username, title) VALUES (?, ?)", (username, title))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False # Username already exists
    finally:
        conn.close()

def get_all_force_sub_channels():
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    cursor.execute("SELECT id, username, title FROM force_subscribe_channels")
    channels = cursor.fetchall()
    conn.close()
    return channels

def remove_force_sub_channel(channel_id):
    conn = sqlite32.connect('bot_data.db')
    cursor = conn.cursor()
    cursor.execute("DELETE FROM force_subscribe_channels WHERE id = ?", (channel_id,))
    deleted = cursor.rowcount > 0
    conn.commit()
    conn.close()
    return deleted

# Helper to escape text for MarkdownV2
def escape_markdown_v2(text):
    """Helper function to escape characters reserved in MarkdownV2."""
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    text = text.replace('\\', '\\\\')
    return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', text)


# Menu functions
def get_admin_menu_keyboard():
    keyboard = [
        [InlineKeyboardButton("🔗 GENERATE CHANNEL LINKS", callback_data="generate_links")],
        [InlineKeyboardButton("📣 BROADCAST", callback_data="broadcast")],
        [InlineKeyboardButton("➕ FORCE SUBSCRIBE MANAGEMENT", callback_data="force_sub_manage")],
        [InlineKeyboardButton("📊 BOT STATS", callback_data="bot_stats")],
    ]
    return InlineKeyboardMarkup(keyboard)

def get_force_sub_keyboard():
    keyboard = [
        [InlineKeyboardButton("➕ ADD NEW CHANNEL", callback_data="force_sub_add")],
        [InlineKeyboardButton("➖ REMOVE CHANNEL", callback_data="force_sub_remove")],
        [InlineKeyboardButton("🔙 BACK TO ADMIN MENU", callback_data="admin_back")],
    ]
    return InlineKeyboardMarkup(keyboard)

async def show_admin_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_states[user_id] = ADMIN_MENU
    
    # Calculate stats
    total_users = len(get_all_active_users())
    
    # Force Subscribe channels
    channels = get_all_force_sub_channels()
    force_sub_channels_count = len(channels)
    
    menu_text = (
        f"🤖 **ADMIN PANEL** 🤖\n\n"
        f"**Total Users:** {total_users}\n"
        f"**Active Force Subscribe Channels:** {force_sub_channels_count}\n\n"
        f"Use the buttons below to manage the bot\\."
    )
    
    if update.callback_query:
        await update.callback_query.edit_message_text(
            menu_text,
            reply_markup=get_admin_menu_keyboard(),
            parse_mode='MarkdownV2'
        )
    else:
        await update.message.reply_text(
            menu_text,
            reply_markup=get_admin_menu_keyboard(),
            parse_mode='MarkdownV2'
        )

async def show_force_sub_management(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_states[user_id] = ADMIN_MENU # State remains ADMIN_MENU
    
    channels = get_all_force_sub_channels()
    
    channels_text = ""
    if not channels:
        channels_text = "_No Force Subscribe channels configured\\._"
    else:
        for id, username, title in channels:
            safe_title = escape_markdown_v2(title)
            safe_username = escape_markdown_v2(username)
            channels_text += rf"• {safe_title} \(`ID: {id}` / `{safe_username}`\)\n"

    menu_text = (
        rf"➕ **FORCE SUBSCRIBE MANAGEMENT** ➖\n\n"
        rf"**Configured Channels:**\n"
        rf"{channels_text}\n"
        rf"Use the buttons below to add or remove channels\\."
    )
    
    await update.callback_query.edit_message_text(
        menu_text,
        reply_markup=get_force_sub_keyboard(),
        parse_mode='MarkdownV2'
    )
    # Core handlers
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    add_user(user_id)
    user_states[user_id] = INIT
    
    # Check if the user is the admin
    if user_id == ADMIN_ID:
        # Show Admin Menu
        await show_admin_menu(update, context)
        return

    # Check for deep link
    link_id = context.args[0] if context.args else None
    
    if link_id and link_id.startswith("verify_deep_"):
        # This is a deep link for verification (from the callback query)
        # This case is handled in button_handler
        return

    # User is NOT admin and NO deep link
    if not link_id:
        # Send a welcome message with subscription check
        keyboard = [
            [InlineKeyboardButton("🔗 PUBLIC ANIME CHANNEL", url=PUBLIC_ANIME_CHANNEL_URL)],
            [InlineKeyboardButton("💬 REQUEST CHANNEL", url=REQUEST_CHANNEL_URL)],
            [InlineKeyboardButton("👤 CONTACT ADMIN", url=f"https://t.me/{ADMIN_CONTACT_USERNAME}")],
        ]
        
        await update.message.reply_text(
            "Welcome to the bot\\! Use the menu below to navigate\\.",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='MarkdownV2'
        )
        return

    # User is NOT admin and HAS a deep link
    channel_identifier, expiry_time, is_used = get_link_data(link_id)
    
    if not channel_identifier:
        await update.message.reply_text(
            "❌ **Error:** The link is invalid or has expired\\.",
            parse_mode='MarkdownV2'
        )
        return
        
    if is_used:
        await update.message.reply_text(
            "❌ **Error:** This link has already been used\\.",
            parse_mode='MarkdownV2'
        )
        return
        
    if datetime.now() > expiry_time:
        await update.message.reply_text(
            "❌ **Error:** This link has expired\\.",
            parse_mode='MarkdownV2'
        )
        return

    # Generate a unique callback data for the verification button
    verify_callback_data = f"verify_deep_{link_id}"
    
    # Check Force Subscribe status
    channels_to_check = get_all_force_sub_channels()
    
    if channels_to_check:
        all_subscribed = True
        channels_list = ""
        for _, username, _ in channels_to_check:
            try:
                # Check subscription status
                chat_member = await context.bot.get_chat_member(username, user_id)
                if chat_member.status in ['left', 'kicked']:
                    all_subscribed = False
                    channels_list += f"• [{username}]({username})\n" # Links work fine in MarkdownV2
            except Exception as e:
                logger.error(f"Error checking force sub for {username}: {e}")
                # Treat error as not subscribed for safety
                all_subscribed = False
                channels_list += f"• [{username}]({username})\n"

        if not all_subscribed:
            # User is not subscribed to all required channels
            # Send Force Subscribe message
            keyboard = [
                [InlineKeyboardButton("✅ I'M SUBSCRIBED", callback_data=verify_callback_data)]
            ]
            await update.message.reply_text(
                "🚨 **FORCE SUBSCRIBE REQUIRED** 🚨\n\n"
                "You must join the following channels to proceed:\n"
                f"{channels_list}\n"
                "After joining, click the button below\\.",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='MarkdownV2',
                disable_web_page_preview=True
            )
            return

    # If force subscribe is passed or not required, proceed directly
    await handle_channel_link_deep(update, context, link_id, channel_identifier)


async def handle_channel_link_deep(update: Update, context: ContextTypes.DEFAULT_TYPE, link_id, channel_identifier):
    """
    Final step to grant access and mark the link as used.
    Assumes all checks (expiry, usage, force sub) have passed.
    """
    try:
        # 1. Send the channel link
        chat = await context.bot.get_chat(channel_identifier)
        invite_link = chat.invite_link
        
        if not invite_link:
            # Try to create a new one if it doesn't exist
            invite_link = await context.bot.create_chat_invite_link(channel_identifier)
            invite_link = invite_link.invite_link

        # Use HTML for the final message to prevent MarkdownV2 issues with channel names
        safe_chat_title = chat.title
        safe_channel_id = str(channel_identifier)
        safe_expiry = str(LINK_EXPIRY_MINUTES)
        
        success_message = (
            f"🎉 <b>Access Granted!</b> 🎉\n\n"
            f"<b>Channel:</b> {safe_chat_title}\n"
            f"<b>ID/Username:</b> <code>{safe_channel_id}</code>\n"
            f"<b>Expires in:</b> {safe_expiry} minutes\n"
            f"<b>Usage:</b> Single use\n\n"
            f"Click the link below to join the channel:\n"
            f"<a href='{invite_link}'>🔗 JOIN CHANNEL</a>\n\n"
            f"<i>Enjoy the content! 🍿</i>"
        )
        
        keyboard = [
            [InlineKeyboardButton("🔗 JOIN CHANNEL", url=invite_link)]
        ]

        # Use the appropriate message type (callback_query or message)
        if update.callback_query:
            await update.callback_query.edit_message_text(
                success_message,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='HTML',
                disable_web_page_preview=True
            )
        else: # From /start deep link
            await update.message.reply_text(
                success_message,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='HTML',
                disable_web_page_preview=True
            )
            
        # 2. Mark link as used
        mark_link_used(link_id)

    except Exception as e:
        logger.error(f"Error handling deep link final step for {link_id}: {e}")
        await context.bot.send_message(
            update.effective_user.id,
            "⚠️ **Error:** Could not generate the channel link\\. The bot may not be an admin in the channel\\.",
            parse_mode='MarkdownV2'
        )

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    data = query.data

    # Ensure admin is still in ADMIN_MENU state
    if user_id == ADMIN_ID and data in ["admin_back", "generate_links", "broadcast", "force_sub_manage"]:
        user_states[user_id] = ADMIN_MENU

    # --- ADMIN MENU NAVIGATION ---
    if data == "admin_back":
        await show_admin_menu(update, context)
        
    elif data == "generate_links":
        user_states[user_id] = GENERATE_LINK_CHANNEL_USERNAME
        keyboard = [[InlineKeyboardButton("🔙 BACK TO MENU", callback_data="admin_back")]]
        
        # FIX 1: Switched to HTML and removed escaping in static text
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text="🔗 <b>GENERATE CHANNEL LINKS</b>\n\nPlease send:\n• Channel username (e.g., @YourChannel) OR\n• Private channel ID (e.g., -1001234567890)\n\nTo get private channel ID, forward any message from that channel to @userinfobot",
            parse_mode='HTML', 
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
    elif data == "broadcast":
        user_states[user_id] = BROADCAST_MESSAGE
        keyboard = [[InlineKeyboardButton("🔙 BACK TO MENU", callback_data="admin_back")]]
        
        # FIX 3: Switched to HTML and removed escaping in static text
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text="📢 <b>BROADCAST MESSAGE</b> 📢\n\nPlease send the message you wish to broadcast to all users (MarkdownV2 is supported).",
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
    elif data == "bot_stats":
        total_users = len(get_all_active_users())
        channels = get_all_force_sub_channels()
        force_sub_channels_count = len(channels)
        
        stats_text = (
            f"📊 **BOT STATISTICS** 📊\n\n"
            f"**Total Registered Users:** {total_users}\n"
            f"**Total Active Deep Links:** {len(get_link_data)}\n" # Placeholder, cleanup is separate
            f"**Force Sub Channels:** {force_sub_channels_count}\n\n"
            f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        
        keyboard = [[InlineKeyboardButton("🔙 BACK TO MENU", callback_data="admin_back")]]
        
        await query.edit_message_text(
            stats_text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='MarkdownV2'
        )

    # --- FORCE SUBSCRIBE MANAGEMENT ---
    elif data == "force_sub_manage":
        await show_force_sub_management(update, context)

    elif data == "force_sub_add":
        user_states[user_id] = FORCE_SUB_ADD_USERNAME
        keyboard = [[InlineKeyboardButton("🔙 BACK", callback_data="force_sub_manage")]]
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text="➕ **ADD NEW CHANNEL**\n\nPlease send the **@username** or **Channel ID** of the channel you want to add\\.",
            parse_mode='MarkdownV2',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
    elif data == "force_sub_remove":
        user_states[user_id] = FORCE_SUB_REMOVE
        channels = get_all_force_sub_channels()
        
        if not channels:
            await query.edit_message_text(
                "➖ **REMOVE CHANNEL**\n\n_No channels to remove\\._",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 BACK", callback_data="force_sub_manage")]])
            )
            return

        remove_text = "➖ **REMOVE CHANNEL**\n\nSend the **ID** of the channel you want to remove:\n\n"
        for id, username, title in channels:
            safe_title = escape_markdown_v2(title)
            remove_text += rf"• {safe_title} \(`ID: {id}` / `{username}`\)\n"

        keyboard = [[InlineKeyboardButton("🔙 BACK", callback_data="force_sub_manage")]]
        await query.edit_message_text(
            remove_text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='MarkdownV2'
        )
        
    elif data.startswith("confirm_remove_"):
        channel_id = data.split("_")[-1]
        if remove_force_sub_channel(channel_id):
            await query.edit_message_text(
                f"✅ **SUCCESS:** Channel with ID `{channel_id}` has been removed\\.",
                parse_mode='MarkdownV2',
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 BACK TO MANAGEMENT", callback_data="force_sub_manage")]])
            )
        else:
            await query.edit_message_text(
                f"❌ **ERROR:** Channel with ID `{channel_id}` not found\\.",
                parse_mode='MarkdownV2',
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 BACK TO MANAGEMENT", callback_data="force_sub_manage")]])
            )

    # --- DEEP LINK VERIFICATION ---
    elif data.startswith("verify_deep_"):
        link_id = data.split("_")[-1]
        channel_identifier, expiry_time, is_used = get_link_data(link_id)
        
        if is_used or datetime.now() > expiry_time:
            await query.edit_message_text(
                "❌ **Error:** The link has already been used or has expired\\.",
                parse_mode='MarkdownV2'
            )
            return
            
        # Check Force Subscribe status again (final check)
        channels_to_check = get_all_force_sub_channels()
        
        if channels_to_check:
            all_subscribed = True
            for _, username, _ in channels_to_check:
                try:
                    chat_member = await context.bot.get_chat_member(username, user_id)
                    if chat_member.status in ['left', 'kicked']:
                        all_subscribed = False
                        break
                except Exception as e:
                    logger.error(f"Error checking force sub for {username}: {e}")
                    all_subscribed = False
                    break

            if not all_subscribed:
                # Still not subscribed, tell them to try again
                await query.answer("You must subscribe to all channels to proceed. Please try again.", show_alert=True)
                return

        # Passed all checks, grant access
        await handle_channel_link_deep(update, context, link_id, channel_identifier)

async def handle_admin_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    state = user_states.get(user_id)
    text = update.text

    if state == GENERATE_LINK_CHANNEL_USERNAME:
        try:
            channel_identifier = text.strip()
            # Attempt to get chat info
            chat = await context.bot.get_chat(channel_identifier)
            channel_title = chat.title
            
            # --- Link Generation ---
            link_id = generate_link_id(channel_identifier, user_id)
            bot_info = await context.bot.get_me()
            bot_username = bot_info.username
            deep_link = f"https://t.me/{bot_username}?start={link_id}"
            
            # FIX 2: Switched success message to HTML
            # Removed escape_markdown_v2 on dynamic variables
            
            expiry_str = str(LINK_EXPIRY_MINUTES)
            
            await update.message.reply_text(
                f"🔗 <b>LINK GENERATED</b> 🔗\n\n"
                f"<b>Channel:</b> {channel_title}\n"
                f"<b>ID/Username:</b> <code>{channel_identifier}</code>\n"
                f"<b>Expires in:</b> {expiry_str} minutes\n\n"
                f"<b>Direct Link:</b>\n<code>{deep_link}</code>\n\n"
                "Share this link with users!",
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 BACK TO MENU", callback_data="admin_back")]])
            )
            
            user_states[user_id] = ADMIN_MENU
            
        except Exception as e:
            logger.error(f"Error in GENERATE_LINK_CHANNEL_USERNAME: {e}")
            await update.message.reply_text(
                "❌ **Error:** Cannot access this channel or invalid input\\. Ensure the bot is an admin in the channel\\.",
                parse_mode='MarkdownV2'
            )
            
    elif state == BROADCAST_MESSAGE:
        user_broadcast_text[user_id] = text
        
        # FIX 4: Switched confirmation message to HTML
        keyboard = [
            [InlineKeyboardButton("✅ CONFIRM BROADCAST", callback_data="confirm_broadcast")],
            [InlineKeyboardButton("❌ CANCEL", callback_data="admin_back")]
        ]
        
        await update.message.reply_text(
            "📣 <b>CONFIRM BROADCAST</b> 📣\n\n"
            "This message will be sent to all active users\\. Review it below:",
            parse_mode='HTML'
        )
        
        # Send the user's message separately for preview
        await context.bot.send_message(
            chat_id=user_id,
            text=text,
            parse_mode='MarkdownV2', # Use MarkdownV2 for the preview of the user's text
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
    elif state == FORCE_SUB_ADD_USERNAME:
        # Check if it's a valid username/ID and bot can access it
        try:
            channel_identifier = text.strip()
            chat = await context.bot.get_chat(channel_identifier)
            
            # Store channel info temporarily
            context.user_data['force_sub_username'] = chat.username or chat.id
            context.user_data['force_sub_title'] = chat.title
            
            user_states[user_id] = FORCE_SUB_ADD_TITLE
            keyboard = [[InlineKeyboardButton("🔙 BACK", callback_data="force_sub_manage")]]
            
            await update.message.reply_text(
                f"✅ **Channel Found:** {escape_markdown_v2(chat.title)}\n\n"
                f"Now, please send the **display name/title** for this channel in the Force Subscribe list\\.",
                parse_mode='MarkdownV2',
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            
        except Exception as e:
            logger.error(f"Error in FORCE_SUB_ADD_USERNAME: {e}")
            await update.message.reply_text(
                "❌ **Error:** Cannot access this channel or invalid input\\. Ensure the bot is an admin in the channel\\.",
                parse_mode='MarkdownV2'
            )
            user_states[user_id] = ADMIN_MENU # Reset state for safety
            
    elif state == FORCE_SUB_ADD_TITLE:
        username = context.user_data.pop('force_sub_username')
        title = text.strip()
        
        if add_force_sub_channel(username, title):
            success_text = f"✅ **SUCCESS:** Force Subscribe channel added\\.\n\n**Title:** {escape_markdown_v2(title)}\n**Username/ID:** `{escape_markdown_v2(str(username))}`"
        else:
            success_text = f"❌ **ERROR:** Channel `{escape_markdown_v2(str(username))}` already exists in the list\\."
            
        await update.message.reply_text(
            success_text,
            parse_mode='MarkdownV2',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 BACK TO MANAGEMENT", callback_data="force_sub_manage")]])
        )
        user_states[user_id] = ADMIN_MENU

    elif state == FORCE_SUB_REMOVE:
        try:
            channel_id = int(text.strip())
            
            keyboard = [
                [InlineKeyboardButton("✅ YES, REMOVE", callback_data=f"confirm_remove_{channel_id}")],
                [InlineKeyboardButton("❌ NO, CANCEL", callback_data="force_sub_manage")]
            ]
            
            await update.message.reply_text(
                f"⚠️ **CONFIRM REMOVAL:** Are you sure you want to remove the channel with ID `{channel_id}` from Force Subscribe list\\?",
                parse_mode='MarkdownV2',
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            user_states[user_id] = ADMIN_MENU # Wait for confirmation button

        except ValueError:
            await update.message.reply_text(
                "❌ **Error:** Invalid input\\. Please send a valid **numerical ID** from the list\\.",
                parse_mode='MarkdownV2',
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 BACK TO MANAGEMENT", callback_data="force_sub_manage")]])
            )
            user_states[user_id] = ADMIN_MENU

    else:
        # Default behavior for admin messages outside of a state
        await show_admin_menu(update, context)

async def cleanup_task(context: ContextTypes.DEFAULT_TYPE):
    cleanup_deep_links()
    logger.info("Deep link cleanup task completed.")
    
async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Exception while handling an update: {context.error}")

def keep_alive():
    """Simple function to ping the webhook URL to keep the bot alive."""
    if WEBHOOK_URL:
        while True:
            try:
                requests.get(WEBHOOK_URL)
                time.sleep(300) # Ping every 5 minutes
            except Exception as e:
                logger.error(f"Keep-alive error: {e}")
                time.sleep(300)
def main():
    init_db()
    
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
        logger.warning("JobQueue is not available.")

    if WEBHOOK_URL and BOT_TOKEN:
        keep_alive_thread = Thread(target=keep_alive, daemon=True)
        keep_alive_thread.start()
        logger.info("✅ Keep-alive service started - Bot will remain active 24/7")
        
        print(f"🤖 Starting Webhook on port {PORT}")
        print(f"🌐 Webhook URL: {WEBHOOK_URL + BOT_TOKEN}")
        application.run_webhook(
            listen="0.0.0.0",
            port=PORT,
            url_path=BOT_TOKEN,
            webhook_url=WEBHOOK_URL + BOT_TOKEN
        )
    else:
        # If running locally or without webhooks
        logger.info("Polling mode started.")
        application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main()                
