import os
import logging
import sqlite3
import secrets
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, MessageHandler, filters
# Removed: from flask import Flask, request
import asyncio
# Removed: from threading import Thread

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Bot configuration
BOT_TOKEN = '7877393813:AAGKvpRBlYWwO70B9pQpD29BhYCXwiZGngw'
ADMIN_ID = 829342319
LINK_EXPIRY_MINUTES = 5  # Links expire after 5 minutes

# User states for conversation
ADD_CHANNEL_USERNAME, ADD_CHANNEL_TITLE = range(2)
user_states = {}

# Initialize databases
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

# User management
def add_user(user_id, username, first_name, last_name):
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    cursor.execute('''
        INSERT OR REPLACE INTO users (user_id, username, first_name, last_name)
        VALUES (?, ?, ?, ?)
    ''', (user_id, username, first_name, last_name))
    conn.commit()
    conn.close()

def get_all_users():
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    cursor.execute('SELECT user_id, username, first_name, last_name FROM users')
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

# Force subscription channels management
def add_force_sub_channel(channel_username, channel_title):
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT OR REPLACE INTO force_sub_channels (channel_username, channel_title)
            VALUES (?, ?)
        ''', (channel_username, channel_title))
        conn.commit()
        return True
    except Exception as e:
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

def get_force_sub_channel_count():
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM force_sub_channels WHERE is_active = 1')
    count = cursor.fetchone()[0]
    conn.close()
    return count

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

# Link management
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

# Check if user is subscribed to all force sub channels
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

# Admin check
def is_admin(user_id):
    return user_id == ADMIN_ID

# Start command - Simplified for regular users
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    add_user(user.id, user.username, user.first_name, user.last_name)
    
    # Check if it's a deep link for channel access
    if context.args and len(context.args) > 0:
        link_id = context.args[0]
        await handle_channel_link_deep(update, context, link_id)
        return
    
    # Check force subscription for regular users
    if not is_admin(user.id):
        not_joined_channels = await check_force_subscription(user.id, context)
        
        if not_joined_channels:
            # User hasn't joined all required channels
            keyboard = []
            for channel_username, channel_title in not_joined_channels:
                keyboard.append([InlineKeyboardButton(f"📢 JOIN {channel_title}", url=f"https://t.me/{channel_username[1:]}")])
            
            keyboard.append([InlineKeyboardButton("✅ VERIFY SUBSCRIPTION", callback_data="verify_subscription")])
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            channels_text = "\n".join([f"• {title} ({username})" for username, title in not_joined_channels])
            
            await update.message.reply_text(
                f"📢 **Please join our channels to use this bot!**\n\n"
                f"**Required Channels:**\n{channels_text}\n\n"
                f"Join all channels above and then click Verify Subscription.",
                reply_markup=reply_markup
            )
            return
    
    # User is either admin or has joined all required channels
    if is_admin(user.id):
        # Admin menu
        keyboard = [
            [InlineKeyboardButton("📊 BOT STATS", callback_data="admin_stats")],
            [InlineKeyboardButton("📺 MANAGE FORCE SUB CHANNELS", callback_data="manage_force_sub")],
            [InlineKeyboardButton("🔗 GENERATE CHANNEL LINKS", callback_data="generate_links")],
            [InlineKeyboardButton("📢 BROADCAST MESSAGE", callback_data="admin_broadcast")],
            [InlineKeyboardButton("👥 USER MANAGEMENT", callback_data="user_management")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(
            "👑 **ADMIN PANEL** 👑\n\n"
            "Welcome back, Admin! Choose an option below:",
            reply_markup=reply_markup
        )
    else:
        # Regular user menu - SIMPLIFIED
        keyboard = [
            [InlineKeyboardButton("📋 REQUEST CONTENT", callback_data="request_content")],
            [InlineKeyboardButton("👤 CONTACT ADMIN", callback_data="contact_admin")],
            [InlineKeyboardButton("ℹ️ ABOUT BOT", callback_data="about_bot")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        welcome_text = """
🌟 **WELCOME TO THE ADVANCED LINKS SHARING BOT** 🌟

Use this bot to request content access safely.
Explore the options below to get started!
        """
        
        await update.message.reply_text(welcome_text, reply_markup=reply_markup)

# Handle deep links for channel access
async def handle_channel_link_deep(update: Update, context: ContextTypes.DEFAULT_TYPE, link_id):
    link_info = get_link_info(link_id)
    
    if not link_info:
        await update.message.reply_text("❌ This link has expired or is invalid.")
        return
    
    channel_username, creator_id, created_time, is_used = link_info
    
    if is_used:
        await update.message.reply_text("❌ This link has already been used.")
        return
    
    # Check if link is expired
    link_age = datetime.now() - datetime.fromisoformat(created_time)
    if link_age.total_seconds() > LINK_EXPIRY_MINUTES * 60:
        await update.message.reply_text("❌ This link has expired.")
        return
    
    user = update.effective_user
    
    # Check force subscription first
    not_joined_channels = await check_force_subscription(user.id, context)
    if not_joined_channels:
        keyboard = []
        for channel_username, channel_title in not_joined_channels:
            keyboard.append([InlineKeyboardButton(f"📢 JOIN {channel_title}", url=f"https://t.me/{channel_username[1:]}")])
        
        keyboard.append([InlineKeyboardButton("✅ VERIFY SUBSCRIPTION", callback_data=f"verify_deep_{link_id}")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        channels_text = "\n".join([f"• {title} ({username})" for username, title in not_joined_channels])
        
        await update.message.reply_text(
            f"📢 **Please join our channels to get access!**\n\n"
            f"**Required Channels:**\n{channels_text}\n\n"
            f"Join all channels above and then click Verify Subscription.",
            reply_markup=reply_markup
        )
        return
    
    # User is subscribed to all channels - generate access link
    try:
        chat = await context.bot.get_chat(channel_username)
        invite_link = await context.bot.create_chat_invite_link(
            chat.id, 
            member_limit=1,  # Single use
            expire_date=datetime.now().timestamp() + 300  # 5 minutes
        )
        
        mark_link_used(link_id)
        
        await update.message.reply_text(
            f"🎉 **Access Granted!** 🎉\n\n"
            f"**Channel:** {chat.title}\n"
            f"**Invite Link:** {invite_link.invite_link}\n"
            f"⏰ **Expires in:** 5 minutes\n"
            f"👥 **Usage:** Single use\n\n"
            f"Enjoy the content! 🍿"
        )
    except Exception as e:
        await update.message.reply_text("❌ Error generating access link.")

# Admin commands
async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin only command.")
        return
    
    if not context.args:
        await update.message.reply_text("Usage: /broadcast <message>")
        return
    
    message = ' '.join(context.args)
    users = get_all_users()
    success_count = 0
    
    for user in users:
        try:
            await context.bot.send_message(chat_id=user[0], text=message)
            success_count += 1
        except Exception:
            pass
        await asyncio.sleep(0.1)
    
    await update.message.reply_text(f"📊 Broadcast sent to {success_count}/{len(users)} users.")

# Button handlers
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    data = query.data
    
    # Handle verification for force subscription
    if data == "verify_subscription":
        not_joined_channels = await check_force_subscription(user_id, context)
        
        if not_joined_channels:
            channels_text = "\n".join([f"• {title}" for _, title in not_joined_channels])
            await query.edit_message_text(
                f"❌ **You haven't joined all required channels!**\n\n"
                f"**Still missing:**\n{channels_text}\n\n"
                f"Please join all channels and try again."
            )
            return
        
        # User joined all channels - show main menu
        if is_admin(user_id):
            keyboard = [
                [InlineKeyboardButton("📊 BOT STATS", callback_data="admin_stats")],
                [InlineKeyboardButton("📺 MANAGE FORCE SUB CHANNELS", callback_data="manage_force_sub")],
                [InlineKeyboardButton("🔗 GENERATE CHANNEL LINKS", callback_data="generate_links")],
                [InlineKeyboardButton("📢 BROADCAST MESSAGE", callback_data="admin_broadcast")],
                [InlineKeyboardButton("👥 USER MANAGEMENT", callback_data="user_management")]
            ]
            text = "👑 **ADMIN PANEL** 👑\n\nWelcome back, Admin!"
        else:
            keyboard = [
                [InlineKeyboardButton("📋 REQUEST CONTENT", callback_data="request_content")],
                [InlineKeyboardButton("👤 CONTACT ADMIN", callback_data="contact_admin")],
                [InlineKeyboardButton("ℹ️ ABOUT BOT", callback_data="about_bot")]
            ]
            text = "✅ **Subscription verified!**\n\nWelcome to the bot!"
        
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    
    # Handle verification for deep links
    elif data.startswith("verify_deep_"):
        link_id = data[12:]
        not_joined_channels = await check_force_subscription(user_id, context)
        
        if not_joined_channels:
            channels_text = "\n".join([f"• {title}" for _, title in not_joined_channels])
            await query.edit_message_text(
                f"❌ **You haven't joined all required channels!**\n\n"
                f"**Still missing:**\n{channels_text}\n\n"
                f"Please join all channels and try again."
            )
            return
        
        # User joined all channels - process the deep link
        link_info = get_link_info(link_id)
        if not link_info:
            await query.edit_message_text("❌ Link expired or invalid.")
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
            
            await query.edit_message_text(
                f"🎉 **Access Granted!** 🎉\n\n"
                f"**Channel:** {chat.title}\n"
                f"**Invite Link:** {invite_link.invite_link}\n"
                f"⏰ **Expires in:** 5 minutes\n"
                f"👥 **Usage:** Single use\n\n"
                f"Enjoy the content! 🍿"
            )
        except Exception as e:
            await query.edit_message_text("❌ Error generating access link.")
    
    # Admin panel handlers
    elif data == "admin_stats":
        if not is_admin(user_id):
            await query.edit_message_text("❌ Admin only.")
            return
        
        user_count = get_user_count()
        channel_count = get_force_sub_channel_count()
        
        stats_text = f"""
📊 **BOT STATISTICS** 📊

👥 **Total Users:** {user_count}
📺 **Force Sub Channels:** {channel_count}
🔗 **Link Expiry:** {LINK_EXPIRY_MINUTES} minutes

**Last Cleanup:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        """
        
        keyboard = [[InlineKeyboardButton("🔄 REFRESH", callback_data="admin_stats")],
                   [InlineKeyboardButton("🔙 BACK", callback_data="admin_back")]]
        await query.edit_message_text(stats_text, reply_markup=InlineKeyboardMarkup(keyboard))
    
    elif data == "manage_force_sub":
        await show_force_sub_management(query, context)
    
    elif data == "generate_links":
        if not is_admin(user_id):
            await query.edit_message_text("❌ Admin only.")
            return
        
        channels = get_all_force_sub_channels()
        
        if not channels:
            keyboard = [[InlineKeyboardButton("📺 ADD CHANNEL", callback_data="add_channel_start")],
                       [InlineKeyboardButton("🔙 BACK", callback_data="admin_back")]]
            await query.edit_message_text(
                "❌ No force sub channels found!\n\nPlease add channels first.",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return
        
        keyboard = []
        for channel_username, channel_title in channels:
            keyboard.append([InlineKeyboardButton(f"🔗 {channel_title}", callback_data=f"genlink_{channel_username}")])
        
        keyboard.append([InlineKeyboardButton("🔙 BACK", callback_data="admin_back")])
        
        await query.edit_message_text(
            "🔗 **GENERATE CHANNEL LINKS**\n\n"
            "Select a channel to generate expirable links:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    elif data.startswith("genlink_"):
        if not is_admin(user_id):
            await query.edit_message_text("❌ Admin only.")
            return
        
        channel_username = data[8:]
        link_id = generate_link_id(channel_username, user_id)
        
        bot_username = context.bot.username
        deep_link = f"https://t.me/{bot_username}?start={link_id}"
        
        await query.edit_message_text(
            f"🔗 **LINK GENERATED** 🔗\n\n"
            f"**Channel:** {channel_username}\n"
            f"**Expires in:** {LINK_EXPIRY_MINUTES} minutes\n\n"
            f"**Direct Link:**\n`{deep_link}`\n\n"
            f"Share this link with users!",
            parse_mode='Markdown'
        )
    
    elif data == "add_channel_start":
        if not is_admin(user_id):
            await query.edit_message_text("❌ Admin only.")
            return
        
        user_states[user_id] = ADD_CHANNEL_USERNAME
        await query.edit_message_text(
            "📺 **ADD FORCE SUBSCRIPTION CHANNEL**\n\n"
            "Please send me the channel username (starting with @):\n\n"
            "Example: `@Beat_Anime_Ocean`",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 CANCEL", callback_data="manage_force_sub")]])
        )
    
    elif data.startswith("channel_"):
        await show_channel_details(query, context, data[8:])
    
    elif data.startswith("delete_"):
        channel_username = data[7:]
        channel_info = get_force_sub_channel_info(channel_username)
        
        if channel_info:
            keyboard = [
                [InlineKeyboardButton("✅ YES, DELETE", callback_data=f"confirm_delete_{channel_username}")],
                [InlineKeyboardButton("❌ NO, CANCEL", callback_data=f"channel_{channel_username}")]
            ]
            await query.edit_message_text(
                f"🗑️ **CONFIRM DELETION**\n\n"
                f"Are you sure you want to delete this force sub channel?\n\n"
                f"**Channel:** {channel_info[1]}\n"
                f"**Username:** {channel_info[0]}\n\n"
                f"This action cannot be undone!",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
    
    elif data.startswith("confirm_delete_"):
        channel_username = data[15:]
        delete_force_sub_channel(channel_username)
        
        await query.edit_message_text(
            f"✅ **CHANNEL DELETED**\n\n"
            f"Force sub channel `{channel_username}` has been deleted successfully.",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📺 BACK TO CHANNELS", callback_data="manage_force_sub")]])
        )
    
    elif data in ["admin_back", "user_back", "channels_back"]:
        if is_admin(user_id):
            keyboard = [
                [InlineKeyboardButton("📊 BOT STATS", callback_data="admin_stats")],
                [InlineKeyboardButton("📺 MANAGE FORCE SUB CHANNELS", callback_data="manage_force_sub")],
                [InlineKeyboardButton("🔗 GENERATE CHANNEL LINKS", callback_data="generate_links")],
                [InlineKeyboardButton("📢 BROADCAST MESSAGE", callback_data="admin_broadcast")],
                [InlineKeyboardButton("👥 USER MANAGEMENT", callback_data="user_management")]
            ]
            text = "👑 **ADMIN PANEL** 👑\n\nChoose an option:"
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            # Regular user menu - SIMPLIFIED
            keyboard = [
                [InlineKeyboardButton("📋 REQUEST CONTENT", callback_data="request_content")],
                [InlineKeyboardButton("👤 CONTACT ADMIN", callback_data="contact_admin")],
                [InlineKeyboardButton("ℹ️ ABOUT BOT", callback_data="about_bot")]
            ]
            await query.edit_message_text(
                "🌟 **MAIN MENU** 🌟\n\nChoose an option:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
    
    elif data == "request_content":
        await query.edit_message_text(
            "📋 **REQUEST CONTENT**\n\n"
            "To request specific content or channel access:\n\n"
            "📧 **Contact Admin:** @Beect\n"
            "💬 **Send your request** directly to the admin\n\n"
            "We'll respond as soon as possible!",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 BACK", callback_data="user_back")]
            ])
        )
    
    elif data == "contact_admin":
        await query.edit_message_text(
            "👤 **CONTACT ADMIN**\n\n"
            "For any questions or support:\n\n"
            "📧 **Admin:** @Beect\n"
            "💬 **Direct Message:** https://t.me/Beect\n\n"
            "We're here to help!",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 BACK", callback_data="user_back")]
            ])
        )
    
    elif data == "about_bot":
        await query.edit_message_text(
            "ℹ️ **ABOUT THIS BOT**\n\n"
            "🌟 **Advanced Links Sharing Bot** 🌟\n\n"
            "**Features:**\n"
            "• Secure content access\n"
            "• Force subscription system\n"
            "• Admin management\n"
            "• User-friendly interface\n\n"
            "Built with ❤️ for content sharing communities!",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 BACK", callback_data="user_back")]
            ])
        )

# Show force sub channels management interface
async def show_force_sub_management(query, context):
    user_id = query.from_user.id
    if not is_admin(user_id):
        await query.edit_message_text("❌ Admin only.")
        return
    
    channels = get_all_force_sub_channels()
    keyboard = []
    
    # Add existing channels
    for channel_username, channel_title in channels:
        keyboard.append([InlineKeyboardButton(f"📺 {channel_title}", callback_data=f"channel_{channel_username}")])
    
    # Add "Add Channel" button at the end
    keyboard.append([InlineKeyboardButton("➕ ADD CHANNEL", callback_data="add_channel_start")])
    keyboard.append([InlineKeyboardButton("🔙 BACK", callback_data="admin_back")])
    
    channel_count = len(channels)
    text = f"📺 **MANAGE FORCE SUBSCRIPTION CHANNELS**\n\n"
    text += f"**Total Channels:** {channel_count}\n\n"
    
    if channel_count == 0:
        text += "No force sub channels added yet. Click 'ADD CHANNEL' to get started!"
    else:
        text += "Users must join ALL these channels to use the bot.\nSelect a channel to manage:"
    
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

# Show channel details
async def show_channel_details(query, context, channel_username):
    user_id = query.from_user.id
    if not is_admin(user_id):
        await query.edit_message_text("❌ Admin only.")
        return
    
    channel_info = get_force_sub_channel_info(channel_username)
    
    if not channel_info:
        await query.edit_message_text("❌ Channel not found.")
        return
    
    channel_username, channel_title = channel_info
    
    keyboard = [
        [InlineKeyboardButton("🔗 GENERATE LINK", callback_data=f"genlink_{channel_username}")],
        [InlineKeyboardButton("🗑️ DELETE CHANNEL", callback_data=f"delete_{channel_username}")],
        [InlineKeyboardButton("📺 BACK TO CHANNELS", callback_data="manage_force_sub")],
        [InlineKeyboardButton("🔙 BACK TO MENU", callback_data="admin_back")]
    ]
    
    text = f"📺 **FORCE SUB CHANNEL DETAILS**\n\n"
    text += f"**Title:** {channel_title}\n"
    text += f"**Username:** {channel_username}\n"
    text += f"**Status:** ✅ Active\n\n"
    text += f"Users must join this channel to access bot features."
    
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

# Handle text messages for adding channels
async def handle_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    if user_id not in user_states:
        return
    
    state = user_states[user_id]
    text = update.message.text
    
    if state == ADD_CHANNEL_USERNAME:
        if not text.startswith('@'):
            await update.message.reply_text(
                "❌ Please provide a valid channel username starting with @\n\n"
                "Example: `@Beat_Anime_Ocean`\n\n"
                "Try again:",
                parse_mode='Markdown'
            )
            return
        
        context.user_data['channel_username'] = text
        user_states[user_id] = ADD_CHANNEL_TITLE
        
        await update.message.reply_text(
            "📝 **STEP 2: Channel Title**\n\n"
            "Now please send me the display title for this channel:\n\n"
            "Example: `Anime Ocean Channel`",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 CANCEL", callback_data="manage_force_sub")]])
        )
    
    elif state == ADD_CHANNEL_TITLE:
        channel_username = context.user_data.get('channel_username')
        
        if add_force_sub_channel(channel_username, text):
            # Clean up state
            if user_id in user_states:
                del user_states[user_id]
            if 'channel_username' in context.user_data:
                del context.user_data['channel_username']
            
            await update.message.reply_text(
                f"✅ **FORCE SUB CHANNEL ADDED SUCCESSFULLY!**\n\n"
                f"**Username:** {channel_username}\n"
                f"**Title:** {text}\n\n"
                f"Channel has been added to force subscription list!",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📺 MANAGE CHANNELS", callback_data="manage_force_sub")]])
            )
        else:
            await update.message.reply_text(
                "❌ Error adding channel. It might already exist or there was a database error."
            )

# Error handler
async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Exception while handling an update: {context.error}")

# Cleanup task
async def cleanup_task(context: ContextTypes.DEFAULT_TYPE):
    cleanup_expired_links()

# Flask app for Render - REMOVED!
# @app.route('/') - REMOVED!
# def run_flask(): - REMOVED!

def main():
    # Initialize database
    init_db()
    
    # Create Application
    application = Application.builder().token(BOT_TOKEN).build()
    
    # Add handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("broadcast", broadcast))
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_message))
    application.add_error_handler(error_handler)
    
    # Add cleanup job (runs every 10 minutes)
    job_queue = application.job_queue
    # This call now succeeds because [job-queue] is in requirements.txt
    if job_queue: 
        job_queue.run_repeating(cleanup_task, interval=600, first=10)
    
    # Start Flask server - REMOVED!
    # from threading import Thread - REMOVED!
    # flask_thread = Thread(target=run_flask) - REMOVED!
    # flask_thread.daemon = True - REMOVED!
    # flask_thread.start() - REMOVED!
    
    # Start the bot
    print("🤖 Force Subscription Bot is starting in Polling Mode...")
    application.run_polling()

if __name__ == '__main__':
    main()
