import os
import logging
import sqlite3
import secrets
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
BOT_TOKEN = os.getenv("BOT_TOKEN", "YOUR_TOKEN_HERE")
ADMIN_ID = 829342319
LINK_EXPIRY_MINUTES = 5

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

def keep_alive():
    """Pings a reliable external URL every 14 minutes to prevent sleep."""
    while True:
        try:
            time.sleep(840)
            _ = requests.get("https://www.google.com/robots.txt", timeout=10)
            logger.info("Keep-alive ping sent.")
        except Exception as e:
            logger.error("Keep-alive error: %s", e)

def init_db():
    conn = sqlite3.connect('bot_data.db')
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            joined_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS force_sub_channels (
            channel_id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_username TEXT UNIQUE,
            channel_title TEXT,
            added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_active BOOLEAN DEFAULT 1
        )
    ''')
    c.execute('''
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
    c = conn.cursor()
    c.execute('''
        INSERT OR REPLACE INTO users (user_id, username, first_name, last_name)
        VALUES (?, ?, ?, ?)
    ''', (user_id, username, first_name, last_name))
    conn.commit()
    conn.close()

def get_all_users(limit=None, offset=0):
    conn = sqlite3.connect('bot_data.db')
    c = conn.cursor()
    if limit is None:
        c.execute('SELECT user_id, username, first_name, last_name, joined_date FROM users ORDER BY joined_date DESC')
    else:
        c.execute('SELECT user_id, username, first_name, last_name, joined_date FROM users ORDER BY joined_date DESC LIMIT ? OFFSET ?', (limit, offset))
    rows = c.fetchall()
    conn.close()
    return rows

def get_user_count():
    conn = sqlite3.connect('bot_data.db')
    c = conn.cursor()
    c.execute('SELECT COUNT(*) FROM users')
    cnt = c.fetchone()[0]
    conn.close()
    return cnt

def get_force_sub_channel_count():
    conn = sqlite3.connect('bot_data.db')
    c = conn.cursor()
    c.execute('SELECT COUNT(*) FROM force_sub_channels WHERE is_active = 1')
    cnt = c.fetchone()[0]
    conn.close()
    return cnt

def add_force_sub_channel(channel_username, channel_title):
    conn = sqlite3.connect('bot_data.db')
    c = conn.cursor()
    try:
        c.execute('''
            INSERT OR IGNORE INTO force_sub_channels (channel_username, channel_title)
            VALUES (?, ?)
        ''', (channel_username, channel_title))
        conn.commit()
        return c.rowcount > 0
    except Exception as e:
        logger.error("DB Error in add_force_sub_channel: %s", e)
        return False
    finally:
        conn.close()

def get_all_force_sub_channels():
    conn = sqlite3.connect('bot_data.db')
    c = conn.cursor()
    c.execute('SELECT channel_username, channel_title FROM force_sub_channels WHERE is_active = 1 ORDER BY channel_title')
    rows = c.fetchall()
    conn.close()
    return rows

def get_force_sub_channel_info(channel_username):
    conn = sqlite3.connect('bot_data.db')
    c = conn.cursor()
    c.execute('SELECT channel_username, channel_title FROM force_sub_channels WHERE channel_username = ? AND is_active = 1', (channel_username,))
    row = c.fetchone()
    conn.close()
    return row

def delete_force_sub_channel(channel_username):
    conn = sqlite3.connect('bot_data.db')
    c = conn.cursor()
    c.execute('UPDATE force_sub_channels SET is_active = 0 WHERE channel_username = ?', (channel_username,))
    conn.commit()
    conn.close()

def generate_link_id(channel_username, user_id):
    link_id = secrets.token_urlsafe(16)
    conn = sqlite3.connect('bot_data.db')
    c = conn.cursor()
    c.execute('''
        INSERT OR REPLACE INTO generated_links (link_id, channel_username, user_id, created_time)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP)
    ''', (link_id, channel_username, user_id))
    conn.commit()
    conn.close()
    return link_id

def get_link_info(link_id):
    conn = sqlite3.connect('bot_data.db')
    c = conn.cursor()
    c.execute('''
        SELECT channel_username, user_id, created_time 
        FROM generated_links WHERE link_id = ?
    ''', (link_id,))
    row = c.fetchone()
    conn.close()
    return row

async def check_force_subscription(user_id, context: ContextTypes.DEFAULT_TYPE):
    channels = get_all_force_sub_channels()
    not_joined = []
    for ch_username, ch_title in channels:
        try:
            member = await context.bot.get_chat_member(ch_username, user_id)
            if member.status in ['left', 'kicked']:
                not_joined.append((ch_username, ch_title))
        except Exception as e:
            logger.error("Error checking subscription for %s: %s", ch_username, e)
    return not_joined

def is_admin(user_id):
    return user_id == ADMIN_ID

async def send_admin_menu(chat_id, context: ContextTypes.DEFAULT_TYPE, query=None):
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
    text = "👨‍💼 <b>ADMIN PANEL</b>\nChoose an option:"
    await context.bot.send_message(chat_id=chat_id, text=text, parse_mode='HTML', reply_markup=reply_markup)

async def send_admin_stats(query, context: ContextTypes.DEFAULT_TYPE):
    try:
        await query.delete_message()
    except Exception:
        pass
    uc = get_user_count()
    fc = get_force_sub_channel_count()
    stats = (
        f"📊 <b>BOT STATISTICS</b>\n\n"
        f"👥 Total users: {uc}\n"
        f"📢 Force-sub channels: {fc}\n"
        f"🔗 Link expiry: {LINK_EXPIRY_MINUTES} minutes"
    )
    keyboard = [
        [InlineKeyboardButton("🔄 REFRESH", callback_data="admin_stats")],
        [InlineKeyboardButton("🔙 BACK", callback_data="admin_back")]
    ]
    await context.bot.send_message(chat_id=query.message.chat_id, text=stats, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))

async def show_force_sub_management(query, context: ContextTypes.DEFAULT_TYPE):
    channels = get_all_force_sub_channels()
    text = "📢 <b>FORCE SUBSCRIPTION CHANNELS</b>\n\n"
    if not channels:
        text += "No channels configured currently."
    else:
        text += "<b>Configured Channels:</b>\n"
        for uname, title in channels:
            text += f"• {title} (<code>{uname}</code>)\n"
    keyboard = [[InlineKeyboardButton("➕ ADD NEW CHANNEL", callback_data="add_channel_start")]]
    if channels:
        buttons = [InlineKeyboardButton(title, callback_data=f"channel_{uname.lstrip('@')}") for uname, title in channels]
        grouped = [buttons[i:i+2] for i in range(0, len(buttons), 2)]
        keyboard.extend(grouped)
        keyboard.append([InlineKeyboardButton("🗑️ DELETE CHANNEL", callback_data="delete_channel_prompt")])
    keyboard.append([InlineKeyboardButton("🔙 BACK TO MENU", callback_data="admin_back")])
    try:
        await query.delete_message()
    except Exception:
        pass
    await context.bot.send_message(chat_id=query.message.chat_id, text=text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))

async def show_channel_details(query, context: ContextTypes.DEFAULT_TYPE, channel_username_clean: str):
    uname = '@' + channel_username_clean
    info = get_force_sub_channel_info(uname)
    if not info:
        await query.edit_message_text("❌ Channel not found.", parse_mode='HTML', reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 BACK", callback_data="manage_force_sub")]]))
        return
    _, title = info
    text = (
        f"📢 <b>CHANNEL DETAILS</b>\n\n"
        f"<b>Title:</b> {title}\n"
        f"<b>Username:</b> <code>{uname}</code>\n"
        f"<i>Choose an action below:</i>"
    )
    keyboard = [
        [InlineKeyboardButton("🔗 GENERATE TEMP LINK", callback_data=f"genlink_{channel_username_clean}")],
        [InlineKeyboardButton("🗑️ DELETE CHANNEL", callback_data=f"delete_{channel_username_clean}")],
        [InlineKeyboardButton("🔙 BACK TO MANAGEMENT", callback_data="manage_force_sub")]
    ]
    await query.edit_message_text(text=text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))

async def send_user_management(query, context: ContextTypes.DEFAULT_TYPE, offset=0):
    if not query.data.startswith("user_page_"):
        try:
            await query.delete_message()
        except Exception:
            pass
    total = get_user_count()
    users = get_all_users(limit=10, offset=offset)
    has_next = total > offset + 10
    has_prev = offset > 0
    text = "👤 <b>USER MANAGEMENT</b>\n\n"
    text += f"<b>Total Users:</b> {total}\n"
    text += f"<b>Showing:</b> {offset+1}-{min(offset+10, total)}\n\n"
    if users:
        for uid, uname, fname, lname, jdate in users:
            name = f"{fname or ''} {lname or ''}".strip() or "N/A"
            disp_uname = f"@{uname}" if uname else f"ID: {uid}"
            try:
                dstr = datetime.fromisoformat(jdate).strftime('%Y-%m-%d %H:%M')
            except:
                dstr = "Unknown"
            text += f"<b>{name}</b> ({disp_uname})\nJoined: {dstr}\n\n"
    else:
        text += "No users found."
    keyboard = []
    nav = []
    if has_prev:
        nav.append(InlineKeyboardButton("⬅️ PREV", callback_data=f"user_page_{offset-10}"))
    if has_next:
        nav.append(InlineKeyboardButton("NEXT ➡️", callback_data=f"user_page_{offset+10}"))
    if nav:
        keyboard.append(nav)
    keyboard.append([InlineKeyboardButton("🔄 REFRESH", callback_data="user_management")])
    keyboard.append([InlineKeyboardButton("🔙 BACK TO MENU", callback_data="admin_back")])
    if query.data.startswith("user_page_"):
        await query.edit_message_text(text=text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await context.bot.send_message(chat_id=query.message.chat_id, text=text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    add_user(user.id, user.username, user.first_name, user.last_name)

    # If /start <link_id> is used
    if context.args:
        link_id = context.args[0]
        await handle_channel_link_deep(update, context, link_id)
        return

    # If not admin, enforce force subscription
    if not is_admin(user.id):
        not_joined = await check_force_subscription(user.id, context)
        if not_joined:
            keyboard = []
            for ch_uname, ch_title in not_joined:
                keyboard.append([InlineKeyboardButton(f"📢 JOIN {ch_title}", url=f"https://t.me/{ch_uname.lstrip('@')}")])
            keyboard.append([InlineKeyboardButton("✅ VERIFY SUBSCRIPTION", callback_data="verify_subscription")])
            reply_markup = InlineKeyboardMarkup(keyboard)
            txt = (
                "📢 <b>Please join our required channels to use this bot.</b>\n\n"
                "<b>Required channels:</b>\n"
                + "\n".join(f"• {title}" for _, title in not_joined)
                + "\n\nJoin them all, then click Verify Subscription."
            )
            await update.message.reply_text(txt, parse_mode='HTML', reply_markup=reply_markup)
            return

    # Admin or user fully subscribed
    if is_admin(user.id):
        await send_admin_menu(update.effective_chat.id, context)
    else:
        keyboard = [
            [InlineKeyboardButton("ANIME CHANNEL", url=PUBLIC_ANIME_CHANNEL_URL)],
            [InlineKeyboardButton("CONTACT ADMIN", url=f"https://t.me/{ADMIN_CONTACT_USERNAME}")],
            [InlineKeyboardButton("REQUEST ANIME CHANNEL", url=REQUEST_CHANNEL_URL)],
            [
                InlineKeyboardButton("ABOUT", callback_data="about_bot"),
                InlineKeyboardButton("CLOSE", callback_data="close_message")
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
            logger.error("Error copying welcome message: %s", e)
            fallback = "👋 Welcome! Use this bot to get your invite link."
            await update.message.reply_text(fallback, parse_mode='HTML', reply_markup=reply_markup)

async def handle_channel_link_deep(update: Update, context: ContextTypes.DEFAULT_TYPE, link_id: str):
    link_info = get_link_info(link_id)
    if not link_info:
        await update.message.reply_text("❌ This link is invalid or not registered.", parse_mode='HTML')
        return

    channel_identifier, creator_id, created_time = link_info

    user = update.effective_user
    not_joined = await check_force_subscription(user.id, context)
    if not_joined:
        keyboard = [
            [InlineKeyboardButton(f"📢 JOIN {title}", url=f"https://t.me/{uname.lstrip('@')}")]
            for uname, title in not_joined
        ]
        keyboard.append([InlineKeyboardButton("✅ VERIFY SUBSCRIPTION", callback_data=f"verify_deep_{link_id}")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        txt = (
            "📢 <b>Please join required channels first.</b>\n\n"
            "<b>Required:</b>\n"
            + "\n".join(f"• {title}" for _, title in not_joined)
        )
        await update.message.reply_text(txt, parse_mode='HTML', reply_markup=reply_markup)
        return

    # Everything ok, generate 5-min invite link
    try:
        ident = channel_identifier
        if isinstance(ident, str) and ident.lstrip('-').isdigit():
            ident = int(ident)
        chat = await context.bot.get_chat(ident)
        invite = await context.bot.create_chat_invite_link(
            chat.id,
            expire_date=datetime.now().timestamp() + LINK_EXPIRY_MINUTES * 60
        )
        msg = (
            f"<b>CHANNEL:</b> {chat.title}\n"
            f"<b>Expires in:</b> {LINK_EXPIRY_MINUTES} minutes\n"
            f"<b>Usage:</b> Multiple uses within period\n\n"
            f"Click below:"
        )
        keyboard = [[InlineKeyboardButton("🔗 Request to Join", url=invite.invite_link)]]
        await update.message.reply_text(msg, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception as e:
        logger.error("Error generating invite link: %s", e)
        await update.message.reply_text("❌ Could not create invite link. Contact admin.", parse_mode='HTML')

async def broadcast_message_to_all_users(update: Update, context: ContextTypes.DEFAULT_TYPE, message_to_copy):
    users = get_all_users(limit=None, offset=0)
    total = len(users)
    sent = 0
    await update.message.reply_text(f"🔄 Broadcasting to {total} users...", parse_mode='HTML')
    for (uid, *_rest) in users:
        try:
            await context.bot.copy_message(chat_id=uid, from_chat_id=message_to_copy.chat_id, message_id=message_to_copy.message_id)
            sent += 1
        except Exception as e:
            logger.warning("Failed to send to %s: %s", uid, e)
        await asyncio.sleep(0.1)
    await context.bot.send_message(chat_id=update.effective_chat.id, text=f"✅ Broadcast done: {sent}/{total}", parse_mode='HTML')

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    data = query.data

    # Clear state if back to main menus
    if user_id in user_states:
        if user_states[user_id] in [PENDING_BROADCAST, GENERATE_LINK_CHANNEL_USERNAME, ADD_CHANNEL_TITLE, ADD_CHANNEL_USERNAME] and data in ["admin_back", "admin_stats", "manage_force_sub", "generate_links", "user_management"]:
            del user_states[user_id]

    if data == "close_message":
        try:
            await query.delete_message()
        except:
            pass
        return

    if data == "admin_broadcast_start":
        if not is_admin(user_id):
            await query.edit_message_text("❌ Admin only.", parse_mode='HTML')
            return
        user_states[user_id] = PENDING_BROADCAST
        try:
            await query.delete_message()
        except:
            pass
        await context.bot.send_message(chat_id=query.message.chat_id, text="📣 Send me the message to broadcast now:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 CANCEL", callback_data="admin_back")]]), parse_mode='HTML')
        return

    if data == "verify_subscription":
        not_joined = await check_force_subscription(user_id, context)
        if not_joined:
            txt = (
                "❌ <b>You haven't joined all required channels.</b>\n\n"
                + "\n".join(f"• {title}" for _, title in not_joined)
                + "\nPlease join and try again."
            )
            await query.edit_message_text(txt, parse_mode='HTML')
            return
        # Now proceed to main menu or welcome
        if is_admin(user_id):
            await send_admin_menu(query.message.chat_id, context)
        else:
            keyboard = [
                [InlineKeyboardButton("ANIME CHANNEL", url=PUBLIC_ANIME_CHANNEL_URL)],
                [InlineKeyboardButton("CONTACT ADMIN", url=f"https://t.me/{ADMIN_CONTACT_USERNAME}")],
                [InlineKeyboardButton("REQUEST ANIME CHANNEL", url=REQUEST_CHANNEL_URL)],
                [
                    InlineKeyboardButton("ABOUT", callback_data="about_bot"),
                    InlineKeyboardButton("CLOSE", callback_data="close_message")
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
                logger.error("Error copying welcome: %s", e)
                await context.bot.send_message(query.message.chat_id, "✅ Subscription verified!", parse_mode='HTML', reply_markup=reply_markup)

    elif data.startswith("verify_deep_"):
        link_id = data[len("verify_deep_"):]
        not_joined = await check_force_subscription(user_id, context)
        if not_joined:
            txt = (
                "❌ <b>You haven't joined required channels.</b>\n\n"
                + "\n".join(f"• {title}" for _, title in not_joined)
                + "\nPlease join and try again."
            )
            await query.edit_message_text(txt, parse_mode='HTML')
            return
        await handle_channel_link_deep(update, context, link_id)
        return

    elif data == "admin_stats":
        if is_admin(user_id):
            await send_admin_stats(query, context)
        else:
            await query.edit_message_text("❌ Admin only.", parse_mode='HTML')

    elif data == "user_management":
        if is_admin(user_id):
            await send_user_management(query, context, offset=0)
        else:
            await query.edit_message_text("❌ Admin only.", parse_mode='HTML')

    elif data.startswith("user_page_"):
        if is_admin(user_id):
            try:
                off = int(data[len("user_page_"):])
            except:
                off = 0
            await send_user_management(query, context, offset=off)
        else:
            await query.edit_message_text("❌ Admin only.", parse_mode='HTML')

    elif data == "manage_force_sub":
        if is_admin(user_id):
            await show_force_sub_management(query, context)
        else:
            await query.edit_message_text("❌ Admin only.", parse_mode='HTML')

    elif data == "generate_links":
        if is_admin(user_id):
            user_states[user_id] = GENERATE_LINK_CHANNEL_USERNAME
            try:
                await query.delete_message()
            except:
                pass
            await context.bot.send_message(chat_id=query.message.chat_id, text=(
                "🔗 Generate channel deep-links\n"
                "Send me the channel username (@YourChannel) or channel ID (-100...)\n"
            ), reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 CANCEL", callback_data="admin_back")]]), parse_mode='HTML')
        else:
            await query.edit_message_text("❌ Admin only.", parse_mode='HTML')

    elif data == "add_channel_start":
        if is_admin(user_id):
            user_states[user_id] = ADD_CHANNEL_USERNAME
            try:
                await query.delete_message()
            except:
                pass
            await context.bot.send_message(chat_id=query.message.chat_id, text=(
                "📢 Add force-sub channel\n"
                "Send channel username (starting with @):"
            ), reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 CANCEL", callback_data="manage_force_sub")]]), parse_mode='HTML')
        else:
            await query.edit_message_text("❌ Admin only.", parse_mode='HTML')

    elif data.startswith("channel_"):
        if is_admin(user_id):
            await show_channel_details(query, context, data[len("channel_"):])
        else:
            await query.edit_message_text("❌ Admin only.", parse_mode='HTML')

    elif data.startswith("delete_"):
        if is_admin(user_id):
            clean = data[len("delete_"):]
            uname = '@' + clean
            info = get_force_sub_channel_info(uname)
            if info:
                keyboard = [
                    [InlineKeyboardButton("✅ YES, DELETE", callback_data=f"confirm_delete_{clean}")],
                    [InlineKeyboardButton("❌ NO, CANCEL", callback_data=f"channel_{clean}")]
                ]
                await query.edit_message_text(
                    f"🗑️ Confirm deletion of {info[1]} ({info[0]})?", parse_mode='HTML',
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
        else:
            await query.edit_message_text("❌ Admin only.", parse_mode='HTML')

    elif data.startswith("confirm_delete_"):
        if is_admin(user_id):
            clean = data[len("confirm_delete_"):]
            uname = '@' + clean
            delete_force_sub_channel(uname)
            await query.edit_message_text(f"✅ Channel {uname} deleted.", parse_mode='HTML', reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📢 Manage Channels", callback_data="manage_force_sub")]]))
        else:
            await query.edit_message_text("❌ Admin only.", parse_mode='HTML')

    elif data in ["admin_back", "user_back", "channels_back"]:
        if is_admin(user_id):
            await send_admin_menu(query.message.chat_id, context, query)
        else:
            keyboard = [
                [InlineKeyboardButton("ANIME CHANNEL", url=PUBLIC_ANIME_CHANNEL_URL)],
                [InlineKeyboardButton("CONTACT ADMIN", url=f"https://t.me/{ADMIN_CONTACT_USERNAME}")],
                [InlineKeyboardButton("REQUEST ANIME CHANNEL", url=REQUEST_CHANNEL_URL)],
                [
                    InlineKeyboardButton("ABOUT", callback_data="about_bot"),
                    InlineKeyboardButton("CLOSE", callback_data="close_message")
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
                logger.error("Error sending main menu: %s", e)
                await context.bot.send_message(query.message.chat_id, "Main menu", parse_mode='HTML', reply_markup=reply_markup)

    elif data == "about_bot":
        about_text = (
            "<b>About Us</b>\n\n"
            "Developed by @Beat_Anime_Ocean\n"
        )
        keyboard = [[InlineKeyboardButton("🔙 BACK", callback_data="user_back")]]
        try:
            await query.delete_message()
        except:
            pass
        await context.bot.send_message(chat_id=query.message.chat_id, text=about_text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))


async def handle_admin_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid not in user_states:
        return
    state = user_states[uid]

    if state == PENDING_BROADCAST:
        del user_states[uid]
        await broadcast_message_to_all_users(update, context, update.message)
        await send_admin_menu(update.effective_chat.id, context)
        return

    text = update.message.text
    if text is None:
        await update.message.reply_text("❌ Send text please.", parse_mode='HTML')
        return

    if state == ADD_CHANNEL_USERNAME:
        if not text.startswith('@'):
            await update.message.reply_text("❌ Username must start with @. Try again:", parse_mode='HTML')
            return
        context.user_data['channel_username'] = text
        user_states[uid] = ADD_CHANNEL_TITLE
        await update.message.reply_text("Send me the channel's display title:", parse_mode='HTML', reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 CANCEL", callback_data="manage_force_sub")]]))
    elif state == ADD_CHANNEL_TITLE:
        chan_uname = context.user_data.get('channel_username')
        chan_title = text.strip()
        if add_force_sub_channel(chan_uname, chan_title):
            del user_states[uid]
            context.user_data.pop('channel_username', None)
            await update.message.reply_text(f"✅ Channel {chan_uname} ({chan_title}) added.", parse_mode='HTML', reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📢 Manage Channels", callback_data="manage_force_sub")]]))
        else:
            await update.message.reply_text("❌ Error adding channel. It might already exist.", parse_mode='HTML')
    elif state == GENERATE_LINK_CHANNEL_USERNAME:
        chan_id = text.strip()
        if not (chan_id.startswith('@') or chan_id.startswith('-100') or chan_id.lstrip('-').isdigit()):
            await update.message.reply_text("❌ Invalid. Send @username or channel ID (-100...):", parse_mode='HTML')
            return
        del user_states[uid]
        try:
            chat = await context.bot.get_chat(chan_id)
            title = chat.title
        except Exception as e:
            logger.error("Error fetching channel %s: %s", chan_id, e)
            await update.message.reply_text("❌ Cannot access that channel. Make sure bot is admin.", parse_mode='HTML')
            return
        link_id = generate_link_id(chan_id, uid)
        botname = context.bot.username
        deep = f"https://t.me/{botname}?start={link_id}"
        await update.message.reply_text(
            f"🔗 Deep link generated:\n\n<b>Channel:</b> {title}\n<b>ID/Username:</b> <code>{chan_id}</code>\n"
            f"<b>Expires in:</b> {LINK_EXPIRY_MINUTES} minutes\n\n<code>{deep}</code>",
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 BACK", callback_data="admin_back")]])
        )

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error("Exception during update: %s", context.error)

async def cleanup_task(context: ContextTypes.DEFAULT_TYPE):
    # Optionally, remove very old links from DB
    conn = sqlite3.connect('bot_data.db')
    c = conn.cursor()
    # Keep generated_links though; we don’t delete by usage
    cutoff = datetime.now() - timedelta(days=7)
    c.execute('DELETE FROM generated_links WHERE datetime(created_time) < ?', (cutoff.isoformat(),))
    conn.commit()
    conn.close()

def main():
    init_db()
    if not BOT_TOKEN or BOT_TOKEN == "YOUR_TOKEN_HERE":
        logger.error("BOT_TOKEN not set.")
        return

    application = Application.builder().token(BOT_TOKEN).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(button_handler))
    admin_filter = filters.User(user_id=ADMIN_ID)
    application.add_handler(MessageHandler(admin_filter & ~filters.COMMAND, handle_admin_message))
    application.add_error_handler(error_handler)

    jobq = application.job_queue
    if jobq:
        jobq.run_repeating(cleanup_task, interval=3600, first=60)

    if WEBHOOK_URL and BOT_TOKEN:
        t = Thread(target=keep_alive, daemon=True)
        t.start()
        application.run_webhook(
            listen="0.0.0.0",
            port=PORT,
            url_path=BOT_TOKEN,
            webhook_url=WEBHOOK_URL + BOT_TOKEN
        )
    else:
        application.run_polling()

if __name__ == '__main__':
    main()
