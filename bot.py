import os
import logging
import sqlite3
import secrets
import re
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, MessageHandler, filters
import asyncio

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

# =================================================================
# ⚙️ CUSTOMIZATION CONSTANTS
# =================================================================
WELCOME_SOURCE_CHANNEL = -1002530952988
WELCOME_SOURCE_MESSAGE_ID = 32  

PUBLIC_ANIME_CHANNEL_URL = "https://t.me/BeatAnime"
REQUEST_CHANNEL_URL = "https://t.me/Beat_Hindi_Dubbed"
ADMIN_CONTACT_USERNAME = "Beat_Anime_Ocean" 
# =================================================================

# User states
ADD_CHANNEL_USERNAME, ADD_CHANNEL_TITLE, GENERATE_LINK_CHANNEL_USERNAME, PENDING_BROADCAST = range(4)
user_states = {}

# --- Escape MarkdownV2 ---
def escape_markdown_v2(text: str) -> str:
    if not text:
        return ""
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    text = text.replace('\\', '\\\\')
    return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', str(text))

# --- DATABASE INITIALIZATION ---
def init_db():
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        first_name TEXT,
        last_name TEXT,
        joined_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS force_sub_channels (
        channel_id INTEGER PRIMARY KEY AUTOINCREMENT,
        channel_username TEXT UNIQUE,
        channel_title TEXT,
        added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        is_active BOOLEAN DEFAULT 1
    )''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS generated_links (
        link_id TEXT PRIMARY KEY,
        channel_username TEXT,
        user_id INTEGER,
        created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        is_used BOOLEAN DEFAULT 0
    )''')
    conn.commit()
    conn.close()

# --- DB HELPERS ---
def add_user(user_id, username, first_name, last_name):
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    cursor.execute('''INSERT OR REPLACE INTO users (user_id, username, first_name, last_name)
                      VALUES (?, ?, ?, ?)''', (user_id, username, first_name, last_name))
    conn.commit()
    conn.close()

def get_all_users(limit=None, offset=0):
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    if limit:
        cursor.execute('SELECT user_id, username, first_name, last_name, joined_date FROM users ORDER BY joined_date DESC LIMIT ? OFFSET ?', (limit, offset))
    else:
        cursor.execute('SELECT user_id, username, first_name, last_name, joined_date FROM users ORDER BY joined_date DESC')
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
        cursor.execute('''INSERT OR IGNORE INTO force_sub_channels (channel_username, channel_title)
                          VALUES (?, ?)''', (channel_username, channel_title))
        conn.commit()
        return cursor.rowcount > 0
    except Exception as e:
        logger.error(f"DB Error adding channel: {e}")
        return False
    finally:
        conn.close()
# --- FORCE SUB CHECK (async) ---
async def check_force_subscription(user_id, context):
    """
    Returns list of (channel_username, channel_title) that the user hasn't joined.
    """
    channels = get_all_force_sub_channels()
    not_joined_channels = []
    for channel_username, channel_title in channels:
        try:
            member = await context.bot.get_chat_member(channel_username, user_id)
            # member.status could be 'member', 'administrator', 'creator', 'left', 'kicked'
            if getattr(member, "status", None) in ('left', 'kicked'):
                not_joined_channels.append((channel_username, channel_title))
        except Exception as e:
            # Could be bot not in channel, invalid channel, or API error.
            logger.error(f"Error checking subscription for {channel_username}: {e}")
            # In doubt, treat as not joined so user is prompted to join (safer)
            not_joined_channels.append((channel_username, channel_title))
    return not_joined_channels

# --- ADMIN CHECK ---
def is_admin(user_id):
    return user_id == ADMIN_ID

# --- ADMIN MENU ---
async def send_admin_menu(chat_id, context, query=None):
    """
    Send the admin main menu. If query provided, try to delete the callback message first.
    """
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

    text = (
        rf"👑 **ADMIN PANEL** 👑\n\n"
        rf"Welcome back, Admin\! Choose an option below\:"
    )

    await context.bot.send_message(chat_id=chat_id, text=text, parse_mode='MarkdownV2', reply_markup=reply_markup)


async def send_admin_stats(query, context):
    """
    Sends statistics to the admin.
    """
    try:
        await query.delete_message()
    except Exception:
        pass

    user_count = get_user_count()
    channel_count = get_force_sub_channel_count()

    stats_text = rf"""
📊 **BOT STATISTICS** 📊

👥 **Total Users:** {escape_markdown_v2(str(user_count))}
📺 **Force Sub Channels:** {escape_markdown_v2(str(channel_count))}
🔗 **Link Expiry:** {escape_markdown_v2(str(LINK_EXPIRY_MINUTES))} minutes

*Last Cleanup:* {escape_markdown_v2(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))}
    """

    keyboard = [
        [InlineKeyboardButton("🔄 REFRESH", callback_data="admin_stats")],
        [InlineKeyboardButton("🔙 BACK", callback_data="admin_back")]
    ]

    await context.bot.send_message(
        chat_id=query.message.chat_id,
        text=stats_text,
        parse_mode='MarkdownV2',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


# --- FORCE SUB CHANNEL MANAGEMENT UI ---
async def show_force_sub_management(query, context):
    """
    Show list of configured force-sub channels and option to add/delete.
    """
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

    if channels:
        # create a button for each channel (callback includes username)
        channel_buttons = [
            InlineKeyboardButton(escape_markdown_v2(channel_title), callback_data=f"channel_{channel_username}")
            for channel_username, channel_title in channels
        ]
        # group into rows of 2 for neatness
        grouped = [channel_buttons[i:i + 2] for i in range(0, len(channel_buttons), 2)]
        for row in grouped:
            keyboard.append(row)

        keyboard.append([InlineKeyboardButton("🗑️ DELETE CHANNEL", callback_data="delete_channel_prompt")])
        keyboard.append([InlineKeyboardButton("ℹ️ CHANNEL DETAILS", callback_data="channel_details_prompt")])

    keyboard.append([InlineKeyboardButton("🔙 BACK TO MENU", callback_data="admin_back")])

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
    """
    Show detailed info for single channel with actions.
    """
    channel_info = get_force_sub_channel_info(channel_username)
    if not channel_info:
        await query.edit_message_text(
            r"❌ Channel not found\.",
            parse_mode='MarkdownV2',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 MANAGE CHANNELS", callback_data="manage_force_sub")]])
        )
        return

    ch_username, ch_title = channel_info
    safe_title = escape_markdown_v2(ch_title)
    safe_username = escape_markdown_v2(ch_username)

    details_text = rf"""
📺 **CHANNEL DETAILS** 📺

**Title:** {safe_title}
**Username:** {safe_username}
**Status:** *Active Force Sub*

_Choose an action below\._
    """

    keyboard = [
        [InlineKeyboardButton("🔗 GENERATE TEMP LINK", callback_data=f"genlink_{ch_username}")],
        [InlineKeyboardButton("🗑️ DELETE CHANNEL", callback_data=f"delete_{ch_username}")],
        [InlineKeyboardButton("🔙 BACK TO MANAGEMENT", callback_data="manage_force_sub")]
    ]

    await query.edit_message_text(text=details_text, parse_mode='MarkdownV2', reply_markup=InlineKeyboardMarkup(keyboard))


# --- USER MANAGEMENT UI ---
async def send_user_management(query, context, offset=0):
    """
    Show paginated user list (10 per page).
    """
    # delete original message only for initial click (non-pagination)
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

        safe_display_name = escape_markdown_v2(display_name)
        safe_display_username = escape_markdown_v2(display_username)

        # joined_date comes as string from DB; try to format safely
        joined_str = escape_markdown_v2(str(joined_date))
        user_list_text += f"**{safe_display_name}** (`{safe_display_username}`)\n"
        user_list_text += f"Joined: {joined_str}\n\n"

    if not user_list_text:
        user_list_text = r"No users found in the database\."

    stats_text = rf"""
👥 **USER MANAGEMENT** 👥

**Total Users:** {escape_markdown_v2(str(user_count))}
**Showing:** {escape_markdown_v2(str(offset + 1))}\-{escape_markdown_v2(str(min(offset + 10, user_count)))} of {escape_markdown_v2(str(user_count))}

{user_list_text}
    """

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
        await query.edit_message_text(text=stats_text, parse_mode='MarkdownV2', reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await context.bot.send_message(chat_id=query.message.chat_id, text=stats_text, parse_mode='MarkdownV2', reply_markup=InlineKeyboardMarkup(keyboard))


# --- START HANDLER (handles /start and deep-link start=) ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Send welcome, handle deep links and force-join check.
    """
    user = update.effective_user
    add_user(user.id, user.username, user.first_name, user.last_name)

    # Deep-link handling: /start <link_id>
    if context.args and len(context.args) > 0:
        link_id = context.args[0]
        await handle_channel_link_deep(update, context, link_id)
        return

    # Force subscription check (non-admin users)
    if not is_admin(user.id):
        not_joined_channels = await check_force_subscription(user.id, context)
        if not_joined_channels:
            keyboard = []
            for channel_username, channel_title in not_joined_channels:
                # Button label can show the channel title (escaped), but the URL must be plain t.me/<username>
                safe_title = escape_markdown_v2(channel_title)
                # show title in button text (escape applied to text, buttons don't accept parse_mode)
                keyboard.append([InlineKeyboardButton(f"📢 JOIN {channel_title}", url=f"https://t.me/{channel_username.lstrip('@')}")])

            keyboard.append([InlineKeyboardButton("✅ VERIFY SUBSCRIPTION", callback_data="verify_subscription")])
            reply_markup = InlineKeyboardMarkup(keyboard)

            channels_text = "\n".join([f"• {escape_markdown_v2(title)} (`{escape_markdown_v2(username)}`)" for username, title in not_joined_channels])
            await update.message.reply_text(
                rf"📢 **Please join our channels to use this bot\!**\n\n"
                rf"**Required Channels:**\n{channels_text}\n\n"
                r"Join all channels above and then click Verify Subscription\.",
                parse_mode='MarkdownV2',
                reply_markup=reply_markup
            )
            return

    # If admin -> admin menu
    if is_admin(user.id):
        await send_admin_menu(update.effective_chat.id, context)
        return

    # Normal user and passed force-sub check: send/copy welcome message
    keyboard = [
        [InlineKeyboardButton("ANIME CHANNEL", url=PUBLIC_ANIME_CHANNEL_URL),
         InlineKeyboardButton("REQUEST ANIME CHANNEL", url=REQUEST_CHANNEL_URL)],
        [InlineKeyboardButton("CONTACT ADMIN", url=f"https://t.me/{ADMIN_CONTACT_USERNAME}"),
         InlineKeyboardButton("ABOUT ME", callback_data="about_bot")],
        [InlineKeyboardButton("CLOSE", callback_data="close_message")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    try:
        # copy the welcome message from source channel (media + text)
        await context.bot.copy_message(
            chat_id=update.effective_chat.id,
            from_chat_id=WELCOME_SOURCE_CHANNEL,
            message_id=WELCOME_SOURCE_MESSAGE_ID,
            reply_markup=reply_markup
        )
    except Exception as e:
        logger.error(f"Error copying welcome message from channel: {e}")
        fallback_text = (
            r"👋 *WELCOME TO THE ADVANCED LINKS SHARING BOT\.*\n\n"
            r"USE THIS BOT TO SAFELY SHARE CONTENT WITHOUT RISKING COPYRIGHT TAKEDOWNS\.\n"
            r"EXPLORE THE OPTIONS BELOW TO GET STARTED\!"
        )
        await update.message.reply_text(fallback_text, parse_mode='MarkdownV2', reply_markup=reply_markup)
# --- DEEP LINK HANDLER ---
async def handle_channel_link_deep(update: Update, context: ContextTypes.DEFAULT_TYPE, link_id: str):
    """
    Handle /start <link_id> deep link: verify, check force-join, create invite.
    """
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
            keyboard.append([InlineKeyboardButton(f"📢 JOIN {chan_title}", url=f"https://t.me/{chan_user.lstrip('@')}")])
        keyboard.append([InlineKeyboardButton("✅ VERIFY SUBSCRIPTION", callback_data=f"verify_deep_{link_id}")])
        reply_markup = InlineKeyboardMarkup(keyboard)

        channels_text = "\n".join([f"• {escape_markdown_v2(title)}" for _, title in not_joined_channels])
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
            expire_date=int(datetime.now().timestamp()) + (LINK_EXPIRY_MINUTES * 60)
        )
        mark_link_used(link_id)

        success_message = (
            rf"🎉 *Access Granted\!* 🎉\n\n"
            rf"*Channel:* {escape_markdown_v2(chat.title)}\n"
            rf"*Expires in:* {LINK_EXPIRY_MINUTES} minutes\n"
            rf"*Usage:* Single use\n\n"
            r"_Enjoy the content\! 🍿_"
        )
        keyboard = [[InlineKeyboardButton("🔔 Request to Join", url=invite_link.invite_link)]]
        await update.message.reply_text(success_message, parse_mode='MarkdownV2', reply_markup=InlineKeyboardMarkup(keyboard))

    except Exception as e:
        logger.error(f"Error generating invite link for {channel_username}: {e}")
        await update.message.reply_text(
            r"❌ Error generating access link\. Make sure the bot is an *Admin* in the target channel and has the right to create invite links\.",
            parse_mode='MarkdownV2'
        )


# --- BROADCAST ---
async def broadcast_message_to_all_users(update: Update, context: ContextTypes.DEFAULT_TYPE, message_to_copy):
    """
    Copy the admin's message to all users.
    """
    users = get_all_users(limit=None, offset=0)
    total_users = len(users)
    success_count = 0

    await update.message.reply_text(
        rf"🚀 Starting broadcast to {escape_markdown_v2(str(total_users))} users\. Please wait\.",
        parse_mode='MarkdownV2'
    )

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
        text=rf"✅ **Broadcast complete\!**\n\n📊 Sent to {escape_markdown_v2(str(success_count))}/{escape_markdown_v2(str(total_users))} users\.",
        parse_mode='MarkdownV2'
    )


# --- BUTTON HANDLER ---
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    data = query.data

    # Reset state if user goes back to menu
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
        try:
            await query.delete_message()
        except Exception:
            pass
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text=(r"📢 **MEDIA BROADCAST MODE**\n\n"
                  r"Please **forward** the message (image, video, file, sticker, or text with caption) you wish to broadcast *now*\.\n\n"
                  r"**Note:** Any message you send next will be copied to all users\."),
            parse_mode='MarkdownV2',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

    # --- VERIFY SUBSCRIPTION ---
    if data == "verify_subscription":
        not_joined_channels = await check_force_subscription(user_id, context)
        if not_joined_channels:
            channels_text = "\n".join([f"• {escape_markdown_v2(title)}" for _, title in not_joined_channels])
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
                [InlineKeyboardButton("ANIME CHANNEL", url=PUBLIC_ANIME_CHANNEL_URL),
                 InlineKeyboardButton("REQUEST ANIME CHANNEL", url=REQUEST_CHANNEL_URL)],
                [InlineKeyboardButton("CONTACT ADMIN", url=f"https://t.me/{ADMIN_CONTACT_USERNAME}"),
                 InlineKeyboardButton("ABOUT ME", callback_data="about_bot")],
                [InlineKeyboardButton("CLOSE", callback_data="close_message")]
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
                logger.error(f"Error copying verified welcome message: {e}")
                fallback_text = r"✅ **Subscription verified\!**\n\nWelcome to the bot\! Explore the options below\:"
                await context.bot.send_message(query.message.chat_id, fallback_text, parse_mode='MarkdownV2', reply_markup=reply_markup)

    # --- VERIFY DEEP LINKS ---
    elif data.startswith("verify_deep_"):
        link_id = data[12:]
        not_joined_channels = await check_force_subscription(user_id, context)
        if not_joined_channels:
            channels_text = "\n".join([f"• {escape_markdown_v2(title)}" for _, title in not_joined_channels])
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
                expire_date=int(datetime.now().timestamp()) + (LINK_EXPIRY_MINUTES * 60)
            )
            mark_link_used(link_id)
            success_message = (
                rf"🎉 *Access Granted\!* 🎉\n\n"
                rf"*Channel:* {escape_markdown_v2(chat.title)}\n"
                rf"*Expires in:* {LINK_EXPIRY_MINUTES} minutes\n"
                rf"*Usage:* Single use\n\n"
                r"_Enjoy the content\! 🍿_"
            )
            keyboard = [[InlineKeyboardButton("🔔 Request to Join", url=invite_link.invite_link)]]
            try:
                await query.delete_message()
            except Exception:
                pass
            await context.bot.send_message(chat_id=query.message.chat_id, text=success_message, parse_mode='MarkdownV2', reply_markup=InlineKeyboardMarkup(keyboard))
        except Exception as e:
            logger.error(f"Error verifying deep link: {e}")
            await query.edit_message_text(
                r"❌ Error generating access link\. Make sure the bot is an *Admin* in the target channel and has the right to create invite links\.",
                parse_mode='MarkdownV2'
            )
# --- HANDLE ADMIN MESSAGES (state machine) ---
async def handle_admin_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in user_states:
        return

    state = user_states[user_id]

    # --- PENDING BROADCAST ---
    if state == PENDING_BROADCAST:
        if user_id in user_states:
            del user_states[user_id]
        await broadcast_message_to_all_users(update, context, update.message)
        await send_admin_menu(update.effective_chat.id, context)
        return

    # Text input expected for other states
    text = update.message.text
    if text is None:
        await update.message.reply_text(
            r"❌ Please send a text message as requested (e.g., a username or title)\.",
            parse_mode='MarkdownV2'
        )
        return

    # --- ADD CHANNEL: Step 1 username ---
    if state == ADD_CHANNEL_USERNAME:
        if not text.startswith('@'):
            await update.message.reply_text(
                r"❌ Please provide a valid channel username starting with @\. Try again\:",
                parse_mode='MarkdownV2'
            )
            return
        context.user_data['channel_username'] = text
        user_states[user_id] = ADD_CHANNEL_TITLE
        await update.message.reply_text(
            r"📝 **STEP 2: Channel Title**\n\n"
            r"Now please send me the display title for this channel\:\n\n"
            r"Example: `Anime Ocean Channel`",
            parse_mode='MarkdownV2',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 CANCEL", callback_data="manage_force_sub")]])
        )

    # --- ADD CHANNEL: Step 2 title ---
    elif state == ADD_CHANNEL_TITLE:
        channel_username = context.user_data.get('channel_username')
        channel_title = text
        if add_force_sub_channel(channel_username, channel_title):
            if user_id in user_states:
                del user_states[user_id]
            if 'channel_username' in context.user_data:
                del context.user_data['channel_username']
            safe_channel_username = escape_markdown_v2(channel_username)
            safe_channel_title = escape_markdown_v2(channel_title)
            await update.message.reply_text(
                rf"✅ **FORCE SUB CHANNEL ADDED SUCCESSFULLY\!**\n\n"
                rf"**Username:** {safe_channel_username}\n"
                rf"**Title:** {safe_channel_title}\n\n"
                r"Channel has been added to force subscription list\!",
                parse_mode='MarkdownV2',
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📺 MANAGE CHANNELS", callback_data="manage_force_sub")]])
            )
        else:
            await update.message.reply_text(
                r"❌ Error adding channel\. It might already exist or there was a database error\.",
                parse_mode='MarkdownV2'
            )

    # --- GENERATE LINK ---
    elif state == GENERATE_LINK_CHANNEL_USERNAME:
        channel_username = text.strip()
        if not channel_username.startswith('@'):
            await update.message.reply_text(
                r"❌ Please provide a valid channel username starting with @\. Try again\:",
                parse_mode='MarkdownV2'
            )
            return
        if user_id in user_states:
            del user_states[user_id]
        link_id = generate_link_id(channel_username, user_id)
        bot_username = context.bot.username
        safe_channel_username = escape_markdown_v2(channel_username)
        deep_link = f"https://t.me/{bot_username}?start={link_id}"
        await update.message.reply_text(
            rf"🔗 **LINK GENERATED** 🔗\n\n"
            rf"**Channel:** {safe_channel_username}\n"
            rf"**Expires in:** {LINK_EXPIRY_MINUTES} minutes\n\n"
            rf"**Direct Link:**\n`{deep_link}`\n\n"
            r"Share this link with users\!",
            parse_mode='MarkdownV2',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 BACK TO MENU", callback_data="admin_back")]])
        )


# --- ERROR HANDLER ---
async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Exception while handling an update: {context.error}")


# --- CLEANUP TASK ---
async def cleanup_task(context: ContextTypes.DEFAULT_TYPE):
    cleanup_expired_links()


# --- MAIN ---
def main():
    init_db()

    if not BOT_TOKEN:
        logger.error("BOT_TOKEN is missing. Please update it.")
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
        logger.warning("JobQueue not available. Cleanup disabled.")

    if WEBHOOK_URL and BOT_TOKEN:
        print(f"🤖 Starting Webhook listener on port {PORT}. Webhook URL: {WEBHOOK_URL + BOT_TOKEN}")
        application.run_webhook(
            listen="0.0.0.0",
            port=PORT,
            url_path=BOT_TOKEN,
            webhook_url=WEBHOOK_URL + BOT_TOKEN
        )
    else:
        print("🤖 Starting in Polling Mode...")
        application.run_polling()


if __name__ == '__main__':
    if 'PORT' not in os.environ:
        os.environ['PORT'] = str(8080)
    main()
