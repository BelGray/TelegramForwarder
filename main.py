import asyncio
import os
import random
import sys
from datetime import datetime, timezone

import aiomysql
from pyrogram import Client, filters, enums
from pyrogram.errors import FloodWait, UserDeactivated, AuthKeyUnregistered, SessionPasswordNeeded, PhoneCodeInvalid, PasswordHashInvalid

from env_loader import DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME, API_ID, API_HASH, ADMIN_ID
from log_config import logger

DB_CONFIG = {
    'host': DB_HOST,
    'port': DB_PORT,
    'user': DB_USER,
    'password': DB_PASS,
    'db': DB_NAME
}

HISTORY_LIMIT = 20

auth_states = {}   # {user_id: "STATE"}
temp_clients = {}  # {user_id: ClientObject}
auth_data = {}     # {user_id: {"phone": str, "hash": str}}

async def execute_query(query, params=None, fetch=None):
    conn = await aiomysql.connect(**DB_CONFIG)
    async with conn.cursor() as cur:
        await cur.execute(query, params)
        if fetch == 'all':
            result = await cur.fetchall()
        elif fetch == 'one':
            result = await cur.fetchone()
        else:
            await conn.commit()
            result = None
    conn.close()
    return result

async def save_new_account(phone, session_string):
    await execute_query(
        "INSERT INTO accounts (phone, session_string, status) VALUES (%s, %s, 'active')",
        (phone, session_string)
    )

async def get_active_sessions():
    return await execute_query("SELECT id, session_string, phone FROM accounts WHERE status = 'active'", fetch='all')


async def get_sources():
    rows = await execute_query("SELECT channel_link FROM sources", fetch='all')
    return [row[0] for row in rows] if rows else []


async def get_destinations_full():
    return await execute_query("SELECT chat_link, interval_minutes, last_sent_at, batch_size, send_mode FROM destinations",
                               fetch='all')


async def update_last_sent_time(chat_link):
    await execute_query("UPDATE destinations SET last_sent_at = UTC_TIMESTAMP() WHERE chat_link = %s", (chat_link,))


async def update_account_status(phone, status):
    await execute_query("UPDATE accounts SET status = %s WHERE phone = %s", (status, phone))


async def add_to_history(source_msg_id, account_id, status):
    await execute_query("INSERT INTO history (source_message_id, account_id, status) VALUES (%s, %s, %s)",
                        (source_msg_id, account_id, status))


async def revive_accounts():
    await execute_query(
        "UPDATE accounts SET status = 'active' WHERE status = 'flood_wait' AND last_used < (NOW() - INTERVAL 30 MINUTE)")


async def run_broadcaster():
    await revive_accounts()

    sessions = await get_active_sessions()
    if not sessions:
        logger.error("No active sessions found!")
        return

    clients = []
    for account_id, session_string, phone in sessions:
        app = Client(f"client_{phone}", api_id=API_ID, api_hash=API_HASH, session_string=session_string, in_memory=True)
        app.phone_number = phone
        app.account_id = account_id
        clients.append(app)

    admin_client = clients[0]

    @admin_client.on_message(filters.command("help") & filters.user(ADMIN_ID))
    async def help_cmd(client, message):
        text = (
            "<b>💻 SYSTEM MANAGEMENT PANEL v2.1</b>\n\n"
            "<b>1️⃣ SOURCES (ОТКУДА БРАТЬ)</b>\n"
            "<code>/add_source @link</code> — Добавить канал-донор.\n\n"
            "<b>2️⃣ DESTINATIONS (КУДА СЛАТЬ)</b>\n"
            "<code>/add_dest @link [min] [batch]</code> — Настроить чат.\n"
            "<i>Пример: /add_dest @chat 60 3 (раз в час по 3 поста)</i>\n\n"
            "<b>3️⃣ SEND MODES (РЕЖИМЫ ОТПРАВКИ)</b>\n"
            "<code>/set_mode @link [0 или 1]</code>\n"
            "• <b>Mode 0 (Forward):</b> С кнопками, виден автор (рекомендуется).\n"
            "• <b>Mode 1 (Copy):</b> Без кнопок, автор скрыт (для закрытых чатов).\n\n"
            "<b>4️⃣ UTILITIES (УТИЛИТЫ)</b>\n"
            "<code>/add_account</code> — Добавить сессию аккаунта в список юзер-ботов (через авторизацию).\n"
            "<code>/list</code> — Список всех чатов и их настроек.\n"
            "<code>/restart</code> — Перезагрузить всех ботов.\n"
            "<code>/delete @link</code> — Удалить чат из базы.\n"
            "<code>/send_ad [link]</code> — Разовая рассылка поста вручную.\n\n"
            "<b>ℹ️ INFO:</b>\n"
            "После добавления нового <b>источника</b> перезапустите бота на сервере. "
            "Настройки <b>получателей</b> и <b>режимов</b> применяются мгновенно."
        )
        await message.reply(text, parse_mode=enums.ParseMode.HTML)

    @admin_client.on_message(filters.command("restart") & filters.user(ADMIN_ID))
    async def restart_cmd(client, message):
        await message.reply("🔄 Перезагрузка главного обработчика и всех юзер-ботов...")
        os.execl(sys.executable, sys.executable, *sys.argv)

    @admin_client.on_message(filters.command("add_account") & filters.user(ADMIN_ID))
    async def add_account_start(client, message):
        user_id = message.from_user.id
        auth_states[user_id] = "WAITING_PHONE"
        await message.reply("📱 Введите номер телефона (с кодом страны, например +7999...):")

    @admin_client.on_message(filters.command("add_source") & filters.user(ADMIN_ID))
    async def add_source_cmd(client, message):
        try:
            link = message.command[1]
            clean = link.replace("https://t.me/", "").replace("@", "").strip()
            await execute_query("INSERT IGNORE INTO sources (channel_link) VALUES (%s)", (clean,))
            await message.reply(f"✅ Source **{clean}** added (добавлен канал-вещатель)! Рекомендуется перезагрузить юзер-ботов для применения изменений (воспользуйтесь командой `/restart`)")
        except:
            await message.reply("❌ Error. Usage: `/add_source @link`")

    @admin_client.on_message(filters.command("add_dest") & filters.user(ADMIN_ID))
    async def add_dest_cmd(client, message):
        try:
            link = message.command[1]
            clean = link.replace("https://t.me/", "").replace("@", "").strip()
            interval = int(message.command[2]) if len(message.command) > 2 else 0
            batch = int(message.command[3]) if len(message.command) > 3 else 1
            await execute_query(
                "INSERT INTO destinations (chat_link, interval_minutes, batch_size) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE interval_minutes = %s, batch_size = %s",
                (clean, interval, batch, interval, batch))
            await message.reply(f"✅ Destination **{clean}** configured! Interval: {interval}m, Batch: {batch}")
        except:
            await message.reply("❌ Error. Usage: `/add_dest @link 60 3`")

    @admin_client.on_message(filters.command("list") & filters.user(ADMIN_ID))
    async def list_cmd(client, message):
        srcs = await get_sources()
        dests = await get_destinations_full()
        text = "**📡 Sources:**\n" + "\n".join([f"• `{s}`" for s in srcs])
        text += "\n\n**📨 Destinations (Interval / Batch / Mode):**\n"
        for link, interval, _, batch, mode in dests:
            text += f"• `{link}` (`{interval}` min / `{batch}` posts / `{mode}`)\n"
        await message.reply(text)

    @admin_client.on_message(filters.command("delete") & filters.user(ADMIN_ID))
    async def delete_cmd(client, message):
        try:
            link = message.command[1]
            clean = link.replace("https://t.me/", "").replace("@", "").strip()
            await execute_query("DELETE FROM destinations WHERE chat_link = %s", (clean,))
            await execute_query("DELETE FROM sources WHERE channel_link = %s", (clean,))
            await message.reply(f"🗑 **{clean}** deleted from lists.")
        except:
            await message.reply("❌ Error. Usage: `/delete @link`")

    @admin_client.on_message(filters.command("set_mode") & filters.user(ADMIN_ID))
    async def set_mode_cmd(client, message):
        try:
            link = message.command[1]
            mode = int(message.command[2])  # 0 или 1
            clean = link.replace("https://t.me/", "").replace("@", "").strip()

            await execute_query("UPDATE destinations SET send_mode = %s WHERE chat_link = %s", (mode, clean))
            msg = "FORWARD (with buttons)" if mode == 0 else "COPY (no buttons)"
            await message.reply(f"✅ Mode for {clean} set to: {msg}")
        except:
            await message.reply("❌ Usage: `/set_mode @chat 0` (Forward) or `1` (Copy)")

    @admin_client.on_message(filters.command("send_ad") & filters.user(ADMIN_ID))
    async def send_ad_cmd(client, message):
        try:
            link = message.command[1]
            parts = link.split("/")
            chat_username = parts[-2]
            message_id = int(parts[-1])

            await message.reply("🚀 Starting manual broadcast...")
            dests = await get_destinations_full()

            for dest_link, _, _, _, send_mode in dests:
                sent = False
                for sender in clients:
                    try:

                        if send_mode == 1:
                            await sender.copy_message(chat_id=dest_link, from_chat_id=chat_username, message_id=message_id)
                            logger.info(f"AD copied to {dest_link}")
                        else:
                            await sender.forward_messages(chat_id=dest_link, from_chat_id=chat_username,
                                                      message_id=message_id)
                            logger.info(f"AD forwarded to {dest_link}")

                        sent = True
                        await asyncio.sleep(2)
                        break
                    except Exception as e:
                        logger.error(f"AD send error: {e}")
                if not sent:
                    logger.error(f"FAILED to send AD to {dest_link}")
            await message.reply("🏁 Manual broadcast finished.")
        except:
            await message.reply("❌ Error. Usage: `/send_ad https://t.me/channel/123`")

    @admin_client.on_message(filters.text & filters.user(ADMIN_ID))
    async def fsm_handler(client, message):
        user_id = message.from_user.id
        state = auth_states.get(user_id)

        if not state: return

        text = message.text.strip()

        try:
            if state == "WAITING_PHONE":

                text = "".join([c for c in text if c.isdigit()])
                if len(text) == 11 and text.startswith("8"):
                    text = "7" + text[1:]

                await message.reply("⏳ Подключаюсь к Telegram...")

                new_client = Client(
                    f"new_{text}",
                    api_id=API_ID,
                    api_hash=API_HASH,
                    in_memory=True
                )
                await new_client.connect()

                try:
                    sent_code = await new_client.send_code(text)
                except Exception as e:
                    await message.reply(f"❌ Ошибка отправки кода: {e}")
                    await new_client.disconnect()
                    del auth_states[user_id]
                    return

                temp_clients[user_id] = new_client
                auth_data[user_id] = {"phone": text, "phone_code_hash": sent_code.phone_code_hash}
                auth_states[user_id] = "WAITING_CODE"

                await message.reply("📩 Код отправлен! Введите код из Telegram (цифры):")

            elif state == "WAITING_CODE":
                new_client = temp_clients[user_id]
                phone = auth_data[user_id]["phone"]
                phone_code_hash = auth_data[user_id]["phone_code_hash"]

                try:
                    await new_client.sign_in(phone, phone_code_hash, text)

                    session_string = await new_client.export_session_string()
                    await save_new_account(phone, session_string)
                    await new_client.disconnect()

                    del auth_states[user_id]
                    del temp_clients[user_id]
                    del auth_data[user_id]

                    await message.reply(
                        f"✅ Аккаунт {phone} добавлен! Перезагружаю ботов для применения изменений... (2FA пароль для входа не требовался)")
                    os.execl(sys.executable, sys.executable, *sys.argv)

                except SessionPasswordNeeded:
                    auth_states[user_id] = "WAITING_PASSWORD"
                    await message.reply("🔑 Требуется 2FA пароль. Введите пароль:")

                except PhoneCodeInvalid:
                    await message.reply("❌ Неверный код. Попробуйте еще раз или начните заново /add_account")

            elif state == "WAITING_PASSWORD":
                new_client = temp_clients[user_id]
                phone = auth_data[user_id]["phone"]

                try:
                    await new_client.check_password(text)

                    session_string = await new_client.export_session_string()
                    await save_new_account(phone, session_string)
                    await new_client.disconnect()

                    del auth_states[user_id]
                    del temp_clients[user_id]
                    del auth_data[user_id]

                    await message.reply(
                        f"✅ Верный 2FA пароль. Аккаунт {phone} добавлен! Перезагружаю бота для применения изменений...")
                    os.execl(sys.executable, sys.executable, *sys.argv)

                except PasswordHashInvalid:
                    await message.reply("❌ Неверный пароль. Попробуйте еще раз.")

        except Exception as e:
            logger.error(f"Auth FSM Error: {e}")
            await message.reply(f"❌ Произошла ошибка: {e}\nАвторизация отменена.")

            if user_id in temp_clients:
                await temp_clients[user_id].disconnect()
                del temp_clients[user_id]
            if user_id in auth_states:
                del auth_states[user_id]

    logger.info("Starting clients...")
    for app in clients:
        try:
            await app.start()
        except Exception as e:
            logger.error(f"Failed to start {app.phone_number}: {e}")

    logger.info("Broadcaster is running...")

    while True:
        try:
            sources = await get_sources()
            if not sources:
                await asyncio.sleep(60)
                continue

            destinations = await get_destinations_full()

            content_pool = []
            async for post in clients[0].get_chat_history(sources[0], limit=HISTORY_LIMIT):
                if post.reply_markup:
                    content_pool.append(post)

            if not content_pool:
                await asyncio.sleep(60)
                continue

            for chat_link, interval, last_sent, batch_size, send_mode in destinations:
                time_to_wait = interval if interval > 0 else 1

                if last_sent:
                    delta = datetime.now(timezone.utc) - last_sent.replace(tzinfo=timezone.utc)
                    minutes_passed = delta.total_seconds() / 60

                    if minutes_passed < time_to_wait:
                        logger.info(f"Skip {chat_link}: passed {int(minutes_passed)}/{time_to_wait} min.")
                        continue

                logger.info(f"Time to post in {chat_link}!")

                posts_to_send = random.sample(content_pool, min(batch_size, len(content_pool)))

                for post in posts_to_send:
                    sent = False
                    for sender in clients:
                        if sent: break

                        try:
                            try:
                                await sender.join_chat(chat_link)
                            except:
                                pass

                            if send_mode == 1:
                                await sender.copy_message(
                                    chat_id=chat_link,
                                    from_chat_id=post.chat.id,
                                    message_id=post.id
                                )
                                logger.info(f"Copied to {chat_link}")
                            else:
                                await sender.forward_messages(
                                    chat_id=chat_link,
                                    from_chat_id=post.chat.id,
                                    message_ids=post.id
                                )
                                logger.info(f"Forwarded to {chat_link}")

                            logger.info(f"Post {post.id} sent to {chat_link} via {sender.phone_number}")
                            await add_to_history(post.id, sender.account_id, 'success')
                            sent = True
                            await asyncio.sleep(5)

                        except FloodWait as e:
                            logger.warning(
                                f"Account {sender.phone_number} got FloodWait for {e.value}s. Switching account.")
                            await update_account_status(sender.phone_number, 'flood_wait')

                        except (UserDeactivated, AuthKeyUnregistered):
                            logger.error(f"Account {sender.phone_number} is DEAD! Removing from pool.")
                            await update_account_status(sender.phone_number, 'banned')
                            if sender in clients:
                                clients.remove(sender)

                        except Exception as e:
                            logger.error(f"Unknown error sending with {sender.phone_number}: {e}. Switching account.")
                    if not sent:
                        logger.critical(f"FAILED to send post {post.id} to {chat_link}")

                if posts_to_send:
                    await update_last_sent_time(chat_link)

        except Exception as e:
            logger.error(f"FATAL ERROR in main loop: {e}")

        logger.info("Main loop finished. Sleeping for 60 seconds...")
        await asyncio.sleep(60)


if __name__ == "__main__":
    try:
        asyncio.run(run_broadcaster())
    except KeyboardInterrupt:
        print("Broadcaster stopped.")