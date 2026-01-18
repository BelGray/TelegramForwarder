import asyncio
from pyrogram import Client
from pyrogram.errors import FloodWait, UserDeactivated, AuthKeyUnregistered
import aiomysql

from env_loader import DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME, API_ID, API_HASH
from log_config import logger

DB_CONFIG = {
    'host': DB_HOST,
    'port': DB_PORT,
    'user': DB_USER,
    'password': DB_PASS,
    'db': DB_NAME
}

async def get_active_sessions() -> list:
    """Get all active user sessions from the database"""
    res = list()
    conn = await aiomysql.connect(**DB_CONFIG)
    async with conn.cursor() as cur:
        await cur.execute("SELECT id, session_string, phone FROM accounts WHERE status = 'active'")
        res = await cur.fetchall()
    conn.close()
    return res


async def get_destinations() -> list:
    """Get all destination chats from the database"""
    res = list()
    conn = await aiomysql.connect(**DB_CONFIG)
    async with conn.cursor() as cur:
        await cur.execute("SELECT chat_link FROM destinations")
        res = [row[0] for row in await cur.fetchall()]
    conn.close()
    return res


async def update_account_status(phone, status):
    """Changes account status (active -> flood_wait / banned)"""
    try:
        conn = await aiomysql.connect(**DB_CONFIG)
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE accounts SET status = %s WHERE phone = %s",
                (status, phone)
            )
            await conn.commit()
        conn.close()
        logger.warning(f"Account {phone} status changed to: {status}")
    except Exception as e:
        logger.error(f"Status update error (phone: {phone}): {e}")



async def start_sender():
    print("\n📢 РУЧНАЯ РАССЫЛКА СООБЩЕНИЯ")
    print("Вставьте ссылку на пост, который нужно переслать.")
    print("Пример: https://t.me/channel_name/123")

    link = input("Ссылка: ").strip()

    if "/" not in link:
        print("❌ Ошибка: Некорректная ссылка.")
        return

    try:
        parts = link.split("/")
        chat_username = parts[-2]
        message_id = int(parts[-1])
    except Exception:
        print("❌ Не удалось распознать ссылку. Нужен формат https://t.me/chat/id")
        return

    print("\n⏳ Загрузка аккаунтов и чатов...")

    sessions = await get_active_sessions()
    if not sessions:
        print("❌ Нет активных аккаунтов в базе!")
        return

    destinations = await get_destinations()
    if not destinations:
        print("❌ Нет целевых чатов в базе!")
        return

    clients = []
    for account_id, session_string, phone in sessions:
        app = Client(
            name=f"sender_{phone}",
            api_id=API_ID,
            api_hash=API_HASH,
            session_string=session_string,
            in_memory=True
        )
        app.phone_number = phone
        clients.append(app)

    print(f"✅ Аккаунтов готово: {len(clients)}")
    print(f"🎯 Чатов для рассылки: {len(destinations)}")
    print("🚀 Запуск клиентов Telegram...")

    # Start all clients
    for app in clients:
        try:
            await app.start()
        except Exception as e:
            logger.error(f"Failed to start client {app.phone_number}: {e}")

    print("\n🚀 Начинаем рассылку...\n")

    # 3. Sending Loop
    for dest in destinations:
        sent_successfully = False
        available_clients = clients.copy()

        for sender in available_clients:
            if sent_successfully:
                break

            try:
                await sender.forward_messages(
                    chat_id=dest,
                    from_chat_id=chat_username,
                    message_ids=message_id
                )

                logger.info(f"Successfully forwarded to {dest} via {sender.phone_number}")
                print(f"✅ Отправлено в {dest}")

                sent_successfully = True
                await asyncio.sleep(2)

            except FloodWait as e:
                logger.warning(f"Account {sender.phone_number} got FloodWait for {e.value}s")
                await update_account_status(sender.phone_number, 'flood_wait')

            except (UserDeactivated, AuthKeyUnregistered):
                logger.error(f"Account {sender.phone_number} is DEAD!")
                await update_account_status(sender.phone_number, 'banned')
                if sender in clients:
                    clients.remove(sender)

            except Exception as e:
                logger.error(f"Error with {sender.phone_number}: {e}")

        if not sent_successfully:
            logger.critical(f"Failed to send to {dest}")
            print(f"❌ Не удалось отправить в {dest}")

    print("\n🏁 Рассылка завершена! Отключаем клиентов...")

    # Stop all clients
    for app in clients:
        try:
            await app.stop()
        except:
            pass

    print("✅ Готово")


if __name__ == "__main__":
    try:
        asyncio.run(start_sender())
    except KeyboardInterrupt:
        print("\nОстановлено пользователем.")