import asyncio
from pyrogram import Client
from pyrogram.errors import FloodWait
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


async def get_active_sessions():
    conn = await aiomysql.connect(**DB_CONFIG)
    async with conn.cursor() as cur:
        await cur.execute("SELECT id, session_string, phone FROM accounts WHERE status = 'active'")
        res = await cur.fetchall()
    conn.close()
    return res


async def get_destinations_with_last_id():
    conn = await aiomysql.connect(**DB_CONFIG)
    async with conn.cursor() as cur:
        await cur.execute("SELECT chat_link, last_ad_msg_id FROM destinations")
        res = await cur.fetchall()
    conn.close()
    return res


async def update_last_msg_id(chat_link, msg_id):
    conn = await aiomysql.connect(**DB_CONFIG)
    async with conn.cursor() as cur:
        await cur.execute(
            "UPDATE destinations SET last_ad_msg_id = %s WHERE chat_link = %s",
            (msg_id, chat_link)
        )
        await conn.commit()
    conn.close()


async def scheduler_loop():
    print("\n⏱ АВТО-ПЛАНИРОВЩИК РЕКЛАМЫ")
    print("Скрипт будет удалять старый пост и публиковать новый с заданным интервалом.\n")

    ad_link = input("Вставьте ссылку на пост (https://t.me/channel/123): ").strip()

    try:
        interval_input = input("Введите интервал в минутах (по умолчанию 60): ").strip()
        if not interval_input:
            interval_minutes = 60
        else:
            interval_minutes = int(interval_input)
    except ValueError:
        print("❌ Ошибка: нужно ввести число. Использую 60 минут.")
        interval_minutes = 60

    try:
        if "/" not in ad_link: raise ValueError
        parts = ad_link.split("/")
        source_chat = parts[-2]
        source_msg_id = int(parts[-1])
    except:
        print("❌ Ошибка: Некорректная ссылка. Перезапустите скрипт.")
        return

    print(f"\n✅ Задача принята!")
    print(f"📢 Пост: {ad_link}")
    print(f"⏰ Интервал: {interval_minutes} мин.")
    print("🚀 Запуск цикла... (Нажмите Ctrl+C для остановки)\n")

    while True:
        logger.info("Scheduled distribution is starting")

        sessions = await get_active_sessions()
        destinations = await get_destinations_with_last_id()

        if not sessions:
            logger.error("There are no active accounts!")
            await asyncio.sleep(60)
            continue

        clients = []
        for _, session, phone in sessions:
            app = Client(f"sched_{phone}", api_id=API_ID, api_hash=API_HASH, session_string=session, in_memory=True)
            app.phone_number = phone
            clients.append(app)

        for app in clients:
            try:
                await app.start()
            except:
                pass

        for chat_link, last_msg_id in destinations:
            sent_success = False

            if last_msg_id:
                for deleter in clients:
                    try:
                        await deleter.delete_messages(chat_link, last_msg_id)
                        logger.info(f"Old message was deleted (msg id {last_msg_id}) in {chat_link}")
                        break
                    except Exception:
                        continue

            for sender in clients:
                if sent_success: break

                try:
                    try:
                        await sender.join_chat(chat_link)
                    except:
                        pass

                    sent_msg = await sender.forward_messages(
                        chat_id=chat_link,
                        from_chat_id=source_chat,
                        message_ids=source_msg_id
                    )

                    await update_last_msg_id(chat_link, sent_msg.id)

                    logger.info(f"Ad was sent to the {chat_link} via {sender.phone_number}")
                    sent_success = True
                    await asyncio.sleep(5)

                except FloodWait as e:
                    logger.warning(f"FloodWait {e.value}s")
                    await asyncio.sleep(e.value)
                except Exception as e:
                    logger.error(f"Error: {e}")

        for app in clients:
            try:
                await app.stop()
            except:
                pass

        print(f"💤 Круг завершен. Ожидание {interval_minutes} минут...")
        await asyncio.sleep(interval_minutes * 60)


if __name__ == "__main__":
    try:
        asyncio.run(scheduler_loop())
    except KeyboardInterrupt:
        print("\nПланировщик остановлен.")