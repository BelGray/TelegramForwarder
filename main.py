import asyncio
from collections import deque
from pyrogram import Client, filters, compose
import aiomysql
from pyrogram.errors import FloodWait, UserDeactivated, AuthKeyUnregistered

from env_loader import DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME, API_ID, API_HASH
from log_config import logger

DB_CONFIG = {
    'host': DB_HOST,
    'port': DB_PORT,
    'user': DB_USER,
    'password': DB_PASS,
    'db': DB_NAME
}

processed_messages = deque(maxlen=1000)
processing_lock = asyncio.Lock()


async def get_active_sessions() -> list:
    """Get all active user sessions (id, session, phone) from the database"""
    res = list()
    conn = await aiomysql.connect(**DB_CONFIG)
    async with conn.cursor() as cur:
        await cur.execute("SELECT id, session_string, phone FROM accounts WHERE status = 'active'")
        res = await cur.fetchall()
    conn.close()
    return res


async def revive_accounts():
    """Revive flood-waited accounts"""
    conn = await aiomysql.connect(**DB_CONFIG)
    async with conn.cursor() as cur:
            await cur.execute("""
                UPDATE accounts 
                SET status = 'active' 
                WHERE status = 'flood_wait' 
                AND last_used < (NOW() - INTERVAL 30 MINUTE)
            """)
            await conn.commit()
    conn.close()


async def get_sources() -> list:
    """Get all source channels from the database"""
    res = list()
    conn = await aiomysql.connect(**DB_CONFIG)
    async with conn.cursor() as cur:
        await cur.execute("SELECT channel_link FROM sources")
        res = [row[0] for row in await cur.fetchall()]
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


async def add_to_history(source_msg_id, account_id, status):
    """Logs the forwarding event to the database history"""
    try:
        conn = await aiomysql.connect(**DB_CONFIG)
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO history (source_message_id, account_id, status) VALUES (%s, %s, %s)",
                (source_msg_id, account_id, status)
            )
            await conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"History logging error: {e}")


async def is_already_processed(source_msg_id):
    """Check if message ID in history"""
    conn = await aiomysql.connect(**DB_CONFIG)
    async with conn.cursor() as cur:
        await cur.execute(
            "SELECT 1 FROM history WHERE source_message_id = %s LIMIT 1",
            (source_msg_id,)
        )
        result = await cur.fetchone()
    conn.close()
    return result is not None


async def main():
    await revive_accounts()

    sessions = await get_active_sessions()
    if not sessions:
        logger.error("No active sessions found in DB! Please run add_account.py")
        return

    sources = await get_sources()
    destinations = await get_destinations()

    logger.info(f"Accounts loaded: {len(sessions)} | Sources: {len(sources)} | Destinations: {len(destinations)}")

    clients = []
    for account_id, session_string, phone in sessions:
        app = Client(
            name=f"client_{phone}",
            api_id=API_ID,
            api_hash=API_HASH,
            session_string=session_string,
            in_memory=True
        )

        app.phone_number = phone
        app.account_id = account_id
        clients.append(app)

    for app in clients:
        @app.on_message(filters.chat(sources))
        async def handler(client, message):

            if await is_already_processed(message.id):
                return

            unique_id = (message.chat.id, message.id)

            async with processing_lock:
                if unique_id in processed_messages:
                    return
                processed_messages.append(unique_id)

            logger.info(f"New post {message.id} detected by {client.phone_number}")

            current_destinations = await get_destinations()

            for dest in current_destinations:
                sent_successfully = False

                available_clients = clients.copy()

                for sender in available_clients:
                    if sent_successfully:
                        break

                    try:
                        logger.info(f"Trying to send via {sender.phone_number}...")

                        try:
                            await sender.join_chat(dest)
                        except Exception:
                            logger.debug(f"Join chat warning: {e}")

                        await sender.forward_messages(
                            chat_id=dest,
                            from_chat_id=message.chat.id,
                            message_ids=message.id
                        )

                        logger.info(f"Successfully forwarded to {dest}")

                        await add_to_history(message.id, sender.account_id, 'success')

                        sent_successfully = True
                        await asyncio.sleep(2)

                    except FloodWait as e:
                        logger.warning(f"Account {sender.phone_number} got FloodWait for {e.value}s")
                        await update_account_status(sender.phone_number, 'flood_wait')

                    except (UserDeactivated, AuthKeyUnregistered):
                        logger.error(f"Account {sender.phone_number} is DEAD (Banned/Revoked)!")
                        await update_account_status(sender.phone_number, 'banned')
                        if sender in clients:
                            clients.remove(sender)

                    except Exception as e:
                        logger.error(f"Unknown error with {sender.phone_number}: {e}")

                if not sent_successfully:
                    logger.critical(f"Failed to forward message to {dest} with all accounts!")


    logger.info("Userbot farm is running...")
    await compose(clients)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Bot stopped.")