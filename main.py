import asyncio
from pyrogram import Client, filters, compose
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
        await cur.execute("SELECT session_string, phone FROM accounts WHERE status = 'active'")
        res = await cur.fetchall()
    conn.close()
    return res


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


async def main():

    sessions = await get_active_sessions()
    if not sessions:
        logger.error("There are no active sessions in the database! Run the add_account.py script")
        return

    sources = await get_sources()
    destinations = await get_destinations()

    logger.info(f"Accounts loaded: {len(sessions)}")
    logger.info(f"Source channels: {sources}")
    logger.info(f"Destination chats: {destinations}")


    clients = []
    for session_string, phone in sessions:

        app = Client(
            name=f"client_{phone}",
            api_id=API_ID,
            api_hash=API_HASH,
            session_string=session_string,
            in_memory=True
        )
        clients.append(app)

        @app.on_message(filters.chat(sources))
        async def handler(client, message):
            logger.info(f"New message in the {message.chat.username} source channel (Message ID: {message.id})")

            for dest in destinations:
                try:

                    await client.forward_messages(
                        chat_id=dest,
                        from_chat_id=message.chat.id,
                        message_ids=message.id
                    )
                    logger.info(f"--> Message was forwarded to {dest}")
                    await asyncio.sleep(2)
                except Exception as e:
                    logger.error(f"Something went wrong! Failed to forward a message to the {dest} channel: {e}")


    logger.info("User bot is working...")
    await compose(clients)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Бот остановлен.")