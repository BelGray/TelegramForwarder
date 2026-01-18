import asyncio

from pyrogram import Client
import aiomysql

from env_loader import DB_PORT, DB_HOST, DB_USER, DB_PASS, DB_NAME, API_ID, API_HASH
from log_config import logger

DB_CONFIG = {
    'host': DB_HOST,
    'port': DB_PORT,
    'user': DB_USER,
    'password': DB_PASS,
    'db': DB_NAME
}


async def add_account():
    logger.info("Launching accounts adder...")

    while True:

        print("--- Add new account --- \n")

        async with Client("temp_session", api_id=API_ID, api_hash=API_HASH, in_memory=True) as app:

            logger.info(f"Logged in successfully! Account: {app.me.first_name} (@{app.me.username})")

            session_string = await app.export_session_string()
            phone = app.me.phone_number

            conn = await aiomysql.connect(**DB_CONFIG)
            async with conn.cursor() as cur:
                await cur.execute("SELECT id FROM accounts WHERE phone = %s", (phone,))
                exists = await cur.fetchone()

                if exists:
                    logger.warn(f"{phone} account already exists in the database!")
                else:
                    await cur.execute(
                        "INSERT INTO accounts (phone, session_string, status) VALUES (%s, %s, 'active')",
                        (phone, session_string)
                    )
                    await conn.commit()
                    logger.info(f"{phone} account info was inserted into the database successfully (New MySQL row)!")

            conn.close()


if __name__ == "__main__":
    asyncio.run(add_account())
