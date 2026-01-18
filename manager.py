import asyncio
import aiomysql
import os

from env_loader import DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME

DB_CONFIG = {
    'host': DB_HOST,
    'port': DB_PORT,
    'user': DB_USER,
    'password': DB_PASS,
    'db': DB_NAME
}

async def execute_query(query, params=None):
    conn = await aiomysql.connect(**DB_CONFIG)
    async with conn.cursor() as cur:
        await cur.execute(query, params)
        if query.strip().upper().startswith("SELECT"):
            result = await cur.fetchall()
        else:
            await conn.commit()
            result = None
    conn.close()
    return result


def clean_link(link):
    """Убирает https://t.me/ и @, оставляя только username"""
    return link.replace("https://t.me/", "").replace("@", "").strip()


async def show_lists():
    sources = await execute_query("SELECT channel_link FROM sources")
    dests = await execute_query("SELECT chat_link FROM destinations")

    print("\n--- 📡 ИСТОЧНИКИ (ОТКУДА) ---")
    for s in sources: print(f"- {s[0]}")

    print("\n--- 📨 ПОЛУЧАТЕЛИ (КУДА) ---")
    for d in dests: print(f"- {d[0]}")
    print("-" * 30)


async def add_source():
    print("Если вы добавили новый ИСТОЧНИК (откуда брать сообщения), перезапустите бота, чтобы он учёл изменения.")
    link = input("Введите ссылку или юзернейм источника: ")
    clean = clean_link(link)
    await execute_query("INSERT IGNORE INTO sources (channel_link) VALUES (%s)", (clean,))
    print(f"✅ Источник {clean} добавлен!")


async def add_dest():
    print("Если добавили ПОЛУЧАТЕЛЯ (куда пересылать сообщения) — перезапускать бота не нужно, он подхватит изменения сам.")
    link = input("Введите ссылку или юзернейм получателя: ")
    clean = clean_link(link)
    await execute_query("INSERT IGNORE INTO destinations (chat_link) VALUES (%s)", (clean,))
    print(f"✅ Получатель {clean} добавлен!")


async def delete_item():
    what = input("Что удалить? (1 - Источник, 2 - Получатель): ")
    name = input("Введите юзернейм для удаления: ")
    clean = clean_link(name)

    if what == "1":
        await execute_query("DELETE FROM sources WHERE channel_link = %s", (clean,))
    elif what == "2":
        await execute_query("DELETE FROM destinations WHERE chat_link = %s", (clean,))
    print(f"🗑 {clean} удален (если был в списке).")


async def main():
    print("🔧 ПАНЕЛЬ УПРАВЛЕНИЯ БОТОМ")
    while True:
        print("\n--- опции ---")
        print("1. Показать списки")
        print("2. Добавить ИСТОЧНИК")
        print("3. Добавить ПОЛУЧАТЕЛЯ")
        print("4. Удалить что-то")
        print("0. Выход")

        choice = input("\nВаш выбор: ")

        if choice == "1":
            await show_lists()
        elif choice == "2":
            await add_source()
        elif choice == "3":
            await add_dest()
        elif choice == "4":
            await delete_item()
        elif choice == "0":
            break
        else:
            print("Неверный ввод")


if __name__ == "__main__":
    asyncio.run(main())