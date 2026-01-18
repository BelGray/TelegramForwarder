import os
from dotenv import load_dotenv

load_dotenv()

API_ID = int(os.getenv("API_ID", "placeholder_api_id"))
API_HASH = os.getenv("API_HASH", "placeholder_hash")

DB_HOST=os.getenv("DB_HOST", "localhost")
DB_USER=os.getenv("DB_USER", "root")
DB_PORT=int(os.getenv("DB_PORT", "3306"))
DB_PASS=os.getenv("DB_PASS", "")
DB_NAME=os.getenv("DB_NAME", "telegram_forwarder")