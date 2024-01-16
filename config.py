import os

from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")


SOURCE_DB_NAME = "lax_backend"
SOURCE_TABLE_NAME = "public.user"

TARGET_DB_NAME = "lax_test_sync"
TARGET_TABLE_NAME = "public.user"

ATTIBUTES = [
    "id",
    "firstname",
    "lastname",
    "email",
]
PRIMARY_KEY = "id"

