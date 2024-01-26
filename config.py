import os

from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
ELASTIC_SEARCH_DB_URI = os.getenv("ELASTIC_SEARCH_DB_URI")


SOURCE_DB_NAME = "lax_prod"
# SOURCE_DB_NAME = "lax_backend"
SOURCE_TABLE_NAME = "public.user"

TARGET_DB_NAME = "fodb"
# TARGET_DB_NAME = "lax_flow"
TARGET_TABLE_NAME = "public.user"

ELASTIC_SEARCH_INDEX = "user_test"

ATTIBUTES = [
    "id",
    "firstname",
    "lastname",
    "email",
    "created_at",
    "updated_at"
]
PRIMARY_KEY = "id"

