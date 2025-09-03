from dotenv import load_dotenv
import os

load_dotenv()  # загружаем переменные из .env

def db_url() -> str:
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5433")  # у тебя 5433!
    name = os.getenv("DB_NAME", "leadmacro")
    user = os.getenv("DB_USER", "postgres")
    pw   = os.getenv("DB_PASS", "postgres")
    return f"postgresql+psycopg://{user}:{pw}@{host}:{port}/{name}"
