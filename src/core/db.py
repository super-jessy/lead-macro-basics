from sqlalchemy import create_engine, text
from .config import db_url

def get_engine():
    """Создаём SQLAlchemy Engine на основе URL из config.py"""
    return create_engine(db_url(), pool_pre_ping=True)

def ping():
    """Пробуем подключиться и получить версию PostgreSQL"""
    eng = get_engine()
    with eng.connect() as conn:
        ver = conn.execute(text("select version()")).scalar()
        return ver
