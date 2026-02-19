import psycopg2
import os
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

def get_connection() -> Optional[psycopg2.extensions.connection]:
    """Establishes a connection to the PostgreSQL database.

    Returns:
        Optional[psycopg2.extensions.connection]: Database connection object or None if failed.
    """
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        )
        return conn
    except Exception as e:
        print(f"DB Connection Error: {e}")
        return None