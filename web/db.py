import psycopg2
import os
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

def get_connection(mode: str = "local") -> Optional[psycopg2.extensions.connection]:
    """Establishes a connection to the PostgreSQL database.
    
    Args:
        mode: "local" for local Docker DB, "prod" for EC2 DB
        
    Returns:
        Optional[psycopg2.extensions.connection]: Database connection object or None if failed.
    """
    try:
        if mode == "prod":
            host     = os.getenv("PROD_DB_HOST")
            port     = os.getenv("PROD_DB_PORT")
            database = os.getenv("PROD_DB_NAME")
            user     = os.getenv("PROD_DB_USER")
            password = os.getenv("PROD_DB_PASSWORD")
        else:  # local
            host     = os.getenv("LOCAL_DB_HOST")
            port     = os.getenv("LOCAL_DB_PORT")
            database = os.getenv("LOCAL_DB_NAME")
            user     = os.getenv("LOCAL_DB_USER")
            password = os.getenv("LOCAL_DB_PASSWORD")

        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )

        try:
            from pgvector.psycopg2 import register_vector
            register_vector(conn)
        except Exception:
            pass

        return conn

    except Exception as e:
        print(f"DB Connection Error ({mode}): {e}")
        return None