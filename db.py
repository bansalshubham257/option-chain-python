import os
import psycopg2
from psycopg2 import pool
from contextlib import contextmanager

# Connection pool setup
connection_pool = None

def init_db():
    global connection_pool
    connection_pool = psycopg2.pool.SimpleConnectionPool(
        minconn=1,
        maxconn=20,
        dsn=os.getenv('DATABASE_URL')
    )

@contextmanager
def get_cursor():
    conn = connection_pool.getconn()
    try:
        with conn.cursor() as cur:
            yield cur
        conn.commit()
    except:
        conn.rollback()
        raise
    finally:
        connection_pool.putconn(conn)
