from fastapi import FastAPI
import os, socket, time
import psycopg

app = FastAPI()

DB_CONFIG = {
    'host': os.getenv('DB_HOST','localhost'),
    'port': int(os.getenv('DB_PORT','5432')),
    'user': os.getenv('DB_USER','amuser'),
    'password': os.getenv('DB_PASSWORD','ampass'),
    'dbname': os.getenv('DB_NAME','amdb')
}

def wait_for_db(max_tries=30, delay=1):
    last_err = None
    for _ in range(max_tries):
        try:
            with psycopg.connect(**DB_CONFIG) as _:
                return True
        except Exception as e:
            last_err = e
            time.sleep(delay)
    raise last_err

def ensure_schema():
    wait_for_db()
    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE IF NOT EXISTS person (id serial primary key, name text)")
            cur.execute("INSERT INTO person(name) SELECT 'Alice' WHERE NOT EXISTS (SELECT 1 FROM person)")
            conn.commit()

ensure_schema()

@app.get("/api/container")
async def container():
    return {"container_id": socket.gethostname()}

@app.get("/api/name")
async def get_name():
    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT name FROM person ORDER BY id DESC LIMIT 1")
            row = cur.fetchone()
            return {"name": row[0] if row else None}

@app.put("/api/name")
async def put_name(payload: dict):
    name = payload.get('name')
    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO person(name) VALUES (%s)", (name,))
            conn.commit()
    return {"ok": True}
