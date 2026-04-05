import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("WH_PG_HOST"),
    port=int(os.getenv("WH_PG_PORT", "5432")),
    user=os.getenv("WH_PG_USER"),
    password=os.getenv("WH_PG_PASSWORD"),
    dbname=os.getenv("WH_PG_NAME"),
    sslmode="require",
)
print("connected")
conn.close()