import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

load_dotenv()

host = os.getenv("SUPABASE_DB_HOST")
port = os.getenv("SUPABASE_DB_PORT", "5432")
dbname = os.getenv("SUPABASE_DB_NAME", "postgres")
user = os.getenv("SUPABASE_DB_USER")
password = os.getenv("SUPABASE_DB_PASSWORD")

print("HOST =", host)
print("PORT =", port)
print("DBNAME =", dbname)
print("USER =", user)
print("PASSWORD exists =", password is not None)

db_url = URL.create(
    drivername="postgresql+psycopg2",
    username=user,
    password=password,
    host=host,
    port=int(port),
    database=dbname,
)

engine = create_engine(
    db_url,
    connect_args={"sslmode": "require"}
)

print("Connecting...")

with engine.connect() as conn:
    result = conn.execute(text("select current_database(), current_user, now();"))
    for row in result:
        print("Connected successfully:")
        print(row)