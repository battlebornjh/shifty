import os

db_host = os.getenv("POSTGRES_HOST")

if db_host is None:
    db_host = "localhost2"

print(db_host)
