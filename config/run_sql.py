"""
run_sql.py
Runs a SQL file against the PostgreSQL server using the settings in .env.
Usage: python config\\run_sql.py sql\\01_setup.sql
"""
import os
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

if len(sys.argv) != 2:
    sys.exit("Usage: python config\\run_sql.py <path-to-sql-file>")

sql_file = Path(sys.argv[1])
conn = psycopg2.connect(
    host=os.environ["PGHOST"],
    port=os.getenv("PGPORT", "5432"),
    dbname=os.environ["PGDATABASE"],
    user=os.environ["PGUSER"],
    password=os.environ["PGPASSWORD"],
    sslmode=os.getenv("PGSSLMODE", "require"),
)
conn.autocommit = True  # each statement is committed as it runs

with conn.cursor() as cur:
    cur.execute(sql_file.read_text(encoding="utf-8"))
    if cur.description:  # the file returned rows, so print them
        for row in cur.fetchall():
            print(row)

conn.close()
print(f"Finished: {sql_file}")