"""
Load synthetic CSVs into Postgres raw schema.
Start Postgres first:  docker compose up -d
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

DATA_DIR = PROJECT_ROOT / "data" / "synthetic_sources"
SCHEMA_SQL = PROJECT_ROOT / "warehouse" / "schema.sql"


def pg_url() -> str:
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5434")
    user = os.getenv("PGUSER", "saas_user")
    pwd = os.getenv("PGPASSWORD", "saas_pass")
    db = os.getenv("PGDATABASE", "saas_analytics")
    return f"postgresql://{user}:{pwd}@{host}:{port}/{db}"


def main() -> None:
    try:
        engine = create_engine(pg_url(), pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as e:
        err = str(e).lower()
        if "password authentication failed" in err:
            print("Password doesn't match the DB. Recreate it:", file=sys.stderr)
            print("  docker compose down -v && docker compose up -d", file=sys.stderr)
        elif "role" in err and "does not exist" in err:
            print("Wrong Postgres (no saas_user). Start the project DB:", file=sys.stderr)
            print("  docker compose up -d", file=sys.stderr)
        elif "connection" in err or "refused" in err or "not permitted" in err or "could not connect" in err:
            print("Postgres not running. Start it:", file=sys.stderr)
            print("  docker compose up -d", file=sys.stderr)
        else:
            print(e, file=sys.stderr)
        sys.exit(1)

    # Apply schema, truncate, load
    sql = SCHEMA_SQL.read_text()
    with engine.begin() as conn:
        conn.execute(text(sql))
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE raw.product_events, raw.payments, raw.subscriptions, raw.customers CASCADE;"))

    for filename, table in [
        ("customers.csv", "customers"),
        ("subscriptions.csv", "subscriptions"),
        ("payments.csv", "payments"),
        ("product_events.csv", "product_events"),
    ]:
        path = DATA_DIR / filename
        if not path.exists():
            print(f"Skip {filename} (not found)", file=sys.stderr)
            continue
        df = pd.read_csv(path)
        for col in ["created_at", "paid_at", "event_ts"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
        df.to_sql(
            table, engine, schema="raw", if_exists="append", index=False, method="multi", chunksize=5000
        )
        print(f"✅ Loaded {len(df):,} rows into raw.{table}")

    print("Done.")


if __name__ == "__main__":
    main()
