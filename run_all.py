#!/usr/bin/env python3
"""
Run the full pipeline: generate data → start Postgres (if needed) → load into warehouse.
From project root:  python run_all.py
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

# Run from project root
PROJECT_ROOT = Path(__file__).resolve().parent
os.chdir(PROJECT_ROOT)
sys.path.insert(0, str(PROJECT_ROOT))

# Load env before anything that needs it
from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")


def run_generate() -> None:
    print("Step 1/3: Generating synthetic data...")
    from etl.generate_data import main as generate_main
    generate_main()
    print()


def start_postgres() -> bool:
    """Start Postgres with docker compose. Return True if running (started or already up)."""
    for cmd in (["docker", "compose"], ["docker-compose"]):
        try:
            r = subprocess.run(
                [*cmd, "up", "-d"],
                cwd=PROJECT_ROOT,
                env=os.environ.copy(),
                capture_output=True,
                text=True,
                timeout=120,
            )
            if r.returncode == 0:
                return True
            if "port is already allocated" in (r.stderr or "") or "port is already allocated" in (r.stdout or ""):
                # Container or something else already on the port - assume Postgres is up
                return True
        except FileNotFoundError:
            continue
        except subprocess.TimeoutExpired:
            print("Docker compose timed out.", file=sys.stderr)
            return False
    print("Docker not found. Install Docker and run: docker compose up -d", file=sys.stderr)
    return False


def wait_for_postgres(max_waits: int = 60) -> bool:
    """Return True when Postgres accepts connections."""
    from sqlalchemy import create_engine, text
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5434")
    user = os.getenv("PGUSER", "saas_user")
    pwd = os.getenv("PGPASSWORD", "saas_pass")
    db = os.getenv("PGDATABASE", "saas_analytics")
    url = f"postgresql://{user}:{pwd}@{host}:{port}/{db}"
    for _ in range(max_waits):
        try:
            engine = create_engine(url, pool_pre_ping=True)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception:
            time.sleep(1)
    return False


def run_load() -> None:
    print("Step 3/3: Loading data into Postgres...")
    from etl.load_to_postgres import main as load_main
    load_main()
    print()


def main() -> None:
    run_generate()

    print("Step 2/3: Starting Postgres...", flush=True)
    if not start_postgres():
        print("Failed to start Postgres. Run: docker compose up -d", file=sys.stderr)
        sys.exit(1)
    print("Waiting for Postgres to accept connections...", flush=True)
    if not wait_for_postgres():
        print("Postgres did not become ready in 60s. Is Docker running? Try: docker compose up -d", file=sys.stderr)
        sys.exit(1)
    print("Postgres is ready.\n", flush=True)

    run_load()
    print("Pipeline finished.", flush=True)


if __name__ == "__main__":
    main()
