from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
import numpy as np
import pandas as pd

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = _PROJECT_ROOT / "data" / "synthetic_sources"


def _rand_uuid(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def generate_customers(n: int = 2000) -> pd.DataFrame:
    now = datetime.now(timezone.utc)
    created = [now - timedelta(days=int(np.random.exponential(scale=120))) for _ in range(n)]
    plans = np.random.choice(["free", "pro", "org"], size=n, p=[0.65, 0.28, 0.07])
    customer_ids = [_rand_uuid("cus") for _ in range(n)]
    emails = [f"user{i}@example.com" for i in range(n)]
    return pd.DataFrame(
        {
            "customer_id": customer_ids,
            "email": emails,
            "created_at": created,
            "plan": plans,
        }
    )


def generate_subscriptions(customers: pd.DataFrame) -> pd.DataFrame:
    subs = customers[customers["plan"].isin(["pro", "org"])].copy()
    n = len(subs)
    subscription_ids = [_rand_uuid("sub") for _ in range(n)]

    start_dates = pd.to_datetime(subs["created_at"]).dt.date
    churn_flags = np.random.binomial(1, 0.18, size=n)
    end_dates = []
    statuses = []
    mrr = []

    for i in range(n):
        if churn_flags[i] == 1:
            # churn sometime after start
            churn_after_days = int(np.random.exponential(scale=90)) + 7
            end_dt = start_dates.iloc[i] + timedelta(days=churn_after_days)
            end_dates.append(end_dt)
            statuses.append("canceled")
        else:
            end_dates.append(None)
            statuses.append("active")

        if subs["plan"].iloc[i] == "pro":
            mrr.append(float(np.random.choice([12, 15, 20], p=[0.5, 0.35, 0.15])))
        else:
            mrr.append(float(np.random.choice([45, 60, 90], p=[0.45, 0.4, 0.15])))

    return pd.DataFrame(
        {
            "subscription_id": subscription_ids,
            "customer_id": subs["customer_id"].values,
            "start_date": start_dates.values,
            "end_date": end_dates,
            "status": statuses,
            "mrr": mrr,
        }
    )


def generate_payments(customers: pd.DataFrame, subs: pd.DataFrame) -> pd.DataFrame:
    # Payments only for pro/org customers
    pay_customers = customers[customers["plan"].isin(["pro", "org"])].copy()
    pay_customers = pay_customers.merge(subs[["customer_id", "mrr", "status", "end_date"]], on="customer_id", how="left")

    rows = []
    now = datetime.now(timezone.utc)

    for _, r in pay_customers.iterrows():
        # monthly payments from start until now (or end_date if canceled)
        start = pd.to_datetime(r["created_at"])
        end = pd.to_datetime(r["end_date"]) if pd.notna(r["end_date"]) else pd.Timestamp(now)
        # normalize to UTC so we can subtract (avoid tz-naive vs tz-aware)
        if start.tz is None:
            start = start.tz_localize("UTC")
        else:
            start = start.tz_convert("UTC")
        if end.tz is None:
            end = end.tz_localize("UTC")
        else:
            end = end.tz_convert("UTC")
        # roughly monthly cycles
        months = max(1, int((end - start).days / 30))

        now_ts = pd.Timestamp(now)
        for k in range(months):
            paid_at = start + timedelta(days=30 * k + np.random.randint(0, 3))
            if paid_at > now_ts:
                break
            rows.append(
                {
                    "payment_id": _rand_uuid("pay"),
                    "customer_id": r["customer_id"],
                    "paid_at": paid_at,
                    "amount": float(r["mrr"]),
                    "currency": "USD",
                    "status": "paid",
                }
            )

    return pd.DataFrame(rows)


def generate_events(customers: pd.DataFrame, days: int = 60) -> pd.DataFrame:
    event_names = ["login", "open_file", "create_file", "invite_member", "export", "comment"]
    pages = ["dashboard", "editor", "files", "billing", "team"]
    devices = ["web", "desktop", "mobile"]

    rows = []
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=days)

    for _, c in customers.iterrows():
        base_rate = 0.4 if c["plan"] == "free" else (1.2 if c["plan"] == "pro" else 2.0)
        # number of events per customer
        n_events = int(np.random.poisson(lam=base_rate * days * 2.5))
        for _ in range(n_events):
            ts = start + timedelta(seconds=np.random.randint(0, days * 24 * 3600))
            rows.append(
                {
                    "event_id": _rand_uuid("evt"),
                    "customer_id": c["customer_id"],
                    "event_ts": ts,
                    "event_name": np.random.choice(event_names, p=[0.18, 0.22, 0.22, 0.12, 0.08, 0.18]),
                    "page": np.random.choice(pages),
                    "device": np.random.choice(devices, p=[0.7, 0.2, 0.1]),
                }
            )

    return pd.DataFrame(rows)


def main() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)

    customers = generate_customers(n=2500)
    subs = generate_subscriptions(customers)
    payments = generate_payments(customers, subs)
    events = generate_events(customers, days=90)

    customers.to_csv(OUT_DIR / "customers.csv", index=False)
    subs.to_csv(OUT_DIR / "subscriptions.csv", index=False)
    payments.to_csv(OUT_DIR / "payments.csv", index=False)
    events.to_csv(OUT_DIR / "product_events.csv", index=False)

    print("✅ Generated synthetic data:")
    print(f"- customers: {len(customers):,}")
    print(f"- subscriptions: {len(subs):,}")
    print(f"- payments: {len(payments):,}")
    print(f"- product_events: {len(events):,}")


if __name__ == "__main__":
    main()
