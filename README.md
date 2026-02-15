# Self-Serve Analytics Data Platform for a Saas Company

Production-style analytics infrastructure simulating how a modern SaaS company build scalable, trusted data pipeline for product, finance, sales, and marketing teams.

This project demonstrates end-to-end data engineering using Python, SQL, dbt, and Dagster to create reliable analytics datasets that enable self-serve reporting and business decision-making.

---

## 🚀 Overview

Fast-growing SaaS companies generate data across multiple systems:

- Product usage events
- Payments & subscriptions
- CRM customer data
- Marketing campaigns
- Financial exports

Business teams need clean, consistent, trsuted datasets - not raw logs.

This project builds a modern analytics stack that ingests, transforms, models and serves data for internal stakeholders.

It mirrors how companies like Figma sclae their analytics infrastructure.

---

## 🧠 Key Features

- Multi-source ETL pipelines using Python
- Cloud warehouse analytics modeling
- dbt transformation layer with tests & documentation
- Dagster pipeline orchestration
- Star-schema warehouse design
- Self-serve curated analytics tables
- Reverse ETL simulation
- Production-style folder structure
- Reproducible Docker environment

## 🏗 Architecture

```
Sources → Python ETL → Warehouse → dbt Models → Analytics Layer → Dashboards
              ↓
           Dagster orchestration
```

### Data Sources

- Product event stream (simulated JSON logs)
- Stripe-style payments API
- CRM customer dataset
- Marekting attribution data

---

## 🧰 Tech Stack

| Layer | Tools |
|------|------|
| ETL | Python |
| Warehouse | Snowflake / BigQuery |
| Transformations | dbt |
| Orchestration | Dagster |
| Analytics | SQL |
| Reverse ETL | Python simulation |
| Environment | Docker |
| Visualization | Optional dashboard tool |

---

## 📦 Project Structure

<img width="215" height="595" alt="image" src="https://github.com/user-attachments/assets/e0b7fe70-94a3-429c-9405-e00d160384f7" />

## 📊 Data Modeling

Warehouse layers follow analytics engineering best practices:

### Raw layer
Unmodified ingested source data

### Staging layer
Cleaned and standardized tables

### Analytics Marts
Business-ready datasets:

- daily_active_users
- revenue_metrics
- churn_cohorts
- subscription_lifecycle
- markting_performance
- customer_lifetime_value

These datasets power self-serve analytics for internal teams.

---

## ⚙️ Pipeline Workflow

1. Python ETL pulls data from simulated APIs & files
2. Raw data loads into warehouse
3. dbt transforms raw -> stageing -> marts
4. Dagster orchestrates scheduled runs
5. Metrics exported via reverse ETL simulation

---

## 🧪 Data Quality

- dbt tests for nulls, uniqueness, referential integrity
- Automated validation checks
- Reproducible pipeline runs
- Documented lineage

---

## 📈 Business Use Cases

This platform enables:

- Revenue forecasting
- Churn analysis
- Customer cohort tracking
- Product engagement insights
- Marketing attribution reporting
- Subscriptiob analytics

--- 

## ▶️ Running the Project

### 1. Clone repo

```
git clone https://github.com/yourname/saas-analytics-platform
cd saas-analytics-platform
```

### 2. Start Docker environment

```
docker-compose up
```

### 3. Run ETL pipelines

```
python etl/ingest_events.py
python etl/ingest_payments.py
```

### 4. Run dbt transformations

```
dbt run
dbt test
```

### 5. Start Dagster orchestration

```
dagster dev
```

---

## 🎯 Skills Demonstrated

- Production-grade data pipeline design
- SQL analytics modeling
- Python ETL engineering
- dbt best practices
- Orchestration workflows
- Warehouse architecture
- Reverse ETL concepts
- Stakeholder-ready dataset design
- Documentation & communication
- End-to-end system ownership

---

## 🔮 Future Improvements

- Streaming ingestion (Kafka)
- Airflow alternative orchestration
- Real-time dashboards
- Feature store integration
- ML pipeline layer
- Data contracts & schema registry
- Monitoring & alerting

---

## ⭐ Why This Project Matters

Modern companies don't just need dashboards.

They need relaible data platforms.

This project demonstrates the ability to build scalable analytics infrastructure that supports real business decisions - exactly what modern data engineering teams require.

