from __future__ import annotations

import subprocess
from dagster import job, op


@op
def generate_data():
    subprocess.check_call(["python", "etl/generate_data.py"])
    
@op
def load_data():
    subprocess.check_call(["python", "etl/load_to_postgres.py"])
    
@job
def saas_pipeline():
    generate_data()
    load_data()