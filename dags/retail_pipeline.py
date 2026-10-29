import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


RAW_DATA_PATH = "/opt/airflow/data/raw/retail_sales.csv"

PROJECT_HOST_DIR = os.environ.get("PROJECT_HOST_DIR", "").strip()
DOCKER_NETWORK_NAME = os.environ.get(
    "DOCKER_NETWORK_NAME",
    "03_retail_data_pipeline_default",
).strip()


def check_raw_data():
    if not os.path.isfile(RAW_DATA_PATH):
        raise FileNotFoundError(
            f"Raw retail data was not found: {RAW_DATA_PATH}"
        )

    print(f"Raw retail data found: {RAW_DATA_PATH}")


def validate_processed_data():
    expected_outputs = [
        "/opt/airflow/data/processed/retail_sales/_SUCCESS",
        "/opt/airflow/data/processed/sales_by_category/_SUCCESS",
    ]

    missing = [
        path for path in expected_outputs
        if not os.path.isfile(path)
    ]

    if missing:
        raise FileNotFoundError(
            f"Expected Spark output files are missing: {missing}"
        )

    print("Both processed datasets were validated successfully.")


def get_host_mounts():
    if not PROJECT_HOST_DIR:
        raise ValueError(
            "PROJECT_HOST_DIR is not configured. "
            "Set it in the project's .env file and pass it "
            "to the Airflow services through Docker Compose."
        )

    if not DOCKER_NETWORK_NAME:
        raise ValueError(
            "DOCKER_NETWORK_NAME is not configured."
        )

    return [
        Mount(
            source=os.path.join(PROJECT_HOST_DIR, "spark"),
            target="/opt/spark-apps",
            type="bind",
        ),
        Mount(
            source=os.path.join(PROJECT_HOST_DIR, "data"),
            target="/opt/spark-data",
            type="bind",
        ),
    ]


with DAG(
    dag_id="retail_sales_pipeline",
    description="Orchestrate retail data processing with Airflow and Spark",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    tags=["retail", "spark", "etl"],
) as dag:

    check_data = PythonOperator(
        task_id="check_raw_data",
        python_callable=check_raw_data,
    )

    process_data = DockerOperator(
        task_id="process_retail_data",
        image="apache/spark:3.5.6",
        api_version="auto",
        auto_remove="never",
        command=(
            "/opt/spark/bin/spark-submit "
            "--master local[*] "
            "--jars /opt/spark-apps/postgresql-42.7.5.jar "
            "/opt/spark-apps/process_retail.py"
        ),
        docker_url="unix://var/run/docker.sock",
        network_mode=DOCKER_NETWORK_NAME,
        mount_tmp_dir=False,
        environment={
            "POSTGRES_HOST": os.environ.get("POSTGRES_HOST", "postgres"),
            "POSTGRES_PORT": os.environ.get("POSTGRES_PORT", "5432"),
            "POSTGRES_DB": os.environ.get("POSTGRES_DB", "airflow"),
            "POSTGRES_USER": os.environ.get("POSTGRES_USER", "airflow"),
            "POSTGRES_PASSWORD": os.environ.get("POSTGRES_PASSWORD", ""),
        },
        mounts=get_host_mounts(),
    )

    validate_output = PythonOperator(
        task_id="validate_processed_data",
        python_callable=validate_processed_data,
    )

    check_data >> process_data >> validate_output
