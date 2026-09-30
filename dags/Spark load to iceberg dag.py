
from __future__ import annotations
import os
from datetime import datetime, timedelta
import boto3  
import requests
from botocore.exceptions import ClientError
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Parametros del pipeline ----------------------
MINIO_ENDPOINT = os.getenv("STORAGE_URI", "http://minio:9000")
NESSIE_API = os.getenv("NESSIE_API", "http://nessie:19120/api/v1")
BUCKETS = ["bronze", "gold"]
SOURCE_BUCKET = "bronze"
SOURCE_KEY = "yellow_tripdata_2025-01.parquet"
TABLE_NAMESPACE = "gold"
TABLE_NAME = "yellowtrip"

def _s3():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "admin"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "password"),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
    )

# verificamos que el parquet exista y se aseguran buckets
def prepare_minio():
    """Crea los buckets si faltan y falla con un mensaje claro si no está el parquet de origen."""
    s3 = _s3()
    for bucket in BUCKETS:
        try:
            s3.head_bucket(Bucket=bucket)
            print(f"Bucket '{bucket}' ya existe")
        except ClientError:
            s3.create_bucket(Bucket=bucket)
            print(f"Bucket '{bucket}' creado")

    try:
        s3.head_object(Bucket=SOURCE_BUCKET, Key=SOURCE_KEY)
        print(f"Archivo de origen OK: s3://{SOURCE_BUCKET}/{SOURCE_KEY}")
    except ClientError as exc:
        raise FileNotFoundError(
            f"No existe s3://{SOURCE_BUCKET}/{SOURCE_KEY}. "
            "Sube el parquet al bucket bronze (consola MinIO: http://localhost:9001) y reintenta."
        ) from exc


def validate_nessie() :
    """Confirma que la tabla Iceberg quedo registrada en el catalogo  de nessie"""
    resp = requests.get(f"{NESSIE_API}/trees/tree/main/entries", timeout=30)
    resp.raise_for_status()
    tablas = [tuple(e["name"]["elements"]) for e in resp.json().get("entries", [])]
    print(f"Entradas en Nessie (main): {tablas}")
    if (TABLE_NAMESPACE, TABLE_NAME) not in tablas:
        raise ValueError(f"La tabla {TABLE_NAMESPACE}.{TABLE_NAME} no aparece en Nessie")
    print(f"Tabla {TABLE_NAMESPACE}.{TABLE_NAME} registrada correctamente")


default_args = {
    "owner": "laura",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# spark-submit al cluster Spark, spark lee los achivos de parquet y los guarda en una tabla de iceberg
with DAG(
    dag_id="spark_load_to_iceberg",
    description="Bronze (parquet en MinIO) -> tabla Iceberg en Nessie usando Spark",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,          # ejecución manual; cambia a "@daily" si quieres programarlo
    catchup=False,
    tags=["spark", "iceberg", "nessie", "minio"],
) as dag:

    t_prepare = PythonOperator(
        task_id="prepare_minio",
        python_callable=prepare_minio,
    )

    t_spark = SparkSubmitOperator(
        task_id="spark_load_iceberg",
        conn_id="spark_default",                       # definida en docker-compose (spark://spark-master:7077)
        application="/opt/airflow/jobs/spark_load_to_iceberg.py",
        name="spark_load_to_iceberg",
        conf={
            "spark.executor.memory": "1g",
            "spark.cores.max": "2",
        },
        verbose=True,
    )

    # consultamos la api de nessie y confirmamos que la tabla quedo registrada
    t_validate = PythonOperator(
        task_id="validate_nessie",
        python_callable=validate_nessie,
    )

    t_prepare >> t_spark >> t_validate