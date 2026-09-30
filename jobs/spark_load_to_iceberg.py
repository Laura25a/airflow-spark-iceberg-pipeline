"""
Spark-load-to-Iceberg  (versión .py del notebook original, para ejecutarse con Airflow).

Es el mismo notebook: cada "# %%" es una celda. Solo se modificó lo que no funciona
fuera de Jupyter y se añadió lo necesario para correr con spark-submit.
Los cambios/adiciones están marcados con "# [AIRFLOW]".
"""

# [AIRFLOW] En Jupyter existe display(); con spark-submit no. Este "shim" permite dejar
# las celdas display(...) tal cual estaban en el notebook.
try:
    display
except NameError:
    def display(obj):
        if hasattr(obj, "show"):
            obj.show(5)          # DataFrame de Spark
        else:
            print(obj)           # DataFrame de pandas, etc.
import socket  # [AIRFLOW] usado abajo para spark.driver.host

# %% Celda 0
# Librerías generales
#=========================
import json
from json import dumps
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, explode, from_json, col, current_date
from pyspark.sql.types import StringType, StructType, StructField, ArrayType
import datetime 
import requests
import urllib3
import matplotlib as mtl
from datetime import datetime,date
import pytz
import io
import glob
import os
import boto3

# %% Celda 1
CATALOG_URI = "http://nessie:19120/api/v1"  # Nessie Server URI
WAREHOUSE = "s3://gold/"               # Minio Address to Write to
STORAGE_URI = "http://minio:9000"      # [AIRFLOW] antes: IP de docker inspect (http://172.18.0.6:9000); ahora el nombre del servicio
AWS_ACCESS_KEY='admin'
AWS_SECRET_KEY='password'

# %% Celda 2
# Configuración de Minio
minio_client = boto3.client(
    's3',
    #endpoint_url='http://172.18.0.4:9000',  # Usar el nombre del servicio de Docker
    endpoint_url='http://minio:9000',
    aws_access_key_id='admin',
    aws_secret_access_key='password',
    region_name='us-east-1'
)

# %% Celda 3
from pyspark.sql import SparkSession
import pyspark

# Configuración combinada
conf = (
    pyspark.SparkConf()
        .setAppName('combined_spark_app')
        # [AIRFLOW] Con spark-submit esta línea se ignora: los JARs ya vienen dentro de las imágenes
        # (Dockerfile / Dockerfile.spark). Se deja igual para que el notebook siga funcionando en Jupyter.
        # Paquetes para PostgreSQL, Iceberg, Nessie, AWS SDK y Hadoop AWS
        .set('spark.jars.packages', 'org.postgresql:postgresql:42.7.3,'
                                    'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,'
                                    'org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,'
                                    'software.amazon.awssdk:bundle:2.24.8,'
                                    'software.amazon.awssdk:url-connection-client:2.24.8,'
                                    'org.apache.hadoop:hadoop-aws:3.2.0,'
                                    'com.amazonaws:aws-java-sdk-bundle:1.11.534')
        # Extensiones de Iceberg y Nessie
        .set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,'
                                     'org.projectnessie.spark.extensions.NessieSparkSessionExtensions')
        # Configuración de Nessie como catálogo Iceberg
        .set('spark.sql.catalog.nessie', 'org.apache.iceberg.spark.SparkCatalog')
        .set('spark.sql.catalog.nessie.uri', CATALOG_URI)
        .set('spark.sql.catalog.nessie.ref', 'main')
        .set('spark.sql.catalog.nessie.authentication.type', 'NONE')
        .set('spark.sql.catalog.nessie.catalog-impl', 'org.apache.iceberg.nessie.NessieCatalog')
        # Configuración para almacenamiento en S3 (MinIO)
        .set('spark.sql.catalog.nessie.s3.endpoint', STORAGE_URI)
        .set('spark.sql.catalog.nessie.warehouse', WAREHOUSE)
        .set('spark.sql.catalog.nessie.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
        # [AIRFLOW] AÑADIDO: el driver corre en el contenedor de Airflow (modo client),
        # los executors del cluster deben poder conectarse de vuelta a él
        .set('spark.driver.host', socket.gethostbyname(socket.gethostname()))
        .set('spark.driver.bindAddress', '0.0.0.0')
        # [AIRFLOW] AÑADIDO: MinIO requiere path-style también en el S3FileIO de Iceberg
        .set('spark.sql.catalog.nessie.s3.path-style-access', 'true')
        .set('spark.sql.catalog.nessie.client.region', 'us-east-1')
        # Configuración para acceso a S3 directamente desde Spark (sin Nessie)
        .set('spark.hadoop.fs.s3a.endpoint', STORAGE_URI)
        .set('spark.hadoop.fs.s3a.access.key', AWS_ACCESS_KEY)
        .set('spark.hadoop.fs.s3a.secret.key', AWS_SECRET_KEY)
        .set('spark.hadoop.fs.s3a.path.style.access', 'true')
         .set("spark.hadoop.fs.s3a.aws.credentials.provider",
             "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        
)

# %% Celda 4
# Creación de la sesión de Spark
spark = SparkSession.builder \
    .config(conf=conf) \
    .getOrCreate()
print("Spark Session Started")

# %% Celda 5
df_taxis = spark.read.parquet("s3a://bronze/yellow_tripdata_2025-01.parquet")

# %% Celda 6
# Celda 1: versión del driver (tu notebook)
import pyspark
print("Driver PySpark:", pyspark.__version__)

# Celda 2: versión del master/executor (desde Spark mismo)
print("Spark version en sesión:", spark.version)

# %% Celda 7
df_taxis.printSchema()

# %% Celda 8
df_taxis.show(5)

# %% Celda 9
display(df_taxis)

# %% Celda 10
pdf = df_taxis.limit(10).toPandas()
display(pdf)  

# %% Celda 11
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold;").show()

# %% Celda 12
df_taxis.writeTo("nessie.gold.yellowtrip").createOrReplace()

# %% [AIRFLOW] AÑADIDO: verificación de que la tabla quedó escrita
print("Filas en nessie.gold.yellowtrip:", spark.table("nessie.gold.yellowtrip").count())

# %% Celda 14
spark.stop()
