
# Añadimos 3.9 para que coincida con el de la imagen de spark 
FROM apache/airflow:2.9.0-python3.9

USER root

# Instalar OpenJDK-17 y herramientas necesarias, incluido wget
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk ant procps wget && \
    apt-get clean

# Configurar JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64/

USER airflow

# Copiar el archivo requirements.txt
COPY requirements.txt /tmp/requirements.txt

# Instalar paquetes desde el archivo requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# JARs de Iceberg, Nessie y S3 dentro del pyspark del driver (mismos que Dockerfile.spark),
# así spark-submit no descarga nada en tiempo de ejecución
RUN wget --tries=5 --timeout=30 https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget --tries=5 --timeout=30 https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    wget --tries=5 --timeout=30 https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.24.8/bundle-2.24.8.jar && \
    wget --tries=5 --timeout=30 https://repo1.maven.org/maven2/software/amazon/awssdk/url-connection-client/2.24.8/url-connection-client-2.24.8.jar