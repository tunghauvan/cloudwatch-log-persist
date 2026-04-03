FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    openjdk-21-jre-headless \
    curl \
    procps \
    gcc \
    g++ \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-arm64

WORKDIR /app

COPY requirements.txt /app/requirements.txt
# pyiceberg[spark] will automatically handle downloading correct Iceberg & Hadoop JARs
RUN pip install --no-cache-dir -r requirements.txt

# Download all JARs for Iceberg + S3 support
RUN SPARK_JARS_DIR=$(python -c "import pyspark; import os; print(os.path.join(os.path.dirname(pyspark.__file__), 'jars'))") && \
    curl -s https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.0/iceberg-spark-runtime-3.5_2.12-1.5.0.jar -o $SPARK_JARS_DIR/iceberg-spark-runtime-3.5_2.12-1.5.0.jar && \
    curl -s https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -o $SPARK_JARS_DIR/hadoop-aws-3.3.4.jar && \
    curl -s https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -o $SPARK_JARS_DIR/aws-java-sdk-bundle-1.12.262.jar && \
    echo "✓ Downloaded Iceberg Spark Runtime + Hadoop AWS JARs"

ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark
ENV PATH=$PATH:$SPARK_HOME/bin
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

COPY src /app/src
COPY examples /app/examples
COPY config.yaml /app/config.yaml

CMD ["tail", "-f", "/dev/null"]
