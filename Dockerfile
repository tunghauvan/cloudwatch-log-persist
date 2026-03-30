FROM python:3.14-slim

RUN apt-get update && apt-get install -y \
    openjdk-21-jre-headless \
    curl \
    procps \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-arm64

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

ENV SPARK_HOME=/usr/local/lib/python3.14/site-packages/pyspark
ENV PATH=$PATH:$SPARK_HOME/bin

COPY src /app/src
COPY examples /app/examples
COPY config.yaml /app/config.yaml

ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

CMD ["tail", "-f", "/dev/null"]
