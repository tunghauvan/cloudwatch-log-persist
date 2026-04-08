FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    curl \
    procps \
    gcc \
    g++ \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY src /app/src
COPY examples /app/examples
COPY config.yaml /app/config.yaml

CMD ["tail", "-f", "/dev/null"]
