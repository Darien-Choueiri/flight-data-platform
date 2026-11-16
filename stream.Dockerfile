FROM python:3.12-slim
RUN pip install --no-cache-dir confluent-kafka pandas pyarrow
WORKDIR /app
