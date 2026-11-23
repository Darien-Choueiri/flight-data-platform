FROM python:3.12-slim
RUN pip install --no-cache-dir confluent-kafka pyarrow "psycopg[binary]>=3.2,<4"
WORKDIR /app
