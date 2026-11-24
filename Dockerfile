FROM apache/spark-py:latest
USER root
RUN pip install --no-cache-dir pandas pyarrow "psycopg[binary]>=3.1,<4"
WORKDIR /app
