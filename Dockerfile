FROM apache/spark-py:latest
USER root
RUN pip install --no-cache-dir pandas pyarrow
WORKDIR /app