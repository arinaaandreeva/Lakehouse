FROM bitnami/spark:3.4.1  

USER root

# Установка Python зависимостей
RUN pip install --no-cache-dir \
    mlflow==2.9.2 \
    delta-spark==2.4.0 \
    pyspark==3.4.1 \
    pandas \
    py4j==0.10.9.7 \
    scikit-learn

# Создаем структуру папок
RUN mkdir -p /app/data/bronze \
    /app/data/silver \
    /app/data/gold \
    /app/logs \
    /app/mlruns

WORKDIR /app

# Копируем файлы
COPY src/Spark_app.py /app/src/
COPY src /app/src
COPY data/podcasts.csv /app/data/podcasts.csv

# Копирование runner.sh и установка прав
COPY /src/runner.sh /app/runner.sh
RUN chmod +x /app/runner.sh

ENV SPARK_LOCAL_DIRS=/tmp/spark
ENV SPARK_WORKER_MEMORY=1g

# Вариант 1: Если runner.sh должен запускать spark-submit
ENTRYPOINT ["/app/runner.sh"]

# ИЛИ Вариант 2: Если нужно напрямую запускать spark-submit
# ENTRYPOINT ["/opt/bitnami/spark/bin/spark-submit"]
# CMD ["--packages", "io.delta:delta-core_2.12:2.4.0", \
#      "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension", \
#      "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog", \
#      "/app/src/spark_app.py"]
# Команда запуска
# CMD ["bash", "-c", "/opt/bitnami/spark/bin/spark-submit \
#     --packages io.delta:delta-core_2.12:2.4.0 \
#     --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
#     --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
#     --conf spark.sql.shuffle.partitions=200 \
#     --conf spark.databricks.delta.optimizeWrite.enabled=true \
#     --conf spark.databricks.delta.autoCompact.enabled=true \
#     /app/src/spark_app.py"]



# FROM bitnami/spark:latest
# # FROM apache/spark:3.3.1-python3

# USER root

# # Установка только необходимых зависимостей
# RUN pip install --no-cache-dir \
#     mlflow==2.9.2 \
#     delta-spark==2.4.0 \
#     pyspark==3.4.1 \
#     pandas \
#     py4j==0.10.9.7 \
#     scikit-learn

# # Создаем структуру папок
# RUN mkdir -p /app/data/bronze \
#     /app/data/silver \
#     /app/data/gold \
#     /app/logs 

# WORKDIR /app

# # Копируем файлы
# COPY src /app/src
# COPY data /app/data

# # Команда запуска без дополнительных пакетов (они уже установлены)
# CMD ["bash", "-c", "/opt/bitnami/spark/bin/spark-submit \
#     --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
#     --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
#     /app/src/spark_app.py"]