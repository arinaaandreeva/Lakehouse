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


ENTRYPOINT ["/app/runner.sh"]

# 