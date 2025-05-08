import mlflow
import mlflow.spark
from pyspark.sql import functions as F
from delta.tables import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when, mean
from delta import configure_spark_with_delta_pip
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType


builder = (
    SparkSession.builder.appName("DeltaLake")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
    .config("spark.sql.debug.maxToStringFields", "100")
    .config("spark.sql.shuffle.partitions", "2")  # Уменьшение параллелизма
    .config("spark.databricks.delta.optimizeWrite.enabled", "false")  # Отключаем для малых данных
    .config("spark.databricks.delta.autoCompact.enabled", "false")    # Отключаем авто-компактизацию
    .config("spark.driver.memory", "1g")          # Лимит памяти драйвера
    .config("spark.executor.memory", "1g")        # Лимит памяти исполнителя
    .config("spark.memory.fraction", "0.5")       # Уменьшаем кэширование
    .config("spark.memory.storageFraction", "0.3") # Меньше памяти для хранения
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
# spark.sparkContext.setLogLevel("INFO")

mlflow.set_tracking_uri("file:/app/logs/mlruns")
# print('session started')

# ===============Bronze Layer==================
with mlflow.start_run(run_name="BronzeLayer"):

    schema = StructType([
    StructField("id", IntegerType(), True),    
    StructField("Podcast_Name", StringType(), True),
    StructField("Episode_Title", StringType(), True),
    StructField("Episode_Length_minutes", FloatType(), True),
    StructField("Genre", StringType(), True),
    StructField("Host_Popularity_percentage", FloatType(), True),
    StructField("Publication_Day", StringType(), True),
    StructField("Publication_Time", StringType(), True),
    StructField("Guest_Popularity_percentage", FloatType(), True),
    StructField("Number_of_Ads", IntegerType(), True),
    StructField("Episode_Sentiment", StringType(), True),
    StructField("Listening_Time_minutes", FloatType(), True)
    ])

    raw_df = spark.read.schema(schema).option("header", "true").csv("/app/data/podcasts.csv")
    raw_df.show(5)
    raw_df.repartition(1).write.format("delta") \
        .mode("overwrite") \
        .save("/app/data/bronze/raw_podcasts")
  
    # print('data loaded')
# ========== Silver Layer ==========
with mlflow.start_run(run_name="SilverLayer"):

    raw_df_1 = spark.read.format("delta").load("/app/data/bronze/raw_podcasts")
    print(f"Записано строк: {raw_df_1.count()}")
    # Преобразования категориальных признаков, изменение типов данных, удаление выбросов из столбца "Кол-во реклам", добавление новых признаков
    day_mapping = {"Monday":1, "Tuesday":2, "Wednesday":3, "Thursday":4, 
                  "Friday":5, "Saturday":6, "Sunday":7}
    time_mapping = {"Morning":1, "Afternoon":2, "Evening":3, "Night":4}
    sentiment_map = {"Negative":1, "Neutral":2, "Positive":3}

    day_udf = udf(lambda x: day_mapping.get(x, None), IntegerType())
    time_udf = udf(lambda x: time_mapping.get(x, None), IntegerType())
    sentiment_udf = udf(lambda x: sentiment_map.get(x, None), IntegerType())

    cleaned_df = raw_df_1 \
        .withColumn("Publication_Day", day_udf(col("Publication_Day"))) \
        .withColumn("Publication_Time", time_udf(col("Publication_Time"))) \
        .withColumn("Episode_Sentiment", sentiment_udf(col("Episode_Sentiment"))) \
        .withColumn("Episode_Length_minutes", col("Episode_Length_minutes").cast("float")) \
        .withColumn("Host_Popularity_percentage", col("Host_Popularity_percentage").cast("float")) \
        .withColumn("Guest_Popularity_percentage", col("Guest_Popularity_percentage").cast("float")) \
        .withColumn("Number_of_Ads",when(col("Number_of_Ads").isNull(), 0).otherwise(when(col("Number_of_Ads") > 3, 0).otherwise(col("Number_of_Ads"))))

    # Разделение данных ДО target encoding, чтобы не было утечки данных
    train_df, test_df = cleaned_df.randomSplit([0.8, 0.2], seed=42)

    # Target Encoding только на train данных
    for col_name in ["Genre", "Podcast_Name"]:
        target_means = train_df.groupBy(col_name).agg(
            mean("Listening_Time_minutes").alias(f"{col_name}_target_enc")
        )
        
        train_df = train_df.join(target_means.hint("broadcast"), col_name, "left")
        test_df = test_df.join(target_means.hint("broadcast"), col_name, "left")
        
        # Заполняем пропуски
        global_mean = train_df.select(mean("Listening_Time_minutes")).first()[0]
        train_df = train_df.fillna({f"{col_name}_target_enc": global_mean})
        test_df = test_df.fillna({f"{col_name}_target_enc": global_mean})
   
    # print('data transformed')
    train_df.write.format("delta").mode("overwrite").save("/app/data/silver/train")
    # print("Data saved successfully train_df")
    test_df.write.format("delta").mode("overwrite").save("/app/data/silver/test")
    # print("Data saved successfully test_df")

    #полный набор для аналитики 
    full_df = train_df.union(test_df)
    full_df.write.format("delta").mode("overwrite").save("/app/data/silver/full")
    # print("Data saved successfully full_df")

    DeltaTable.forPath(spark, "/app/data/silver/train").optimize().executeZOrderBy(["Genre"])
    DeltaTable.forPath(spark, "/app/data/silver/test").optimize().executeZOrderBy(["Genre"])


# ========== Gold Layer ==========

with mlflow.start_run(run_name="Gold_Layer"):
    print('start gold layer')
    train_df = spark.read.format("delta").load("/app/data/silver/train")
    test_df = spark.read.format("delta").load("/app/data/silver/test")
    
    ml_df = train_df.drop("Episode_Title", "Podcast_Name")
    ml_df.write.format("delta").mode("overwrite").save("/app/data/gold/train")
    
    test_df = test_df.drop("Episode_Title", "Podcast_Name")
    test_df.write.format("delta").mode("overwrite").save("/app/data/gold/test")

    # Агрегации для аналитики (на полных данных)
    full_df = spark.read.format("delta").load("/app/data/silver/full")
    gold_agg = full_df.groupBy("Genre", "Publication_Day").agg(
        F.mean("Listening_Time_minutes").alias("avg_listening_time"),
        F.sum("Number_of_Ads").alias("total_ads")
    )
    # gold_agg.write.format("delta").save("/app/data/gold/")
    # print('end gold layer')
    
# ============ ML Model===================

train_df = spark.read.format("delta").load("/app/data/gold/train")
test_df = spark.read.format("delta").load("/app/data/gold/test")

feature_cols = [
    "Genre_target_enc", 
    "Host_Popularity_percentage",
    "Guest_Popularity_percentage",
    "Publication_Day",
    "Episode_Length_minutes",
    "Number_of_Ads",
    "Publication_Time",
    "Episode_Sentiment"
]

gbt = GBTRegressor(
    labelCol="Listening_Time_minutes",
    featuresCol="features",
    maxDepth=5,                   
    maxIter=100,                  
    stepSize=0.05,                
    maxBins=32,            
    seed=42                    
)
pipeline = Pipeline(stages=[
    VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="skip"),
    gbt
])


mlflow.set_experiment("/Podcasts/GBT")
try:
    with mlflow.start_run(run_name="GBT_optimized"):
        # Проверка данных перед обучением
        print(f"Train count: {train_df.count()}")
        print(f"Test count: {test_df.count()}")
        train_df.select(feature_cols).summary().show()
        
        # Обучение
        model = pipeline.fit(train_df)
    
        predictions = model.transform(test_df)
        evaluator = RegressionEvaluator(labelCol="Listening_Time_minutes")
        
        mlflow.log_metrics({
            "rmse": evaluator.evaluate(predictions, {evaluator.metricName: "rmse"}),
            "r2": evaluator.evaluate(predictions, {evaluator.metricName: "r2"})
        })
        
        mlflow.spark.log_model(model, "model")
        
except Exception as e:
    print(f"Ошибка: {str(e)}")
    spark.stop()
    raise


# 
# docker build -t lakehouse-toy-spark .
# docker run -it --rm lakehouse-toy-spark
# docker run -it --entrypoint /bin/bash lakehouse-toy-spark
# /ls -la /app/data/bronze/