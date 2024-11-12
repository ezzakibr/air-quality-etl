from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, corr, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, LongType

# Définir le schéma des données entrantes
schema = StructType([
    StructField("city", StringType()),
    StructField("timestamp", LongType()),
    StructField("temperature", DoubleType()),
    StructField("air_quality", StructType([
        StructField("main", StructType([
            StructField("aqi", IntegerType())
        ])),
        StructField("components", StructType([
            StructField("co", DoubleType()),
            StructField("no2", DoubleType()),
            StructField("o3", DoubleType()),
            StructField("so2", DoubleType()),
            StructField("pm2_5", DoubleType()),
            StructField("pm10", DoubleType())
        ])),
        StructField("dt", LongType())
    ]))
])

# Créer une session Spark
spark = SparkSession.builder.appName("AirQualityProcessor").getOrCreate()

# Lire le flux de données depuis Kafka
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "air_quality_data") \
    .load()

# Parser les données JSON
parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Extraire les champs pertinents
air_quality_df = parsed_df.select(
    col("city"),
    col("timestamp"),
    col("temperature"),
    col("air_quality.main.aqi"),
    col("air_quality.components.co"),
    col("air_quality.components.no2"),
    col("air_quality.components.o3"),
    col("air_quality.components.so2"),
    col("air_quality.components.pm2_5"),
    col("air_quality.components.pm10")
)

# Calculer la moyenne mobile sur une fenêtre de 24 heures
windowed_avg = air_quality_df \
    .groupBy(
        window(col("timestamp").cast("timestamp"), "24 hours", "1 hour"),
        col("city")
    ) \
    .agg(
        avg("temperature").alias("avg_temperature"),
        avg("aqi").alias("avg_aqi"),
        avg("co").alias("avg_co"),
        avg("no2").alias("avg_no2"),
        avg("o3").alias("avg_o3"),
        avg("so2").alias("avg_so2"),
        avg("pm2_5").alias("avg_pm2_5"),
        avg("pm10").alias("avg_pm10")
    )

def process_batch(df, epoch_id):
    # Calculer les corrélations
    correlations = df.groupBy("city").agg(
        corr("avg_temperature", "avg_aqi").alias("temp_aqi_corr"),
        corr("avg_temperature", "avg_co").alias("temp_co_corr"),
        corr("avg_temperature", "avg_no2").alias("temp_no2_corr"),
        corr("avg_temperature", "avg_o3").alias("temp_o3_corr"),
        corr("avg_temperature", "avg_so2").alias("temp_so2_corr"),
        corr("avg_temperature", "avg_pm2_5").alias("temp_pm2_5_corr"),
        corr("avg_temperature", "avg_pm10").alias("temp_pm10_corr")
    )

    # Calculer les seuils de température
    thresholds = df.groupBy("city").agg(
        avg(when(col("avg_aqi") >= 100, col("avg_temperature")).otherwise(None)).alias("moderate_aqi_temp_threshold"),
        avg(when(col("avg_so2") >= 80, col("avg_temperature")).otherwise(None)).alias("high_so2_temp_threshold"),
        avg(when(col("avg_no2") >= 70, col("avg_temperature")).otherwise(None)).alias("high_no2_temp_threshold"),
        avg(when(col("avg_pm10") >= 50, col("avg_temperature")).otherwise(None)).alias("high_pm10_temp_threshold"),
        avg(when(col("avg_pm2_5") >= 25, col("avg_temperature")).otherwise(None)).alias("high_pm2_5_temp_threshold"),
        avg(when(col("avg_o3") >= 100, col("avg_temperature")).otherwise(None)).alias("high_o3_temp_threshold"),
        avg(when(col("avg_co") >= 9400, col("avg_temperature")).otherwise(None)).alias("high_co_temp_threshold")
    )

    # Joindre tous les résultats
    final_results = df.join(correlations, "city").join(thresholds, "city")

    # Écrire les résultats dans PostgreSQL
    final_results.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/air_quality_db") \
        .option("dbtable", "air_quality_results") \
        .option("user", "ow_project") \
        .option("password", "passedemot") \
        .mode("append") \
        .save()

# Écrire les résultats dans PostgreSQL
query = windowed_avg \
    .writeStream \
    .foreachBatch(process_batch) \
    .outputMode("update") \
    .start()

# Attendre la fin du traitement
spark.streams.awaitAnyTermination()