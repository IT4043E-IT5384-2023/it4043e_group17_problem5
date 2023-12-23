from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, from_json
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType

KAFKA_SERVER = 'localhost:9092'
BUCKET_CREDENTIAL = 'gcs.json'
BUCKET_NAME = 'it4043e-it5384'

message_schema = StructType([
    StructField("address", StringType()),
    StructField("bytecode", StringType()),
    StructField("timestamp", StringType()),
    StructField("tx", ArrayType(
        StructType([
            StructField("blockNumber", StringType()),
            StructField("timeStamp", StringType()),
            StructField("hash", StringType()),
            StructField("from", StringType()),
            StructField("to", StringType()),
            StructField("value", StringType()),
            StructField("input", StringType())
        ])
    )),
    StructField("itx", ArrayType(
        StructType([
            StructField("blockNumber", StringType()),
            StructField("timeStamp", StringType()),
            StructField("hash", StringType()),
            StructField("from", StringType()),
            StructField("to", StringType()),
            StructField("value", StringType()),
            StructField("input", StringType())
        ])
    )),
    StructField("supply", StringType()),
])

spark = SparkSession.builder \
    .appName("KafkaToGCS") \
    .config("spark.jars", "jar/gcs-connector-hadoop3-latest.jar") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .getOrCreate()

spark._jsc.hadoopConfiguration().set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
spark._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.enable", "true")
spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile", BUCKET_CREDENTIAL)

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVER) \
    .option("subscribe", "daily_contracts") \
    .load()

processed_df = df.select(from_json(col("value").cast("string"), message_schema).alias("data")) \
    .select("data.*")

def write_to_gcs(df, epoch_id):
    df.write \
        .format("parquet") \
        .mode("append") \
        .option("path", "gs://it4043e-it5384/it4043e/it4043e_group17_problem5/daily_contracts") \
        .save()

query = processed_df.writeStream \
    .foreachBatch(write_to_gcs) \
    .outputMode("append") \
    .start()

query.awaitTermination()
