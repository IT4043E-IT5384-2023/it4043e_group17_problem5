from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeansModel
from pyspark.ml.linalg import Vectors
from pyspark.ml.pipeline import PipelineModel
from pyspark.ml.feature import StandardScalerModel, VectorAssembler
from pyspark.sql.functions import udf, col, from_json, explode, concat, collect_list, struct, size
from pyspark.sql.types import ArrayType, IntegerType, StringType, StructType, StructField
from pyspark.ml.linalg import VectorUDT

from eth.vm.opcode_values import *
from eth.vm.logic import *

OPCODES = {v: k for k, v in globals().items() if k.isupper()}


ES_NODES = '34.143.255.36'  
ES_PORT = '9200'        
ES_RESOURCE = 'group17-final_test' 
ES_CONF = {
    "es.nodes": ES_NODES,
    "es.port": ES_PORT,
    "es.resource": ES_RESOURCE,
    "es.mapping.id": "address",  
    "es.write.operation": "index" 
}


KAFKA_SERVER = 'localhost:9092'
BUCKET_CREDENTIAL = 'gcs.json'
BUCKET_NAME = 'it4043e-it5384'
MALICIOUS_ADDRESS = 'it4043e/it4043e_group17_problem5/malicious_address.txt'

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
    .appName("KafkaInference") \
    .config("spark.jars", "jar/gcs-connector-hadoop3-latest.jar") \
    .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:8.11.3,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .config("spark.es.nodes", ES_NODES) \
    .config("spark.es.port", ES_PORT) \
    .config("spark.es.resource", f"{ES_RESOURCE}/_doc") \
    .config("spark.es.nodes.wan.only", "true") \
    .config("spark.es.net.http.auth.user", "elastic")  \
    .config("spark.es.net.http.auth.pass", "elastic2023")  \
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


def bytecode_to_opcodes(bytecode):
    opcode_d = {}
    if bytecode.startswith('0x'):
        bytecode = bytecode[2:]

    opcode_list = []

    i = 0
    while i < len(bytecode):
        op_hex = bytecode[i:i+2]
        i += 2

        op_val = int(op_hex, 16)
        op = OPCODES.get(op_val, 'INVALID/UNKNOWN')
        opcode_d[op] = opcode_d.get(op, 0) + 1
    opcode_features = [opcode_d.get(i, 0) for i in ['SSTORE', 'POP', 'MSTORE', 'SWAP1', 'STOP', 'DUP9', 'RETURN', 'SWAP2', 'DUP12', 'JUMP']]
    return Vectors.dense(opcode_features)


bytecode_to_features_udf = udf(bytecode_to_opcodes, VectorUDT())

df = df.select(from_json(col("value").cast("string"), message_schema).alias("data")) \
    .select("data.*")

def combine_and_extract_timestamps(tx_array, itx_array):
    combined_array = tx_array + itx_array
    
    timestamps = [item['timeStamp'] for item in combined_array]
    
    return timestamps

combine_and_extract_timestamps_udf = udf(combine_and_extract_timestamps, ArrayType(StringType()))
df = df.withColumn("combined_timestamps", combine_and_extract_timestamps_udf(col("tx"), col("itx")))

df = df.withColumn("num_transactions", size(col("tx")))
df = df.withColumn("num_internal_transactions", size(col("itx")))

df = df.select(
    col("address"),
    col("combined_timestamps").alias("TxList"),
    col("supply"),
    col("bytecode"),
    col("num_transactions"),
    col("num_internal_transactions")
)


kmeans_model = KMeansModel.load("gs://it4043e-it5384/it4043e/it4043e_group17_problem5/Kmeans")
scaler_model = StandardScalerModel.load("gs://it4043e-it5384/it4043e/it4043e_group17_problem5/StandardScaler")

def writeToElastic(df, epoch_id):
    df.write.format("org.elasticsearch.spark.sql") \
        .options(**ES_CONF) \
        .mode("append") \
        .save()

df = df.withColumn("features", bytecode_to_features_udf(col("bytecode")))

df = scaler_model.transform(df)

predictions = kmeans_model.transform(df.select(col('scaledFeatures').alias('features'), col('address'), col('supply'), col("TxList"),col("num_transactions"), col("num_internal_transactions")))

query = predictions \
    .select(
        col("supply"),
        col("address"),
        col("TxList"),
        col("prediction").cast("string"),
        col("num_transactions"),
        col("num_internal_transactions")
    ) \
    .writeStream \
    .outputMode("append") \
    .foreachBatch(lambda df, epoch_id: (
    df.persist(),
    df.write
        .format("parquet")
        .mode("append")
        .option("path", "gs://it4043e-it5384/it4043e/it4043e_group17_problem5/prediction_final")
        .option("checkpointLocation", "gs://it4043e-it5384/it4043e/it4043e_group17_problem5/checkpoint/predictions_test3")
        .save(),
    writeToElastic(df, epoch_id),
    df.unpersist()
)) \
    .start()

query.awaitTermination()