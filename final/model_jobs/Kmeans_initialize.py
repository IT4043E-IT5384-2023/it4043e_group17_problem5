from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql.functions import udf, col
from pyspark.sql.types import ArrayType, IntegerType, StringType
from pyspark.ml.linalg import VectorUDT

from eth.vm.opcode_values import *
from eth.vm.logic import *

OPCODES = {v: k for k, v in globals().items() if k.isupper()}
KAFKA_SERVER = 'localhost:9092'
BUCKET_CREDENTIAL = 'gcs.json'
BUCKET_NAME = 'it4043e-it5384'


spark = SparkSession.builder \
    .appName("KafkaKMeanInit") \
    .config("spark.jars", "jar/gcs-connector-hadoop3-latest.jar") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .getOrCreate()

spark._jsc.hadoopConfiguration().set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
spark._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.enable", "true")
spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile", BUCKET_CREDENTIAL)

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
gcs_input_path = "gs://it4043e-it5384/it4043e/it4043e_group17_problem5/prediction_final"
df = spark.read.format("parquet").load(gcs_input_path)


transformed_df = df.withColumn("features", bytecode_to_features_udf(df["bytecode"]))
assembler = VectorAssembler(inputCols=["features"], outputCol="features_vector")
vector_df = assembler.transform(transformed_df)
scaler = StandardScaler(inputCol="features_vector", outputCol="scaled_features")

scaler_model = scaler.fit(vector_df)
scaled_df = scaler_model.transform(vector_df)
kmeans = KMeans().setK(2)  
model = kmeans.fit(scaled_df.select(col('scaled_features').alias('features')))

scaler_model_path = "gs://it4043e-it5384/it4043e/it4043e_group17_problem5/StandardScaler"
gcs_output_path = "gs://it4043e-it5384/it4043e/it4043e_group17_problem5/Kmeans"
scaler_model.write().overwrite().save(scaler_model_path)
model.write().overwrite().save(gcs_output_path)

spark.stop()