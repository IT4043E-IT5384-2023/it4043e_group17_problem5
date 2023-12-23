from flask import Flask, request, jsonify
from kafka_jobs import send_to_kafka_inference
import random
import json
from google.cloud import storage
from pyspark.sql import SparkSession

app = Flask(__name__)
gcs_path = f"gs://it4043e-it5384/it4043e/it4043e_group17_problem5/prediction_final"


def download_from_gcs(bucket_name, blob_name):
    """Downloads data from a given GCS bucket"""
    storage_client = storage.Client.from_service_account_json('gcs.json')
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    return blob.download_as_string()

spark = SparkSession.builder \
    .appName("KafkaKMeanInit") \
    .config("spark.jars", "jar/gcs-connector-hadoop3-latest.jar") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .getOrCreate()

spark._jsc.hadoopConfiguration().set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
spark._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.enable", "true")
spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile", 'gcs.json')


@app.route('/predict', methods=['POST'])
def send_address():
    data = request.json
    address = data.get('address')
    if not address:
        return jsonify({"error": "No address provided"}), 400
    df = spark.read.parquet(gcs_path)
    if not df.filter(df['address'] == address).count() > 0:
        send_to_kafka_inference({'address': address})
    return jsonify({"status": "Address sent to Kafka"})


@app.route('/result/<reference_id>', methods=['GET'])
def get_result(reference_id):
    # Construct the GCS path for the Parquet directory

    try:
        df = spark.read.parquet(gcs_path)

        prediction_df = df.filter(df.address == reference_id).select("prediction")

        prediction_result = prediction_df.collect()
        mapping = {'0': 'RISKY', '1': 'SAFE'}
        if prediction_result:
            return jsonify({"status": "Completed", "reference_id": reference_id, "prediction": mapping[prediction_result[0]['prediction']]})
        else:
            return jsonify({"status": "Not Found", "message": "No prediction found for the provided reference ID"}), 404
    except Exception as e:
        return jsonify({"status": "Error", "message": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
