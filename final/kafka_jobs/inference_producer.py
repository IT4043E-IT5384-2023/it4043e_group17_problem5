from kafka import KafkaProducer
import requests
from google.cloud import storage
import json
import web3
from web3 import Web3
import datetime
import time


BSC_API_KEY = 'K5ZJSJVW4D23IWGN3UFK3TSBMJHG922AA1'
KAFKA_SERVER = 'localhost:9092'
BUCKET_CREDENTIAL = 'gcs.json'
BUCKET_NAME = 'it4043e-it5384'
MALICIOUS_ADDRESS = 'it4043e/it4043e_group17_problem5/train_addresses.txt'


def call_bytecodes(address):
    http_provider_url = 'https://bsc-dataseed.binance.org/'
    w3 = Web3(Web3.HTTPProvider(http_provider_url))
    contract_address= Web3.to_checksum_address(address)
    bytecode = w3.eth.get_code(contract_address)
    return bytecode.hex()

def produce_to_kafka(topic, message):
    """Produces a message to a Kafka topic"""
    producer.send(topic, value=message)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)