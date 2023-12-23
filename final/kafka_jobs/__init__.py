from . import inference_producer
import time
import datetime
from .daily_producer import get_contracts_information

def send_to_kafka_inference(data):
    bytecode = inference_producer.call_bytecodes(data['address'])
    api_response = get_contracts_information(data['address'])
    api_response['address'] = data['address']
    api_response['bytecode'] = bytecode
    api_response['timestamp'] = str(datetime.datetime.now())
    inference_producer.producer.send('daily_contracts', value=api_response) 
