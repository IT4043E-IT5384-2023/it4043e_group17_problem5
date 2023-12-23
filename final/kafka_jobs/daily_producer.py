from eth_utils import to_checksum_address
from web3 import Web3
from web3.middleware import geth_poa_middleware
from kafka import KafkaProducer
import requests
import json
import time
import datetime

KAFKA_SERVER = 'localhost:9092'
BSC_API_KEY = 'K5ZJSJVW4D23IWGN3UFK3TSBMJHG922AA1'

def is_contract_creation(tx):
    """Check if a transaction is a contract creation"""
    return tx['to'] is None and tx['input'] != '0x'


def get_contracts_information(contract_address):
    tx_url = 'https://api.bscscan.com/api'\
            '?module=account'\
            '&action=txlist'\
            f'&address={contract_address}'\
            '&startblock=0'\
            '&endblock=99999999'\
            '&page=1'\
            '&offset=10000'\
            '&sort=asc'\
            f'&apikey={BSC_API_KEY}'

    txinternal_url = 'https://api.bscscan.com/api'\
            '?module=account'\
            '&action=txlistinternal'\
            f'&address={contract_address}'\
            '&startblock=0'\
            '&endblock=99999999'\
            '&page=1'\
            '&offset=10000'\
            '&sort=asc'\
            f'&apikey={BSC_API_KEY}'

    supply_url = 'https://api.bscscan.com/api'\
                '?module=stats'\
                '&action=tokenCsupply'\
                f'&contractaddress={contract_address}'\
                f'&apikey={BSC_API_KEY}'

    tx_response = requests.get(tx_url).json()
    if tx_response['status'] == '1':
        tx_list = tx_response['result']
    else:
        tx_list = []

    itx_response = requests.get(txinternal_url).json()
    if itx_response['status'] == '1':
        itx_list = itx_response['result']
    else:
        itx_list = []
    
    supply_response = requests.get(supply_url).json()
    if supply_response['status'] == '1':
        supply = supply_response['result']
    else:
        supply = 0
    
    return {'tx': tx_list, 'itx': itx_list, 'supply': supply}
    

def send_contracts():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    bsc_node_url = 'https://bsc-dataseed.binance.org/'  
    web3 = Web3(Web3.HTTPProvider(bsc_node_url))
    web3.middleware_onion.inject(geth_poa_middleware, layer=0)
    block_number = web3.eth.block_number
    count = 0
    while count < 100:
        block = web3.eth.get_block(block_number, full_transactions=True)
        for tx in block['transactions']:
            if is_contract_creation(tx):
                tx_receipt = web3.eth.get_transaction_receipt(tx['hash'].hex())
                contract_address = tx_receipt['contractAddress']
                count += 1
                contract_address= Web3.to_checksum_address(contract_address)
                bytecode = web3.eth.get_code(contract_address)
                api_response = get_contracts_information(contract_address)
                api_response['address'] = contract_address
                api_response['bytecode'] = bytecode.hex()
                api_response['timestamp'] = str(datetime.datetime.now())
                producer.send('daily_contracts', value=api_response)              
            if count >= 100:
                return 
        block_number -= 1
    return 

if __name__ == "__main__":
    send_contracts()