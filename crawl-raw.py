import pymongo
import json
from bson.json_util import dumps
from pymongo import MongoClient

print(f'Version of pymongo is {pymongo.__version__}')

def dump_data(data, filename):
    with open(filename, 'w') as file:
        for record in data:
            file.write(json.dumps(record) + "\n")

raw_url = "mongodb://etlReaderAnalysis:etl_reader_analysis__Gr2rEVBXyPWzIrP@34.126.84.83:27017,34.142.204.61:27017,34.142.219.60:27017"

raw_session = MongoClient(raw_url)

chains = ['ethereum_blockchain_etl', 'blockchain_etl', 'polygon_blockchain_etl']
for chain in chains:
    transaction = raw_session[chain].transactions.find({'receipt_status': {"$eq": 1}, 'item_timestamp': {'$exists': 'true'}}, 
                                                       {"hash": 1, "block_number": 1, "block_timestamp":1, "from_address": 1, "to_address": 1, "value": 1, "gas": 1, "gas_price":1, "receipt_gas_used": 1})
    lending = raw_session[chain].lending_events.find({}, {"type": 1, "amount": 1, "block_timestamp": 1, "transaction_hash": 1, "contract_address" : 1,
                                                          "event_type": 1, "on_behalf_of" : 1, "user": 1, "wallet" : 1, "reserve": 1})
    dump_data(transaction, f'{chain}_transactions.json')
    dump_data(lending, f'{chain}_multi_wallets.json')
    
