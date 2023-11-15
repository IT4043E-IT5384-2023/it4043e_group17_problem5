import pymongo
import json
from bson.json_util import dumps
from pymongo import MongoClient

print(f'Version of pymongo is {pymongo.__version__}')

raw_url = "mongodb://etlReaderAnalysis:etl_reader_analysis__Gr2rEVBXyPWzIrP@34.126.84.83:27017,34.142.204.61:27017,34.142.219.60:27017"

raw_session = MongoClient(raw_url)

with open("wallet-address.json") as f:
    address_set = json.load(f)

chains = ['ethereum_blockchain_etl', 'blockchain_etl', 'polygon_blockchain_etl']
for chain in chains:
    transaction = raw_session[chain].transactions.find({'receipt_status': {"$eq": 1}, 'block_timestamp': {'$gte': 1696896000}}, 
                                                       {"hash": 1, "block_number": 1, "block_timestamp":1, "from_address": 1, "to_address": 1, "value": 1, "gas": 1, "gas_price":1, "receipt_gas_used": 1})
    with open(f'{chain}_transactions.json', "w") as f:
        for record in transaction:
            if record.get('from_address', '') in address_set:
                f.write(json.dumps(record) + "\n")

    lending = raw_session[chain].lending_events.find({'block_timestamp': {'$gte': 1696896000}}, {"type": 1, "amount": 1, "block_timestamp": 1, "transaction_hash": 1, "contract_address" : 1,
                                                          "event_type": 1, "on_behalf_of" : 1, "user": 1, "wallet" : 1, "reserve": 1})
    with open(f'{chain}_lending_events.json', "w") as f:
        for record in lending:
            f.write(json.dumps(record) + "\n")

    tokens = raw_session[chain].tokens.find()
    with open(f'{chain}_tokens.json', "w") as f:
        for record in tokens:
            f.write(json.dumps(record) + "\n")
    
