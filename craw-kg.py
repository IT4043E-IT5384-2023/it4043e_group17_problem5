import pymongo
import json
from bson.json_util import dumps
from pymongo import MongoClient

print(f'Version of pymongo is {pymongo.__version__}')

def dump_data(data, filename):
    with open(filename, 'w') as file:
        for record in data:
            file.write(json.dumps(record) + "\n")

kg_url = "mongodb://klgReaderAnalysis:klgReaderAnalysis_4Lc4kjBs5yykHHbZ@35.198.222.97:27017,34.124.133.164:27017,34.124.205.24:27017/"

kg_session = MongoClient(kg_url)

wallets = kg_session['knowledge_graph'].wallets.find({'dailyAllTransactions': {'$exists':  "true"}, 'balanceChangeLogs': {'$exists':  "true"}, 'chainId': {'$in': ['0x1', '0x38', '0x89']}})
multi_wallets = kg_session['knowledge_graph'].multichain_wallets.find({'balanceChangeLogs': {'$exists':  "true"}})
smart_contracts = kg_session['knowledge_graph'].smart_contracts.find({'chainId': {'$in': ['0x1', '0x38', '0x89']}})

dump_data(wallets, 'wallets.json')
dump_data(multi_wallets, 'multi_wallets.json')
dump_data(smart_contracts, 'smart_contracts.json')
