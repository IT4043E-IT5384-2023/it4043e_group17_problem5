import pymongo
import json
from bson.json_util import dumps
from pymongo import MongoClient

print(f'Version of pymongo is {pymongo.__version__}')

kg_url = "mongodb://klgReaderAnalysis:klgReaderAnalysis_4Lc4kjBs5yykHHbZ@35.198.222.97:27017,34.124.133.164:27017,34.124.205.24:27017/"
kg_session = MongoClient(kg_url)
chains = ['0x1', '0x38', '0x89']
address_set = set()
#Extract wallet of chain: Etherum, BNB and Polygon
wallets = kg_session['knowledge_graph'].wallets.find({'dailyAllTransactions': {'$exists':  "true"}, 'balanceChangeLogs': {'$exists': "true"}, 'chainId': {'$in': ['0x1', '0x38', '0x89']}}, 
{"address": 1, "balanceChangeLogs": 1, "balanceInUSD" : 1, "borrowChangeLogs": 1, "borrowInUSD": 1, "borrowTokenChangeLogs": 1, "borrow_tokens": 1,
"chainId": 1, "dailyAllTransactions": 1, "dailyNumberOfTransactions": 1, "dailyTransactionAmounts": 1, "depositChangeLogs": 1,
"depositInUSD": 1, "depositTokenChangeLogs": 1, "depositTokens": 1, "frequencyOfDappTransactions": 1, "lendings": 1, "liquidationLogs": 1, 
"numberOfLiquidation": 1, "tokenChangeLogs": 1, "tokens": 1, "totalValueOfLiquidation": 1})
with open("wallets.json", "w") as f:
    for record in wallets:
        if 'dailyAllTransactions' in record.keys():
            last_date = max([int(i) for i in record['dailyAllTransactions'].keys()])
            if last_date >= 1696896000:
                f.write(json.dumps(record) + "\n")
                address_set.add(record['address'])

#Write list of addresses to 
with open("wallet-address.json", "w") as f:
    json.dump(list(address_set), f)

#Clear object
import gc
del address_set
gc.collect()

#Extract multi wallet involve in one of these chains: Etherum, BNB and Polygon
multi_wallets = kg_session['knowledge_graph'].multichain_wallets.find({'balanceChangeLogs': {'$exists':  "true"}, 'dailyNumberOfTransactions': {'$exists':  "true"}, 'frequencyOfDappTransactionsInEachChain': {'$exists':  "true"}}, 
{"address": 1, "balanceChangeLogs": 1, "balanceInUSD": 1, "borrowChangeLogs": 1, "borrowInUSD": 1, "borrowTokenChangeLogs": 1, "borrowTokens": 1, "dailyNumberOfTransactions": 1, 
"dailyNumberOfTransactionsInEachChain": 1, "dailyTransactionAmounts": 1, "dailyTransactionAmountsInEachChain": 1, "depositChangeLogs": 1, "depositInUSD": 1, "depositTokenChangeLogs": 1,
"depositTokens": 1, "frequencyOfDappTransactions": 1, "frequencyOfDappTransactionsInEachChain": 1, "lendings": 1, "liquidationLogs": 1, "numberOfLiquidation": 1, "tokenChangeLogs": 1, 
"tokens": 1, "totalValueOfLiquidation": 1})
with open("multi_wallets.json", "w") as f:
    for record in multi_wallets:
        if sum(record['dailyNumberOfTransactions'].values()) == 0:
            continue
        for chain in chains:
            if chain in record['frequencyOfDappTransactionsInEachChain'].keys():
                break
        else:
            continue
        f.write(json.dumps(record) + "\n")

contract_set = set()
smart_contracts = kg_session['knowledge_graph'].smart_contracts.find({'chainId': {'$in': ['0x1', '0x38', '0x89']}})
with open("smart_contracts.json", "w") as f:
    for record in smart_contracts:
        f.write(json.dumps(record) + "\n")
        contract_set.add(record['address'])

with open("contract-address.json", "w") as f:
    json.dump(list(contract_set), f)