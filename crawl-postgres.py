import psycopg2
import json
import pandas as pd

db = psycopg2.connect(database = "postgres", user = "student_token_transfer", host='34.126.75.56', password = "svbk_2023", port = 5432)

with open("contract-address.json") as f:
    contract_address = json.load(f)

contract_address = tuple(contract_address)
cursor = db.cursor()
chains = ['chain_0x1', 'chain_0x38', 'chain_0x89']
for chain in chains:
    cursor.execute(f'SELECT * FROM {chain}.token_transfer WHERE contract_address in {contract_address}')
    column_names = [d[0] for d in cursor.description]
    results = cursor.fetchall()
    df = pd.DataFrame(results, columns=column_names)
    df.to_csv(f'{chain}_token_transfer.csv', index=False)
