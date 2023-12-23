import requests
import sys
import time
contract = sys.argv[1]
url = 'http://localhost:5000'
post_response = requests.post(f'{url}/predict', json={'address': contract})
print(post_response.json())
time.sleep(1)
get_response = requests.get(f'{url}/result/{contract}')
print(get_response.json())