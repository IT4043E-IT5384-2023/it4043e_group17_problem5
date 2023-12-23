# Group17_Problem5
To crawl the data, run file run.sh with the command
to read.

```
bash run.sh
```
To run the system, there are something needs to put in mind:
- Package installation
```
pip install -r requirements.txt
```
- In this project, we use local Kafka due to the fact the Centic Kafka is crowded. The Kafka we config is at 'localhost:9092'.
- Put the Google Cloud Storage credential file and name it as final/gcs.json
- Now you are ready to run!
```
cd final/
python run_schedule.py
```
- To check if a smart contract might be fraudulent or not, you can call the API at 'localhost:5000/predict' and localhost:5000/result, or run the python file
```
python sample_request.py <Contract hash>
```
- Access the two dashboards summarize the system at: http://34.143.255.36:5601/s/it4043e-group17-problem5/app/dashboards