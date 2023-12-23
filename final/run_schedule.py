import schedule
import subprocess
import time
import threading

script1 = 'app.py'
script2 = 'model_jobs/Kmeans_predict.py'
script3 = 'kafka_jobs/data_consumer.py'
script4 = 'kafka_jobs/daily_producer.py'  
script5 = 'model_jobs/Kmeans_initialize.py' 

def run_script(script_path):
    """Function to run a Python script using subprocess in a new thread."""
    def target():
        subprocess.run(['python', script_path])
    thread = threading.Thread(target=target)
    thread.start()

run_script(script1)
run_script(script2)
run_script(script3)

schedule.every().hour.do(run_script, script4)

schedule.every().day.at("00:00").do(run_script, script5)  

while True:
    schedule.run_pending()
    time.sleep(1)
