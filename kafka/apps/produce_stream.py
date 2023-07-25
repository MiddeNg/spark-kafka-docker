import time
import json
import random
from kafka import KafkaProducer

# initialize the Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# generate random temperature readings and send them to Kafka
while True:
    # simulate a temperature reading
    temperature = round(random.uniform(0, 100), 2)
    
    # create a JSON message containing the temperature reading and a timestamp
    message = {'timestamp': int(time.time()), 'temperature': temperature}
    message_json = json.dumps(message).encode('utf-8')
    
    # send the message to Kafka
    producer.send('temperature', message_json)
    print(f"Sent message: {message_json}")
    
    # wait for a random time between 1 and 5 seconds before sending the next message
    time.sleep(1)