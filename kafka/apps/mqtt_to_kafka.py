import paho.mqtt.client as mqtt
from kafka import KafkaProducer

MQTT_BROKER = 'test.mosquitto.org'
MQTT_TOPIC = 'n1odeconf/eu'
KAFKA_TOPIC = 'iot-data'

def on_connect(client, userdata, flags, rc):
    print('Connected to MQTT broker with result code ' + str(rc))
    client.subscribe(MQTT_TOPIC)
    client.publish(MQTT_TOPIC,'Hello')


def on_message(client, userdata, msg):
    print('Connected to MQTT broker: ' + str(msg.payload))

    producer.send(KAFKA_TOPIC, msg.payload)
    print('Message published to Kafka topic ' + KAFKA_TOPIC)

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.connect(MQTT_BROKER, 1883, 60)

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

client.loop_forever()
