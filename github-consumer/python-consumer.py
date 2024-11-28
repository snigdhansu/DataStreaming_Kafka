#import datetime
from json import loads

# from faker import Faker
from kafka import KafkaConsumer

KAFKA_SERVER = "kafka:9092"
KAFKA_TOPIC = "trending-github-repos"

if __name__ == "__main__":
  consumer = KafkaConsumer(KAFKA_TOPIC, 
                           bootstrap_servers=KAFKA_SERVER, 
                           group_id='groupdId-919292',
                           auto_offset_reset='earliest',
                           value_deserializer=lambda x: loads(x.decode('utf-8')))
  print("Consumer started")
  for message in consumer:
      repo_info = message.value
      print(repo_info['f0'], repo_info['f1'])