#import datetime
import time
import schedule
import requests
from json import dumps, loads

# from faker import Faker
from kafka import KafkaProducer


'''
kafka_nodes = "kafka:9092"
myTopic = "weather"

def gen_data():
  faker = Faker()

  prod = KafkaProducer(bootstrap_servers=kafka_nodes, value_serializer=lambda x:dumps(x).encode('utf-8'))
  my_data = {'city': faker.city(), 'temperature': random.uniform(10.0, 110.0)}
  print(my_data)
  prod.send(topic=myTopic, value=my_data)

  prod.flush()

if __name__ == "__main__":
  gen_data()
  schedule.every(10).seconds.do(gen_data)

  while True:
    schedule.run_pending()
    time.sleep(0.5)
'''

KAFKA_SERVER = "kafka:9092"
KAFKA_TOPIC = "github-events"
GITHUB_EVENTS_URL = "https://api.github.com/orgs/microsoft/events"

def gen_data():

  producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER, value_serializer=lambda x:dumps(x).encode('utf-8')) 

  try:
    response = requests.get(GITHUB_EVENTS_URL) #, headers={"Authorization": "token YOUR_GITHUB_TOKEN"})
    events = response.json()
    print(len(events))
    for event in events:
      if "id" not in event:
        print(event)
      producer.send(KAFKA_TOPIC, event)
  except Exception as e:
    print(f"Error: {e}")

  producer.flush()

if __name__ == "__main__":
  gen_data()
  schedule.every(20).seconds.do(gen_data) # Respect GitHub API rate limits

  while True:
    schedule.run_pending()
    time.sleep(0.5)