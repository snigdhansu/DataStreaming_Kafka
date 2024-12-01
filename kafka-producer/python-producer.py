#import datetime
import time
import schedule
import requests
from json import dumps
from datetime import datetime, timedelta

from kafka import KafkaProducer

KAFKA_SERVER = "kafka:9092"
KAFKA_TOPIC = "github-events"
GITHUB_EVENTS_URL = "https://api.github.com/orgs/microsoft/events"

# Initialize last fetched timestamp as one minute ago
last_fetched_timestamp = (datetime.utcnow() - timedelta(minutes=1)).isoformat() + "Z"  # ISO 8601 format

def gen_data():
  global last_fetched_timestamp
  
  producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER, value_serializer=lambda x:dumps(x).encode('utf-8')) 
  
  try:
    response = requests.get(GITHUB_EVENTS_URL, headers={"If-Modified-Since": last_fetched_timestamp})
    event_count = 0
    if response.status_code == 200:
      events = response.json()

      for event in events:
        # Ensure only new recent modified events are sent to the topic
        if "id" in event and event['created_at']>last_fetched_timestamp:
          event_count += 1
          producer.send(KAFKA_TOPIC, event)
          
      # Update the last_fetched_timestamp
      if events:
        last_fetched_timestamp = events[0]['created_at']
        print(f"Updated last_fetched_timestamp: {last_fetched_timestamp}")
        print(f"Total Events fetched: {event_count}")

        
    elif response.status_code == 304:
      print("No new events since the last fetch.")
      
    else:
      print(f"Error: {response.status_code}, {response.text}")
    
  except Exception as e:
    print(f"Error: {e}")

  producer.flush()

if __name__ == "__main__":
  gen_data()
  schedule.every(20).seconds.do(gen_data) # Respect GitHub API rate limits
  
  while True:
    schedule.run_pending()
    time.sleep(0.5)