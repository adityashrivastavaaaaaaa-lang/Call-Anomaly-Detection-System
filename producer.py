"""
Producer that generates fake call center events and sends them to Kafka.
Simulates:
- Call Drops
- Long Hold Times (AHT spikes)
- Agent Unavailable
"""
import time
import json
import random
import uuid
from datetime import datetime
from kafka import KafkaProducer

# Configuration
KAFKA_BROKER = 'localhost:29092'
TOPIC = 'call-events'

# Initialize Producer
def get_producer():
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
        except Exception as e:
            print(f"Waiting for Kafka... ({e})")
            time.sleep(5)

producer = get_producer()


def generate_call_event():
    """Generates a random call event."""
    call_id = str(uuid.uuid4())[:8]
    agent_id = f"agent_{random.randint(100, 110)}"
    
    # Randomly decide scenario
    scenario = random.choices(
        ['normal', 'long_call', 'dropped', 'abandoned'],
        weights=[0.7, 0.1, 0.1, 0.1],
        k=1
    )[0]
    
    event = {
        'call_id': call_id,
        'timestamp': datetime.utcnow().isoformat(),
        'agent_id': agent_id,
        'scenario': scenario
    }
    
    # Generate specific metrics based on scenario
    if scenario == 'normal':
        event['event_type'] = 'call_ended'
        event['duration'] = random.uniform(100, 300) # Normal duration
        event['status'] = 'completed'
    elif scenario == 'long_call':
        event['event_type'] = 'call_ended'
        event['duration'] = random.uniform(601, 900) # High AHT > 600s
        event['status'] = 'completed'
    elif scenario == 'dropped':
        event['event_type'] = 'call_ended'
        event['duration'] = random.uniform(5, 30)
        event['status'] = 'dropped'
    elif scenario == 'abandoned':
        event['event_type'] = 'call_ended'
        event['duration'] = 0
        event['status'] = 'abandoned'
        
    return event

print(f"Starting Producer to topic: {TOPIC}...")

if producer:
    while True:
        event = generate_call_event()
        print(f"Sending: {event}")
        producer.send(TOPIC, event)
        producer.flush()
        
        # Simulate traffic rate
        time.sleep(random.uniform(0.5, 2.0))
else:
    print("No Kafka connection. Exiting.")
