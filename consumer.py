"""
Consumer that listens to call events, detects anomalies, and updates the database.
Anomalies:
1. High AHT (Duration > 600s)
2. Abandoned Calls (Status == 'abandoned') -- Simple rule for now
3. Dropped Calls
"""
import json
from kafka import KafkaConsumer
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from database import CallEvent, Anomaly, ENGINE
from datetime import datetime

# Configuration
KAFKA_BROKER = 'localhost:29092'
TOPIC = 'call-events'

Session = sessionmaker(bind=ENGINE)
session = Session()

# Initialize Consumer
def get_consumer():
    while True:
        try:
            return KafkaConsumer(
                TOPIC,
                bootstrap_servers=[KAFKA_BROKER],
                auto_offset_reset='latest',
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
        except Exception as e:
            print(f"Waiting for Kafka... ({e})")
            time.sleep(5)

consumer = get_consumer()
print(f"Connected to Kafka topic: {TOPIC}")


def process_event(event):
    # Store raw event
    db_event = CallEvent(
        call_id=event['call_id'],
        timestamp=datetime.fromisoformat(event['timestamp']),
        event_type=event['event_type'],
        duration=event.get('duration'),
        status=event.get('status'),
        agent_id=event.get('agent_id')
    )
    session.add(db_event)
    
    # Anomaly Detection Logic
    anomalies = []
    
    # Rule 1: High AHT
    if event.get('duration', 0) > 600:
        anomalies.append({
            'type': 'high_aht',
            'desc': f"Call {event['call_id']} duration {event['duration']:.2f}s exceeds threshold.",
            'value': event['duration']
        })
        
    # Rule 2: Abandoned Call (Simple check)
    if event.get('status') == 'abandoned':
        anomalies.append({
            'type': 'abandoned_call',
            'desc': f"Call {event['call_id']} was abandoned.",
            'value': 1.0
        })

    # Rule 3: Dropped Call
    if event.get('status') == 'dropped':
        anomalies.append({
            'type': 'dropped_call',
            'desc': f"Call {event['call_id']} was dropped unexpectedly.",
            'value': 1.0
        })

    # Store Anomalies and Alert
    for a in anomalies:
        print(f"🚨 ALERT: {a['desc']}")
        db_anomaly = Anomaly(
            timestamp=datetime.utcnow(),
            anomaly_type=a['type'],
            description=a['desc'],
            value=a['value']
        )
        session.add(db_anomaly)
    
    try:
        session.commit()
    except Exception as e:
        print(f"Error saving to DB: {e}")
        session.rollback()

if consumer:
    print("Listening for messages...")
    for message in consumer:
        event = message.value
        process_event(event)
else:
    print("No Kafka consumer available.")
