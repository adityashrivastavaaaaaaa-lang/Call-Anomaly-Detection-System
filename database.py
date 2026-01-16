from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Text, func
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime

Base = declarative_base()

class CallEvent(Base):
    __tablename__ = 'call_events'
    
    id = Column(Integer, primary_key=True)
    call_id = Column(String, nullable=False)
    timestamp = Column(DateTime, default=datetime.utcnow)
    event_type = Column(String, nullable=False) # 'call_started', 'call_ended', 'agent_unavailable'
    duration = Column(Float, nullable=True)     # In seconds (e.g. for call_ended)
    status = Column(String, nullable=True)      # 'completed', 'dropped', 'abandoned'
    agent_id = Column(String, nullable=True)
    
class Anomaly(Base):
    __tablename__ = 'anomalies'
    
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, default=datetime.utcnow)
    anomaly_type = Column(String, nullable=False) # 'high_aht', 'high_abandon_rate'
    description = Column(Text, nullable=False)
    value = Column(Float, nullable=True) # The metric value that triggered it

# Setup DB
ENGINE = create_engine('sqlite:///calls.db', echo=False)
Session = sessionmaker(bind=ENGINE)

def init_db():
    Base.metadata.create_all(ENGINE)

if __name__ == "__main__":
    init_db()
    print("Database initialized.")
