from kafka import KafkaConsumer
from pymongo import MongoClient
import json

# MongoDB setup
# Adjust connection string if necessary
mongo_client = MongoClient('mongodb://127.0.0.1:27017/')
db = mongo_client['sentiment_db']
collection = db['predictions']

# Kafka Consumer setup
consumer = KafkaConsumer(
    'sentiment_predictions',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Process messages from Kafka and insert into MongoDB
for message in consumer:
    payload = message.value
    text = payload.get('text')
    sentiment = payload.get('predicted_sentiment')
    date = payload.get('date')
    time = payload.get('time')

    # Prepare MongoDB document
    document = {
        'text': text,
        'predicted_sentiment': sentiment,
        'date': date,
        'time': time
    }

    collection.insert_one(document)

    print(f"Inserted into MongoDB: {document}")
