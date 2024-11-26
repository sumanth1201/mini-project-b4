from kafka import KafkaConsumer, KafkaProducer
from transformers import BertTokenizer
import torch
from joblib import load
import json

# Kafka Consumer setup
consumer = KafkaConsumer(
    'incoming_messages',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Kafka Producer setup
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Load the sentiment model
try:
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    model = load("./bertModel.pkl")
    model.to(device)
    tokenizer = BertTokenizer.from_pretrained('bert-base-uncased')
except:
    print("Error while loading the model")
sentiment_map = {0: 'negative', 1: 'neutral', 2: 'positive'}

# Listen for new messages and make predictions
for message in consumer:
    payload = message.value.get('text')
    text = payload.get('text')
    date = payload.get('date')
    time = payload.get('time')

    if not text:
        print("No text found in the message, skipping...")
        continue

    # Tokenize and predict sentiment
    inputs = tokenizer(text, return_tensors='pt', truncation=True,
                       padding=True, max_length=512).to(device)
    with torch.no_grad():
        outputs = model(**inputs)
        predicted_class = torch.argmax(outputs.logits, dim=1).item()

    # Prepare the payload for Kafka
    response_payload = {
        'text': text,
        'predicted_sentiment': sentiment_map[predicted_class],
        'date': date,
        'time': time
    }

    # Send prediction back to Kafka
    producer.send('sentiment_predictions', response_payload)
    producer.flush()

    print(f"Processed message: {response_payload}")
