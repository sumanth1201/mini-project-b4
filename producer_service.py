from flask import Flask, request, jsonify
from kafka import KafkaProducer
from datetime import datetime
import json

app = Flask(__name__)
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],  # Kafka hostname in Docker network
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


@app.route('/send_message', methods=['POST'])
def send_message():
    data = request.json
    message = data.get('text')
    if message:
        dt = datetime.now()
        time = dt.strftime('%H:%M:%S')
        date = dt.strftime('%Y-%m-%D')
        payload = {
            'text': message,
            'date': date,
            'time': time
        }
        print(payload)
        try:
            producer.send('incoming_messages', {'text': payload})
            producer.flush()
        except:
            return jsonify({'error': 'Failed to send message to Kafka'}), 500
        return jsonify({'status': 'Message sent to Kafka'}), 200
    else:
        return jsonify({'error': 'No text provided'}), 400


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
