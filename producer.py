import json
import time
from confluent_kafka import Producer

class KafkaProducer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.producer = Producer({'bootstrap.servers': bootstrap_servers})

    def delivery_report(self, err, msg):
        if err is not None:
            print(f"Failed to send message: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

    def send_message(self, topic, key, value):
        json_value = json.dumps(value)
        self.producer.produce(topic, key=key, value=json_value, callback=self.delivery_report)
        self.producer.flush()

    def send_messages_from_file(self, file_path, topic):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                news_data = json.load(f)
            
            for news_object in news_data:
                key = str(news_object['_id'])
                self.send_message(topic, key, news_object)

            print(f"All messages sent to topic '{topic}'.")

            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump([], f, ensure_ascii=False, indent=4)

            print(f"Cleared the JSON file '{file_path}'.")

        except FileNotFoundError:
            print(f"File not found: {file_path}")
        except json.JSONDecodeError:
            print(f"Error decoding JSON from file: {file_path}")

if __name__ == "__main__":
    producer = KafkaProducer()
    
    while True:
        producer.send_messages_from_file('news_articles.json', 'test')
        print("Waiting for 30 minutes before reading the JSON file again...")
        time.sleep(1800)  
