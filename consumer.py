from kafka import KafkaConsumer, KafkaProducer
from const import *
import sys

try:
    input_topic = sys.argv[1]
    output_topic = sys.argv[2]
except:
    print('Usage: python3 consumer <input_topic> <output_topic>')
    exit(1)

consumer = KafkaConsumer(
    input_topic,
    bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT],
    group_id='consumer-group',
    auto_offset_reset='earliest'
)

producer = KafkaProducer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])

print(f'Consuming from "{input_topic}" and publishing to "{output_topic}"...')

for msg in consumer:
    original = msg.value.decode()
    processed = 'PROCESSED: ' + original.upper()
    print(f'Received: {original}')
    print(f'Publishing: {processed}')
    producer.send(output_topic, value=processed.encode())
    producer.flush()
