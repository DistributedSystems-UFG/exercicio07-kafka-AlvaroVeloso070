from kafka import KafkaConsumer
from const import *
import sys

try:
    topic = sys.argv[1]
except:
    print('Usage: python3 consumer2 <topic_name>')
    exit(1)

consumer = KafkaConsumer(
    topic,
    bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT],
    group_id='consumer2-group',
    auto_offset_reset='earliest'
)

print(f'Consuming from "{topic}"...')

for msg in consumer:
    print('Received:', msg.value.decode())
