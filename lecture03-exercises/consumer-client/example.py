from kafka import KafkaConsumer
consumer = KafkaConsumer('book', bootstrap_servers=['kafka:9092'], group_id='group1')

for msg in consumer:
    print(msg)

