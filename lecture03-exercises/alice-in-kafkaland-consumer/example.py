from hdfs import InsecureClient
from kafka import KafkaConsumer

# Create a KafkaConsumer that consumes messages from the topic 'alice-in-kafkaland'
consumer = KafkaConsumer('alice-in-kafkaland')

# Combine all the messages into a single string
text = ""
for msg in consumer:
    text += msg
# Write the string to HDFS in a file called 'alice-in-kafkaland.txt'
hdfsclient = InsecureClient('http://local_host:9092', user='superuser')
hdfsclient.upload(text, "alice-in-kafkaland.txt")

