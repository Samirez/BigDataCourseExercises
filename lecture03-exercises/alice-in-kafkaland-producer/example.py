from hdfs import InsecureClient
from kafka import KafkaProducer

# Create an insecure client that can read from HDFS
hdfsclient = InsecureClient('http://local_host:9092', user='superuser')
# Read the alice in wonderland text file from HDFS
text=hdfsclient.read('alice-in-kafkaland.txt')
# Write each sentence in alice in wonderland to a kafka topic with a KafkaProducer
producer = KafkaProducer(bootstrap_servers=['kafka:9092'])

for x in text:
    lines = x.readlines()
    producer.send('foo', lines)




