from kafka import KafkaProducer
# input
string = str(input())
  
# output
print(string)
producer = KafkaProducer(bootstrap_servers=['kafka:9092'])

for msg in string:
    producer.send('book', string)

producer.close()
