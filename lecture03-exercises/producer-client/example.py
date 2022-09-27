<<<<<<< HEAD
from kafka import KafkaProducer
# input
string = str(input())
  
# output
print(string)
producer = KafkaProducer(bootstrap_servers=['kafka:9092'])

for msg in string:
    producer.send('book', string)

producer.close()
=======
from operator import truediv
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers='kafka:9092')
exit = False
while not exit:
    input = input()
    if(input == "exit"):
        exit == True
        break
    producer.send('foobar', )
for _ in range(100):
   
>>>>>>> upstream/main
