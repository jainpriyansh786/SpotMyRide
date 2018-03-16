
from kafka import  KafkaProducer
import time



file_path = '/home/jainpriyansh786'

def dataStreamer(kafka_topic_name):



      with open("000000_0") as f :

          p = KafkaProducer(bootstrap_servers = 'nn02.itversity.com:6667',acks = 1)

          i = 0

          for line in f :

                topic_key = line.split(' ')[0]
                print(topic_key)
                p.send(kafka_topic_name,value = line.rstrip() , key = topic_key  )
                i = i +1
                if i > 3 :
                    break
                time.sleep(.1)
      f.close()

dataStreamer("location_data")

