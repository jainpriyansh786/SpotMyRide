
from kafka import  KafkaConsumer
import sys
import os
import datetime
import time


def dataConsumer(kafka_topic_name , kafka_group , temp_file_path , hdfs_path):


          kafka = KafkaConsumer('location_data',bootstrap_servers='nn01.itversity.com:6667',group_id = 'cab')
          messages = kafka.poll()

          if not messages :
              print('No new events in kafka topic')
              return


          temp_file = temp_file_path+'/'+kafka_topic_name + '_'+ kafka_group +'_'+ str(time.time())+'.txt'
          f = open(temp_file,'w')

          for partition , message  in messages.iteritems() :
              for i in message:
                  #f.write(str(msg.topic) + ' ' + str(msg.partition) + ' '+str( msg.offset)+ ' ' +str( msg.key) + ' '  +str( msg.value)+'\r\n' )
                  print(i.value)
                  f.write(str(i.key)+ ' '+ str(i.partition) + ' ' + str(i.offset)+ ' '  +str(i.value)+'\r\n')

          f.close()
          kafka.commit()

          os.system('hadoop fs -put '+ temp_file + '    '+ hdfs_path)
          os.system('mv ' + temp_file + ' ' +  '/home/jainpriyansh786/data_archive' )
          print(' Messages Archived')







if __name__ == '__main__':

    topic = 'location_data'
    consumer_group = 'cab'
    output_path = '/home/jainpriyansh786/data'
    hdfs_path = 'data'
    dataConsumer(topic,consumer_group,output_path , hdfs_path)


