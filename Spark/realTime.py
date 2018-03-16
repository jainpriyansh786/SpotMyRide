from pyspark import SparkConf , SparkContext
from pyspark.streaming import  StreamingContext
from pyspark.streaming.kafka import  KafkaUtils

import happybase




def sendPartition(iter):
    connection = happybase.Connection('gw01.itversity.com', port=9060)
    table = connection.table('idletrips')
    #unoccupiedcabs = {}
    for record in iter:
        if len(record.split(" ")) == 5 :
             cabId , lat , lng , occ , timestamp = record.split(" ")
             if int(occ) == 0:
                 table.put(cabId, {'c:lat': lat, 'c:lng': lng , 'c:occ': occ , 'c:timestamp': timestamp})
             else:
                 if int(occ) == 1:
                     table.delete(cabId)
        else:
            print('error in record')
            continue


        #connection.send(record)
    connection.close()


conf = SparkConf().setAppName("realtimecablocation")

sc = SparkContext(conf = conf)

sc.setLogLevel("WARN")

ssc = StreamingContext(sc , 10)

ssc.checkpoint("/user/jainpriyansh786/sparkLogs")

kafkaStream = KafkaUtils.createStream(ssc , "nn01.itversity.com:2181","spark-streaming",{'location_data1':5} )
kafkaStreamData = kafkaStream.map(lambda line : line[1])
#occupiedCab = kafkaStreamData.filter(lambda line: line.split(" ")[3] != None and int(line.split(" ")[3]) == 1)
#unoccupiedCab = kafkaStreamData.filter(lambda line: line.split(" ")[3] != None and int(line.split(" ")[3]) == 0)

kafkaStreamData.foreachRDD(lambda rdd: rdd.foreachPartition(sendPartition))

#occupiedCab.saveAsTextFiles("/home/jainpriyansh786","txt")
#unoccupiedCab.saveAsTextFiles("/home/jainpriyansh786","txt")
#occupiedCab.pprint()
#unoccupiedCab.pprint()
ssc.start()
ssc.awaitTermination()










