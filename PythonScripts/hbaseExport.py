import happybase

connection = happybase.Connection('gw01.itversity.com')

table = connection.table('idleTrips')


with open("hdfs:/user/jainpriyansh786/SortedCabData/000000_0",'r') as f :
    for i in f.readlines():
        data = i.split(",")
        print(data)
        key = data[0]
        table.put(key, {'c:day': data[1], 'c:month': data[2] , 'c:year': data[3] , 'c:duration': data[4]})


connection.close()
