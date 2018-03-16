from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Popular-movie").setMaster()
sc = SparkContext(conf=conf)


lines = sc.textFile("cabdata.txt")
movies = lines.map(lambda x: (x.split(" ")[0] , x))
movieCounts = movies.reduceByKey(lambda x, y: x[3] + y[3])


results = movieCounts.collect()

for result in results:
    print(result)