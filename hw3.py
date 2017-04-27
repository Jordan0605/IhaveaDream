from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("hw3")
sc = SparkContext(conf=conf)

a = sc.textFile("IhaveaDream.txt")
#print a.first()
c = a.flatMap(lambda line: line.split(" ")) \
	.map(lambda word: (word, 1)) \
	.reduceByKey(lambda a, b: a + b) \
	.map(lambda (a, b): (b, a)) \
	.sortByKey(False)
for x in c.collect():
	print x
