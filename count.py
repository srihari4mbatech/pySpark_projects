from pyspark import SparkContext
sc=SparkContext("local","count app")
words= sc.parallelize(["scala", 
   "java", 
   "hadoop", 
   "spark", 
   "akka",
   "spark vs hadoop", 
   "pyspark",
   "pyspark and spark"]
)
counts= words.count()
coll=words.collect()
print("Number of elements in RDD- >{}".format(counts))
print("Elements in RDD -> {}".format(coll))
