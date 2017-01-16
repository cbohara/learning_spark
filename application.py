from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf=conf)

# transformation example
inputRDD = sc.textFile("README.md")
# filter method returns pointer to new RDD
pythonRDD = inputRDD.filter(lambda x: "python" in x)
# the original inputRDD was not changed by the last filter method
fastRDD = inputRDD.filter(lambda x: "fast" in x)
# can apply union method to both RDDs
union_example = pythonRDD.union(fastRDD)
