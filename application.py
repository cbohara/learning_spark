from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf=conf)

# transformations return new RDD
inputRDD = sc.textFile("README.md")
# filter method returns pointer to new RDD
pythonRDD = inputRDD.filter(lambda x: "Python" in x)
# the original inputRDD was not changed by the last filter method
fastRDD = inputRDD.filter(lambda x: "fast" in x)
# can apply union method to both RDDs
union_example = pythonRDD.union(fastRDD)

# actions force the evaluation of transformations and produce output
print("Input had " + str(pythonRDD.count()) + " instances of python.")
# take retreives a number of elements from the RDD
for line in fastRDD.take(10):
    # iterate over the elements locally to print out info at the driver
    print(line)

# parallelize() takes collection of objects into driver program > create a RDD
nums = sc.parallelize([1, 2, 3, 4])
# collect() should only be used on small local datasets
squared = nums.map(lambda x: x ** x).collect()
# display output of new RDD created by map transformations
for num in squared:
    print(num)
