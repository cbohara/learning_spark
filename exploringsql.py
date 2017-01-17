from pyspark.sql import SparkSession

# entry point into all functionality in Spark is the SparkSession class
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# creates a DataFrame based on the content of a JSON file
df = spark.read.json("testweet.json")
# displays content of dataframe to stout
df.show()
# print schema in tree format
df.printSchema()

# select only the name column
df.select("name").show()
