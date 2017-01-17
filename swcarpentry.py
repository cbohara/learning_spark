from pyspark.sql import SparkSession


if __name__ == "__main__":
    # entry point into all functionality in Spark is the SparkSession class
    spark = SparkSession \
            .builder \
            .appName("Python Spark SQL basic example") \
            .config("spark.some.config.option", "some-value") \
            .getOrCreate()

    # csv file will be transformed into dataframe
    peopleDF = spark.read.load("data/swcarpentry/person.csv", format="csv")

    spark.stop()
