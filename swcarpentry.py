import functools
from pyspark.sql import SparkSession


def explore_people(peopleDF):
    """Explore people data."""
    # temp view to explore data
    peopleDF.createOrReplaceTempView("people")
    # use .sql() to explore dataframe using SQL programmatically
    sqlDF = spark.sql("SELECT * FROM people")
    sqlDF.show()


if __name__ == "__main__":
    # entry point into all functionality in Spark is the SparkSession class
    spark = SparkSession \
            .builder \
            .appName("Python Spark SQL basic example") \
            .config("spark.some.config.option", "some-value") \
            .getOrCreate()

    # csv file transformed into dataframe
    peopleDF = spark.read.load("data/swcarpentry/person.csv", format="csv")
surveyDF = spark.read.load("data/swcarpentry/survey.csv", format="csv")

    spark.stop()
