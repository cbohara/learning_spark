from pyspark.sql import SparkSession


def basic_df_example(spark):
    # creates a DataFrame based on the content of a JSON file
    df = spark.read.json("examples/src/main/resources/people.json")
    # displays content of dataframe to stout
    df.show()
    # print schema in tree format
    df.printSchema()

    # select only the name column
    df.select("name").show()
    # select everybody but increment the age by 1
    df.select(df['name'], df['age'] + 1).show()
    # filter people over 21
    df.filter(df['age'] > 21).show()
    # count people by age
    df.groupBy("age").count().show()

    # register table so it is available for SQL contextc
    df.createOrReplaceTempView("people")
    # sql function on a SparkSession enables applications to run SQL queries
    # programmatically and returns the result as a DataFrame
    sqlDF = spark.sql("SELECT * FROM people")
    sqlDF.show()


if __name__ == "__main__":
    # entry point into all functionality in Spark is the SparkSession class
    spark = SparkSession \
            .builder \
            .appName("Python Spark SQL basic example") \
            .config("spark.some.config.option", "some-value") \
            .getOrCreate()

    basic_df_example(spark)

    spark.stop()
