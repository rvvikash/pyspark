#to connect plsql using Jdbc and performing spark operation
Things which need to take care 
1) need to load jar file for connection using jdbc and it required when you want to connect any application with database and after that need to copy the path to make
a connection .
https://jdbc.postgresql.org/download/



from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.jars", "/Users/vikashraj/PycharmProjects/pyspark/devl/example1/src/main/python/library/postgresql-42.7.3.jar") \
    .getOrCreate()

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "employee") \
    .option("user", "postgres") \
    .option("password", "ADMIN") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df.show()
