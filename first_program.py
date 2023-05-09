from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()
my_path="C:/Users/Dell/Desktop/5m Sales Records.csv"
df = spark.read.format("csv").option("header", "true").load(my_path)
df.createOrReplaceTempView("mytable")
# df.summary()
# result = spark.sql("SELECT * from mytable order by Region asc ")
# result.show(3)
distinctDF = df.distinct()
distinctDF.show(truncate=False)
