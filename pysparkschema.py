# Import Libraries
from pyspark.sql.types import StructType, StructField, FloatType, BooleanType
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql import SparkSession
from pyspark import SparkContext
import pyspark
from pyspark import SQLContext
# Setup the Configuration
conf = pyspark.SparkConf()
sc = SparkContext.getOrCreate();
spark_context = SparkSession.builder.config(conf=conf).getOrCreate()
sqlcontext = SQLContext(sc)
# Setup the Schema
schema = StructType([
StructField("User ID", IntegerType(),True),
StructField("Username", StringType(),True),
StructField("Browser", StringType(),True),
StructField("OS", StringType(),True),
])
# Add Data
data = ([(1580, "Barry", "FireFox", "Windows" ),
(5820, "Sam", "MS Edge", "Linux"),
(2340, "Harry", "Vivaldi", "Windows"),
(7860, "Albert", "Chrome", "Windows"),
(1123, "May", "Safari", "macOS"),
(232,"vikash","fgh","android"),
])
# Setup the Data Frame
user_data_df = sqlcontext.createDataFrame(data,schema=schema)
