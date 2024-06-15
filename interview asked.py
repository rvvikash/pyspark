interview question :

Cinema table:

Write a solution to report the movies with an odd-numbered ID and a description that is not "boring".
 Return the result table ordered by rating in descending order.

filtered_df = df.filter((col('id') % 2 != 0) & (col('description') != 'boring'))


how to create table in hive ?

-- Create the table stored as text file in this we used to used row format and all .
CREATE TABLE cinema (
    id INT,
    movie STRING,
    description STRING,
    rating FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Create the table with ORC format
CREATE TABLE cinema (
    id INT,
    movie STRING,
    description STRING,
    rating FLOAT
)
STORED AS ORC, JSONFILE, PARQUET ;

-- Load data into the ORC table
LOAD DATA INPATH '/path/to/hdfs/datafile.csv' INTO TABLE cinema;

-- Query the table
SELECT * FROM cinema 
WHERE id % 2 != 0 AND description != 'boring' 
ORDER BY rating DESC;



how to save the data in json format,orc,parquet?
CREATE TABLE cinema (
    id INT,
    movie STRING,
    description STRING,
    rating FLOAT
)
STORED AS ORC, JSONFILE, PARQUET

how to use list comprehnsion ?

list1=[1,4,35,9,0,45,1,8]
a=[x*2 for x in list1 if x%2==0]
print(a)

how to use lambda with map ?

b=map (lambda c:c*2,list1)
print(list(b))

how to read json/orc file data as a data frame.?

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("ReadORC").getOrCreate()

# Read ORC file into a DataFrame
df_orc = spark.read.orc('/path/to/orcfile.orc')
df_json = spark.read.json('/path/to/jsonfile.json')
df_parquet = spark.read.parquet('/path/to/parquetfile.parquet')


# Show the DataFrame
df_orc.show()



Read Parquet file and select specific columns
df_parquet = spark.read.parquet('/path/to/parquetfile.parquet').select("id", "movie", "rating")

# Show the DataFrame
df_parquet.show()
