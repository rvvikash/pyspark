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
STORED AS TEXTFILE, STORED AS TEXTFILE LOCATION 'user/entity/site/inbound/CSS_INSTALLATION_PARTITIONED';;

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


create table with partion and bucket ?

CREATE TABLE IF NOT EXISTS employees_table (
    emp_id INT,
    salary DECIMAL(10, 2)
)
PARTITIONED BY (department STRING)
CLUSTERED BY (emp_id) INTO 4 BUCKETS
STORED AS ORC

how to insert specfic data in particular partition

set hive.exec.dynamic.partition=true;  
set hive.exec.dynamic.partition.mode=nonstrict;  

drop table tmp.table1;

create table tmp.table1(  
col_a string,col_b int)  
partitioned by (ptdate string,ptchannel string)  
row format delimited  
fields terminated by '\t' ;  

insert overwrite table tmp.table1 partition(ptdate,ptchannel)  
select col_a,count(1) col_b,ptdate,ptchannel
from tmp.table2
group by ptdate,ptchannel,col_a ;






from pyspark import SparkContext

sc = SparkContext.getOrCreate()

# List of sentences
sentences = [
    "Hi my name is vikash",
    "Hi my name is raj"
]

# Step 1: Create RDD
rdd = sc.parallelize(sentences)

# Step 2: Split each sentence into words
rdd_words = rdd.flatMap(lambda line: line.split())

# Step 3: Create key-value pairs (word, 1)
rdd_pairs = rdd_words.map(lambda word: (word.lower(), 1))  # optional: .lower() to count case-insensitive

# Step 4: Reduce by key to count words
word_counts = rdd_pairs.reduceByKey(lambda a, b: a + b)

# Step 5: Collect result
output = word_counts.collect()

# Print result
for word, count in output:
    print(word, count)












------------------------big data --------------------------------


🔹 1. AWS & Data Engineering

You have raw transaction logs landing in S3 every 5 minutes, and downstream ML models consume transformed data hourly. How would you design an incremental ETL pipeline using Glue + EMR + partitioning strategies to minimize cost and latency?

How would you handle schema evolution in Glue when new fields are added to JSON data without breaking existing jobs?

Suppose your Glue ETL job is running out of memory when processing nested parquet files in S3. What optimizations would you apply?

Explain how you would implement fine-grained security controls on S3 data using IAM, Lake Formation, and Athena.

You have to move 100TB of on-prem Hadoop data into AWS S3 with minimal downtime. What migration strategy would you use (tools, compression, partitioning)?

How would you build a multi-zone resilient data pipeline in S3 and EMR to withstand AWS region failures?

Compare using Athena vs EMR vs Redshift Spectrum for querying S3 data. In what scenarios would you prefer one over the other?

Explain a strategy to design time-travel queries on S3 datasets (e.g., using versioning or Delta Lake).

How would you handle PII data masking while storing data in S3 but still allowing analysts to query aggregated insights via Athena?

A Glue job joins 3 very large tables (100M+ rows). What techniques (e.g., partitioning, broadcasting, bucketing) would you use to avoid shuffle bottlenecks?

🔹 2. PySpark / Python

Explain how you would implement a Slowly Changing Dimension (SCD Type 2) in PySpark efficiently on 500M rows.

You are joining a streaming dataset (Kafka) with a batch dimension table in PySpark. How do you handle late-arriving data and watermarks?

Write PySpark logic to compute a rolling 7-day average on clickstream data partitioned by user_id and ordered by timestamp.

How do you optimize PySpark jobs suffering from data skew during joins or aggregations?

Compare RDD vs DataFrame vs Dataset API in PySpark with an example where the choice significantly affects performance.

How would you implement a rule-based anomaly detection pipeline in PySpark for transaction fraud?

Describe the approach to optimize PySpark job execution when using Glue or EMR (e.g., partitioning, caching, serialization).

How do you enforce data quality checks (e.g., null %, duplicate % thresholds) at scale using PySpark?

Suppose your PySpark job failed halfway. How would you design a checkpointing / idempotent job strategy to safely restart?

How would you handle a multi-format ingestion pipeline (CSV, JSON, Avro, Parquet) with schema inference and validation in PySpark?

🔹 3. Airflow / Orchestration

Design an Airflow DAG that ingests daily customer data, validates schema, applies transformations in Spark, and loads to S3. Explain how you’d implement task retries, SLA monitoring, and error notifications.

How would you implement dynamic DAG creation in Airflow for 100+ data sources without manually writing DAGs?

Suppose a DAG fails frequently due to intermittent Kafka ingestion issues. How would you redesign the DAG with sensors, retries, and circuit breakers?

What’s the difference between Airflow XCom, Variables, and Connections? Provide an example of misuse and its consequences.

How do you implement data lineage tracking in Airflow (e.g., capturing which input tables led to which output dataset)?

How would you integrate Airflow with CI/CD pipelines (GoCD/Bitbucket) to promote DAGs across dev, UAT, prod?

How do you manage backfills and catchups for a DAG that missed 3 days of scheduled runs due to infrastructure downtime?

Your SLA requires real-time alerts for failed jobs. How would you integrate Airflow with Slack, PagerDuty, or AWS SNS?

How would you schedule an Airflow DAG to handle both streaming (Kafka) and batch jobs in a hybrid workflow?

How do you ensure idempotency in Airflow tasks that write to downstream systems (so re-runs don’t cause duplicates)?

🔹 4. Big Data Ecosystem (Hadoop / Streaming / Kafka)

Compare Kafka vs Kinesis for ingestion pipelines. When would you prefer one over the other in a bank?

Suppose your Spark Structured Streaming job is reading from Kafka and writing to S3, but you notice exactly-once guarantees are not being met. How would you debug and fix this?

You need to implement windowed aggregations on Kafka data (e.g., fraud detection in 5-min windows). How do you ensure correctness in the presence of late data?

What’s the difference between checkpointing and WAL (write-ahead logs) in Spark Structured Streaming?

You have Hive data stored in ORC format but ML teams want Parquet. What’s the best way to convert and optimize storage for multiple consumers?

Explain how you’d handle HDFS small files problem in a Spark + Hive setup.

How would you build a real-time + batch hybrid pipeline using Kafka + Spark + Hive, ensuring both consistency and low latency?

Explain the role of YARN vs Kubernetes in Spark cluster management. What trade-offs exist?

How would you optimize Hive queries running slowly due to unoptimized partitioning?

Suppose you have to ingest API + Kafka + Flat files into a common Lakehouse. How do you design a unified ingestion layer?
