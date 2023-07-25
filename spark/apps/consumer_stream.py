from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# set the batch interval to 1 second
batch_interval = 5

# initialize the SparkSession
spark = SparkSession.builder.appName('KafkaSparkStreamingDemo').getOrCreate()

spark.sparkContext.setLogLevel('WARN')

# define the schema for the Kafka messages
schema = StructType([
    StructField('timestamp', StringType(), True),
    StructField('temperature', DoubleType(), True)
])

# create a DataFrame that reads from the Kafka topic
kafka_df = spark \
    .readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'kafka:9093') \
    .option('subscribe', 'temperature') \
    .load()

# decode the Kafka messages as JSON and add a timestamp column
parsed_df = kafka_df \
    .select(from_json(col('value').cast('string'), schema).alias('data')) \
    .select('data.*', current_timestamp().alias('timestamp'))

# write the parsed data to the console
query = parsed_df \
    .writeStream \
    .outputMode('append') \
    .format('console') \
    .start()

# wait for the query to finish
query.awaitTermination()