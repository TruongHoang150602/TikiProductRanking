from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType
import pymongo

# Initialize Spark session
spark = SparkSession.builder.appName("KafkaToMongoDB").getOrCreate()

# Kafka configuration
kafkaServer = "localhost:9092"  # Replace with your Kafka broker list
topic = "product-tiki"  # Replace with your Kafka topic

# Subscribe to the Kafka topic
df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafkaServer).option("subscribe", topic).load()

# Define the schema for the incoming data
schema = StructType().add("key", StringType()).add("value", StringType())
df = df.selectExpr("CAST(value AS STRING")  # Assuming the message value is in JSON format

# Parse the JSON data
df = df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Store data in MongoDB
def foreachBatchFunction(df, epoch_id):
    if not df.isEmpty():
        client = pymongo.MongoClient("mongodb://localhost:27017/")  # Replace with your MongoDB connection details
        db = client["Tiki"]  # Replace with your MongoDB database name
        collection = db["product"]  # Replace with your MongoDB collection name
        df.write.format("mongo").mode("append").option("database", "Tiki").option("collection", "product").save()

query = df.writeStream.outputMode("append").foreachBatch(foreachBatchFunction).start()

query.awaitTermination()
