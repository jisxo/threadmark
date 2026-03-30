import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, window, max as spark_max
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

class OrderProcessor:
    def __init__(self, spark):
        self.spark = spark
        # [Fix 1] Initialize schema in constructor to avoid AttributeError
        self.schema = self.get_order_schema()

    def get_order_schema(self):
        """Defines the Debezium payload schema for the orders table."""
        return StructType([
            StructField("after", StructType([
                StructField("order_id", IntegerType()),
                StructField("customer_id", IntegerType()),
                StructField("shop_id", IntegerType()),
                StructField("status", StringType()),
                StructField("created_at", LongType()),
            ])),
            StructField("op", StringType())
        ])

    def transform(self, df):
        """Main cleansing logic: Parsing, Flattening, and Flagging."""
        
        # [Fix 2] Cast BINARY to STRING to prevent DATATYPE_MISMATCH
        # [Fix 3] Parse and immediately flatten the nested structure
        parsed_df = df.select(
            from_json(col("value").cast("string"), self.schema).alias("data")
        ).select("data.after.*", "data.op")

        # 2. Add Event Time and other core fields
        flat_df = parsed_df.select(
            col("order_id"),
            col("customer_id"),
            col("shop_id"),
            col("status"),
            (col("created_at") / 1000000).cast("timestamp").alias("event_time")
        )

        # 3. Add priority scores to handle 'Out-of-Order' events
        scored_df = flat_df.withColumn(
            "status_score",
            when(col("status") == "PENDING", 1)
            .when(col("status") == "SHIPPED", 2)
            .when(col("status") == "DELIVERED", 3)
            .otherwise(0)
        )

        # 4. Detect Swapped Shop (Logic: if shop_id is 999, it's a race condition)
        # [Fix 4] Explicitly create 'is_swapped' column before grouping
        flagged_df = scored_df.withColumn(
            "is_swapped",
            when(col("shop_id") == 999, True).otherwise(False)
        )

        # 5. Final Aggregation using Watermarking
        final_df = flagged_df \
            .withWatermark("event_time", "10 minutes") \
            .groupBy(
                col("order_id"),
                col("shop_id"),
                col("is_swapped"),
                window(col("event_time"), "10 minutes")
            ) \
            .agg(
                spark_max("status_score").alias("final_status_score")
            )

        return final_df

    def save_to_s3(self, batch_df, batch_id):
        """Saves processed batch to MinIO (S3) in Parquet format."""
        s3_path = "s3a://silver-layer/orders_cleansed"
        
        # Extract start and end times from the window struct
        db_df = batch_df.select(
            "order_id", "shop_id", "is_swapped",
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "final_status_score"
        )
        
        # Save to S3 bucket in Parquet format (Append mode)
        db_df.write.format("parquet").mode("append").save(s3_path)


    def run(self):
        """Starts the streaming pipeline."""
        KAFKA_SERVER = "kafka:29092"
        TOPIC = "cdc.public.orders"

        raw_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_SERVER) \
            .option("subscribe", TOPIC) \
            .option("startingOffsets", "earliest") \
            .load()

        clean_df = self.transform(raw_df)

        # Use foreachBatch to execute the S3 save function
        query = clean_df.writeStream \
            .foreachBatch(self.save_to_s3) \
            .outputMode("update") \
            .option("checkpointLocation", "s3a://silver-layer/checkpoints/orders_v1") \
            .start()

        query.awaitTermination()

def create_spark_session(app_name):
    """
    Initializes SparkSession and configures S3 (MinIO) settings 
    using environment variables.
    """
    # 1. Initialize SparkSession
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()

    # 2. Set Log Level
    spark.sparkContext.setLogLevel("WARN")

    # 3. Apply Hadoop configurations for S3A
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    
    # Load from environment variables
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")
    endpoint = os.getenv("MINIO_ENDPOINT")

    hadoop_conf.set("fs.s3a.access.key", access_key)
    hadoop_conf.set("fs.s3a.secret.key", secret_key)
    hadoop_conf.set("fs.s3a.endpoint", endpoint)
    
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

    return spark

if __name__ == "__main__":
    # Now the entry point is clean and focused on execution
    spark_session = create_spark_session("OrderCleansingService")
    
    processor = OrderProcessor(spark_session)
    processor.run()
    