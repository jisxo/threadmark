import os
from pyspark.sql import functions as F
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
                StructField("total_price", LongType()),
                StructField("paid_amount", LongType()),
                StructField("stock_quantity", IntegerType()),
                StructField("created_at", LongType()),
            ])),
            StructField("op", StringType())
        ])

    def split_data(self, df):
        # 8 major defects check and reason tagging (Data Quality Validation)
        df_tagged = df.withColumn("rejection_reason",
            F.when(F.col("created_at") < F.current_timestamp() - F.expr("INTERVAL 10 MINUTES"), "LATE_ARRIVAL") # [SCEN 1] LATE_ARRIVAL
            .when(F.col("paid_amount").isNull(), "ZOMBIE_ORDER") # [SCEN 5] ZOMBIE_ORDER
            .when(F.col("stock_quantity") < 0, "OVERSELLING") # [SCEN 6] OVERSELLING
            .when(F.col("paid_amount") < F.col("total_price"), "PRICE_HIJACK") # [SCEN 7] PRICE_HIJACK
            .when(F.col("shop_id") == 999, "SWAPPED_SHOP") # [SCEN 8] SWAPPED_SHOP
            .otherwise(None)
        )

        # 사유가 없으면 Silver(정상), 있으면 Quarantine(결함)
        silver_df = df_tagged.filter("rejection_reason IS NULL")
        quarantine_df = df_tagged.filter("rejection_reason IS NOT NULL")
        
        return silver_df, quarantine_df

    def transform(self, df):
        """Main cleansing logic: Parsing, Flattening, and Flagging."""
        
        # [Fix 2] Cast BINARY to STRING to prevent DATATYPE_MISMATCH
        # [Fix 3] Parse and immediately flatten the nested structure
        parsed_df = df.select(
            from_json(col("value").cast("string"), self.schema).alias("data")
        ).select("data.after.*", "data.op")

        # 2. Add Event Time and other core fields
        flat_df = parsed_df.withColumn(
            "event_time", (col("created_at") / 1000000).cast("timestamp")
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
                spark_max("status_score").alias("final_status_score"),
                spark_max("customer_id").alias("cust_id"),
                spark_max("total_price").alias("total_price"),
                spark_max("paid_amount").alias("paid_amount"),
                spark_max("stock_quantity").alias("stock_quantity"),
                spark_max("event_time").alias("created_at")
            )

        return final_df

    def save_to_s3(self, batch_df, batch_id):
        """Saves processed batch to Silver and Quarantine paths with monitoring."""
        print(f"\n" + "="*20 + f" BATCH {batch_id} PROCESSING START " + "="*20)

        # 1. Split data using validation logic (This adds 'rejection_reason' column)
        df_silver, df_quarantine = self.split_data(batch_df)
        
        # 2. Monitor counts for console logging
        silver_count = df_silver.count()
        quarantine_count = df_quarantine.count()
        
        print(f"[*] Summary - Silver: {silver_count} records / Quarantine: {quarantine_count} records")

        # 3. Handle Clean Data (Silver)
        if silver_count > 0:
            s3_silver_path = "s3a://silver-layer/orders_cleansed"
            # Select specific columns for Silver Layer
            df_silver.select(
                "order_id", "cust_id", "shop_id", "total_price", "paid_amount",
                "stock_quantity", "is_swapped", "final_status_score", "created_at"
            ).write.format("parquet").mode("append").save(s3_silver_path)
            print(f"[OK] Successfully saved {silver_count} records to Silver Layer.")

        # 4. Handle Dirty Data (Quarantine) - CRITICAL for A2 Evidence
        if quarantine_count > 0:
            print(f"🚨 ALERT: {quarantine_count} defects detected in Batch {batch_id}!")
            
            # Show the actual failed data in terminal (This is what you capture!)
            print(">>> QUARANTINE DATA PREVIEW (Reason Tagging):")
            df_quarantine.select("order_id", "shop_id", "rejection_reason", "created_at").show()
            
            # Save to Quarantine path for debugging/reprocessing
            s3_quarantine_path = "s3a://quarantine-layer/orders_failed"
            df_quarantine.write.format("parquet").mode("append").save(s3_quarantine_path)
            print(f"[FAIL] Saved {quarantine_count} records to Quarantine Layer.")
            
        else:
            # Even if 0, show structure once to prove the logic is running
            print(">>> QUARANTINE STATUS: No defects found in this batch.")

        print("="*20 + f" BATCH {batch_id} PROCESSING END " + "="*20 + "\n")
        

    def split_data(self, df):
        # 8대 결함 체크 및 사유 기록
        df_validated = df.withColumn("rejection_reason", 
            F.when(F.col("shop_id") == 999, "INVALID_SHOP_ID")
            .when(F.col("created_at") < F.current_timestamp() - F.expr("INTERVAL 1 HOUR"), "LATE_ARRIVAL")
            .otherwise(None)
        )
        
        # 정상(Silver)과 결함(Quarantine) 분리
        silver_df = df_validated.filter(F.col("rejection_reason").isNull())
        quarantine_df = df_validated.filter(F.col("rejection_reason").isNotNull())
        
        return silver_df, quarantine_df

    def run(self):
        """Starts the streaming pipeline."""
        KAFKA_SERVER = "kafka:29092".strip()
        TOPIC = "cdc.public.orders"

        raw_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_SERVER) \
            .option("subscribe", TOPIC) \
            .option("startingOffsets", "earliest") \
            .option("maxOffsetsPerTrigger", 1000) \
            .load()

        # [캡처 1-A] 
        # print("\n" + "!"*30 + " CAPTURE THIS: RAW SCHEMA " + "!"*30+"\n")
        # raw_df.printSchema()

        clean_df = self.transform(raw_df)

        # Use foreachBatch to execute the S3 save function
        query = clean_df.writeStream \
            .foreachBatch(self.save_to_s3) \
            .outputMode("update") \
            .option("checkpointLocation", "s3a://silver-layer/checkpoints/orders_v9") \
            .start()

        # [캡처 1-A] 
        # print("\n" + "!"*30 + " CAPTURE THIS: SILVER SCHEMA " + "!"*30+"\n")
        # clean_df.printSchema()

        query.awaitTermination()


def create_spark_session(app_name):
    """
    Initializes SparkSession and configures S3 (MinIO) settings 
    using environment variables.
    """
    # 1. Initialize SparkSession
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

    # 2. Set Log Level
    spark.sparkContext.setLogLevel("WARN")

    # 3. Apply Hadoop configurations for S3A
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    
    # Load from environment variables
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")
    endpoint = os.getenv("MINIO_ENDPOINT").replace("http://", "").replace("https://", "").strip("/")

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
    