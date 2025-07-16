# /spark-apps/process_shows.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType

def main():
    """
    Main function to run the Spark processing job.
    This version correctly handles multi-line JSON files.
    """
    spark = SparkSession.builder \
        .appName("TVShowProcessing") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    print("Spark Session created successfully with S3 config.")

    show_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("type", StringType(), True),
        StructField("language", StringType(), True),
        StructField("genres", ArrayType(StringType()), True),
        StructField("status", StringType(), True),
        StructField("premiered", StringType(), True),
        StructField("averageRuntime", IntegerType(), True),
        StructField("rating", StructType([
            StructField("average", DoubleType(), True)
        ]), True),
        StructField("network", StructType([
            StructField("name", StringType(), True)
        ]), True),
    ])

    input_path = "/opt/spark-data/raw/tv_shows_*.json"
    output_path = "s3a://datalake/processed/shows_clean"

    # --- FINAL FIX ---
    # Add the multiLine=true option to correctly read the array-based JSON file.
    df_raw = spark.read.option("multiLine", "true").schema(show_schema).json(input_path)
    
    print("--- Raw DataFrame from JSON ---")
    df_raw.printSchema()
    df_raw.show(5, truncate=False)

    df_transformed = df_raw.select(
        col("id"),
        col("name"),
        col("type"),
        col("language"),
        col("genres"),
        col("status"),
        col("premiered"),
        col("averageRuntime"),
        col("rating.average").alias("rating"),
        col("network.name").alias("network_name")
    ).na.fill(0, subset=["rating"])

    print("\n--- Transformed DataFrame ---")
    df_transformed.printSchema()
    df_transformed.show(5, truncate=False)

    print(f"\nWriting cleaned data to {output_path}")
    df_transformed.write.mode("overwrite").parquet(output_path)

    print("Spark job completed successfully!")

    spark.stop()

if __name__ == "__main__":
    main()
