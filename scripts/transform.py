from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize Spark
spark = SparkSession.builder \
    .appName("Data_Transformation") \
    .getOrCreate()

# 1. Read ALL Parquet files from GCS
df = spark.read.parquet("gs://emergency-admission-data/raw/*.parquet")

# 2. Comprehensive Type Casting for All Columns
# This ensures BigQuery receives a perfectly structured schema
silver_df = df.select(
    F.to_date(F.col("date"), "yyyy-MM-dd").alias("admission_date"),
    F.col("ed_type").cast("string"),
    F.col("age_group").cast("string"),
    F.col("syndrome").cast("string"),
    F.col("relative_cases").cast("float"),
    F.col("relative_cases_7day_ma").cast("float"),
    F.col("expected_value").cast("float"),
    F.col("expected_lowerbound").cast("float"),
    F.col("expected_upperbound").cast("float"),
    F.col("ed_count").cast("integer")
).filter(F.col("admission_date").isNotNull())

# 3. Write to the Staging Table
# Use 'overwrite' so that every time the job runs, it refreshes the staging area
silver_df.write \
    .format("bigquery") \
    .option("table", "emergency-admission-492214.emergency_admission_data.stg_admissions") \
    .option("temporaryGcsBucket", "emergency-admission-data") \
    .mode("overwrite") \
    .save()

spark.stop()