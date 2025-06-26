from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

# 1. Create a Spark session
spark = SparkSession.builder \
    .appName("SLM ETL Pipeline") \
    .getOrCreate()

# 2. Read the raw CSV
df = spark.read.csv("data/raw_data.csv", header=True, inferSchema=True)

print("✅ Loaded Raw Data")
df.printSchema()
print(f"Total rows: {df.count()}")

# 3. Data Cleaning
df_clean = df.dropna(how="any")       # Drop rows with nulls
df_clean = df_clean.dropDuplicates()  # Remove duplicate rows

# 4. Feature Engineering (cast 'budget' to double safely)
if "budget" in df_clean.columns:
    df_clean = df_clean.withColumn("budget", expr("try_cast(budget as double)"))

# 5. Save processed data
df_clean.coalesce(1).write.csv("data/processed_data", header=True, mode="overwrite")


print("✅ ETL Completed: Cleaned data saved to 'data/processed_data'")
