from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, to_date, date_format
import pandas as pd

# Initialize Spark Session
spark = SparkSession.builder.appName("BigDataTask1").getOrCreate()

# Load dataset
file_path = r"C:\Users\HP\transactions_results.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

print("✅ Dataset Loaded Successfully")
df.printSchema()
df.show(5)

# -----------------------------
# Scalability Info
# -----------------------------
print("\n⚡ Scalability Info:")
print(f"Total Rows: {df.count()}")
print(f"Number of Partitions: {df.rdd.getNumPartitions()}")

# -----------------------------
# Summary Stats
# -----------------------------
summary = df.agg(
    count("*").alias("total_transactions"),
    sum("TransactionAmount").alias("total_amount"),
    avg("TransactionAmount").alias("avg_amount")
)
summary.show()

# -----------------------------
# Top Customers by Spending
# -----------------------------
top_accounts = df.groupBy("AccountID").agg(
    count("*").alias("n_transactions"),
    sum("TransactionAmount").alias("total_spent")
).orderBy(col("total_spent").desc())
top_accounts.show(10)

# -----------------------------
# Transactions by Type
# -----------------------------
by_type = df.groupBy("TransactionType").agg(
    count("*").alias("n_transactions"),
    sum("TransactionAmount").alias("total_amount"),
    avg("TransactionAmount").alias("avg_amount")
)
by_type.show()

# -----------------------------
# Monthly Trends
# -----------------------------
monthly_trends = df.withColumn("month", date_format(col("TransactionDate"), "yyyy-MM")) \
    .groupBy("month").agg(
        count("*").alias("n_transactions"),
        sum("TransactionAmount").alias("total_amount")
    ).orderBy("month")
monthly_trends.show(12)

# -----------------------------
# Save results as CSV (via Pandas, no Hadoop issues!)
# -----------------------------
output_dir = r"C:\Users\HP\output"

summary.toPandas().to_csv(f"{output_dir}\\summary.csv", index=False)
top_accounts.toPandas().to_csv(f"{output_dir}\\top_accounts.csv", index=False)
by_type.toPandas().to_csv(f"{output_dir}\\by_type.csv", index=False)
monthly_trends.toPandas().to_csv(f"{output_dir}\\monthly_trends.csv", index=False)

print(f"\n🎯 Task 1 completed successfully! CSVs saved to {output_dir}")
