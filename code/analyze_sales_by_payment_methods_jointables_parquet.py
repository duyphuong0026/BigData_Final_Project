from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

spark = SparkSession.builder \
    .appName("SalesAnalysisByPaymentMethod") \
    .config("spark.sql.catalogImplementation", "hive") \
    .enableHiveSupport() \
    .getOrCreate()

query = """
SELECT *
FROM sale_db.sales s
INNER JOIN sale_db.payment_methods p
ON s.payment_method = p.payment_method_id
"""
joined_df = spark.sql(query)

sales_with_revenue_df = joined_df.withColumn("total_revenue", col("quantity") * col("price"))

result_df = sales_with_revenue_df.groupBy("method_name") \
    .agg({"total_revenue": "sum"}) \
    .withColumnRenamed("sum(total_revenue)", "total_revenue_by_method") \
    .select("method_name", "total_revenue_by_method")

temp_output_path = "hdfs:///user/hadoop/output/sales_by_payment_temp"
final_output_path = "hdfs:///user/hadoop/output/sales_by_payment.parquet"

result_df.coalesce(1).write.mode("overwrite").parquet(temp_output_path)

spark.stop()

os.system(f"hdfs dfs -mv {temp_output_path}/part-*.parquet {final_output_path}")

os.system(f"hdfs dfs -rm -r {temp_output_path}")