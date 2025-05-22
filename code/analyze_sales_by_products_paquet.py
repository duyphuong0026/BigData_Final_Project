from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

spark = SparkSession.builder \
    .appName("SalesAnalysisWithCustomOutputName") \
    .config("spark.sql.catalogImplementation", "hive") \
    .enableHiveSupport() \
    .getOrCreate()

sales_df = spark.sql("SELECT * FROM sale_db.sales")

sales_df.printSchema()

result_df = sales_df.groupBy("product_name") \
    .agg({"quantity": "sum", "price": "first"}) \
    .withColumnRenamed("sum(quantity)", "total_quantity") \
    .withColumnRenamed("first(price)", "unit_price") \
    .withColumn("total_revenue", col("total_quantity") * col("unit_price")) \
    .select("product_name", "total_revenue")

temp_output_path = "hdfs:///user/hadoop/output/sales_by_products_temp"
final_output_path = "hdfs:///user/hadoop/output/sales_by_products.parquet"

result_df.coalesce(1).write.mode("overwrite").parquet(temp_output_path)

spark.stop()

hdfs_move_cmd = f"hdfs dfs -mv {temp_output_path}/part-*.parquet {final_output_path}"

os.system(hdfs_move_cmd)

os.system(f"hdfs dfs -rm -r {temp_output_path}")