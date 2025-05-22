from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ReadParquetFromHDFS") \
    .getOrCreate()

parquet_path = "hdfs:///user/hadoop/output/sales_by_products.parquet"

df = spark.read.parquet(parquet_path)

df.show()

spark.stop()