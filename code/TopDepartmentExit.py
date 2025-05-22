from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

with SparkSession.builder.appName("TopDepartmentExit").getOrCreate() as spark:
    sc = spark.sparkContext

    df = spark.read.csv("hdfs:///user/hadoop/department_exit.csv", header=True, inferSchema=True)

    result = df.groupBy("Department") \
        .agg(
            count("*").alias("total_employees"),
            count(when(col("Left") == 1, True)).alias("employees_left")
        ) \
        .withColumn("attrition_rate", col("employees_left") / col("total_employees")) \
        .orderBy(col("attrition_rate").desc())

    output_data = result.rdd.map(lambda row: f"{row['Department']}: Attrition Rate = {row['attrition_rate']:.2%}")

    output_data.saveAsTextFile("hdfs:///user/hadoop/output_top_department_exit")