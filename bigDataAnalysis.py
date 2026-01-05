from pyspark.sql import SparkSession
import time

spark = SparkSession.builder \
    .appName("Big Data Analytics Scalability Demo") \
    .getOrCreate()

df = spark.read.csv(
    "sales_data.csv",
    header=True,
    inferSchema=True
)


print("Total Records:", df.count())


print("Initial Partitions:", df.rdd.getNumPartitions())


df = df.repartition(8)
print("Partitions After Repartition:", df.rdd.getNumPartitions())

high_sales = df.filter(df.sales > 50000)
print("High Sales Count:", high_sales.count())


region_sales = df.groupBy("region") \
                 .sum("sales") \
                 .withColumnRenamed("sum(sales)", "total_sales")
region_sales.show()

df.groupBy("product").avg("sales").show()

df.orderBy(df.sales.desc()).show(5)


start_time = time.time()
df.groupBy("region").sum("sales").count()
end_time = time.time()

print("Execution Time:", end_time - start_time, "seconds")

# 8. Stop Spark Session
spark.stop()
