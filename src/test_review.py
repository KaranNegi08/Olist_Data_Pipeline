from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder.appName("Testing").getOrCreate()

df= spark.read \
        .option("header", "true") \
        .option("mode", "PERMISSIVE") \
        .option("columnNameOfCorruptRecord", "_corrupt_record") \
        .option("inferSchema","true") \
        .csv("./data_lake/bronze/olist/olist_order_reviews_dataset.csv")


df = df.dropDuplicates()
df = df.filter(col("review_score")==1).agg(count(col("review_score")))
df.show()