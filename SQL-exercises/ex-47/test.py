from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pathlib import Path
import os

output_sql_path = os.path.join(Path().absolute(), 'result_sql')

spark = SparkSession.builder.appName("Exercise-47").getOrCreate()

df = spark.read.option("header", "true").option("inferSchema", "true").csv("input.csv")

df.createOrReplaceTempView("user_table")

filer_db = spark.sql("SELECT * FROM user_table WHERE gender = 'male'")

result =filer_db.select("name","age").withColumn("age", df.age +1)

result.write.csv(output_sql_path,mode='overwrite')