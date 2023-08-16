from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pathlib import Path
import os
from pyspark.sql import SparkSession

def perform_analysis_dataframe(df, output_df_path):
    pass
    df_profile = df.groupBy("name").agg({'age':'avg','*':'count'}) \
    .withColumnRenamed("avg(age)","age") \
    .withColumnRenamed("count(1)","counter") \
    
    result_df = df_profile.filter(df_profile.counter >=2)
    result_df = result_df.withColumn("age",result_df["age"].cast(IntegerType()))
    result_df.repartition(1).write.csv(output_df_path, mode='overwrite')
    
def perform_analysis_sql(session,df, output_sql_path):
    df.createOrReplaceTempView("user_table")
    result_sql = spark.sql("SELECT name, cast(avg(age) as int) as age, count(*) as counter FROM user_table GROUP BY name HAVING count(1) >= 2")
    session.catalog.dropTempView("user_table")
    result_sql.repartition(1).write.csv(output_sql_path, mode='overwrite')
    
if __name__ == "__main__":

    # Create an instance of spark
    spark = SparkSession.builder.appName('Exercise-48').getOrCreate()

    # Current path
    absolute_path = Path().absolute()

    # Input path
    input_path = os.path.join(absolute_path, 'input.csv')

    # Output paths
    output_df_path = os.path.join(absolute_path, 'result_dataframe_mode')
    output_sql_path = os.path.join(absolute_path, 'result_sql_mode')

    # Load input data into a dataframe
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    # Perform the analysis by using the dataframe
    perform_analysis_dataframe(df, output_df_path)

    # Perform the analysis by using SQL queries
    perform_analysis_sql(spark, df, output_sql_path)

    # Stop Spark execution
    spark.stop()