import sys
import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf

@udf
def parse_age(age):
    return "[{}-{}]".format(int(age/10)*10, int(age/10)*10+9)

def perform_analysis_dataframe(df, output_path):
    result = df.withColumn("rangeage",parse_age(df["age"]))
    result.repartition(1).write.csv(output_path, mode="overwrite", header=True)

def  perform_analysis_sql(session, df, output_path):
    # Register the user-defined function into the session
    session.udf.register("range_age_func", parse_age)

    # Create a temporary view associated with the input data
    df.createOrReplaceTempView("users_table")
    
    result_2 = session.sql("SELECT name,surname, range_age_func(age) as rangeage  FROM users_table")
    result_2.repartition(1).write.csv(output_path, mode="overwrite", header=True)

    

if __name__ == "__main__":

    # Create an instance of spark
    spark = SparkSession.builder.appName('Exercise-49').getOrCreate()

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