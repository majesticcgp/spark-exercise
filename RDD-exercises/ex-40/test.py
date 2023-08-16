from operator import add
import sys
import os
from pathlib import Path

from pyspark.sql import SparkSession


def split_data_and_add_counter(line):
    data = str.split(line, ',')
    return (data[0], 1)


if __name__ == "__main__":

    # Create an instance of spark
    spark = SparkSession.builder.appName('Exercise-32').getOrCreate()

    # Current path
    absolute_path = Path().absolute()

    # Input path
    input_path = os.path.join(absolute_path, 'input.csv')

    # Get a spark context
    sc = spark.sparkContext

    # Input data from CSV file
    lines = sc.textFile(input_path)

    # Get temperature greater than 50

    temperatures = lines.filter(lambda x: float(x.split(',')[2]) > 50)
    # Transform to new RDD format with count the number of day greater than 50
    transform_to_new_rdd = temperatures.map(split_data_and_add_counter)
    # Number of day greater than 2
    result = transform_to_new_rdd.reduceByKey(add).map(lambda x: (x[1],x[0]))
    
    print(result.collect())
    # Stop spark
    spark.stop()
