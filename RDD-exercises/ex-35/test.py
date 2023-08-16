import sys
import os
from pathlib import Path

from pyspark.sql import SparkSession

def get_temp_of_each_line(line):
    """
    Return the temperature of each line
    """
    data = str.split(line, ',')
    return data[2]

def get_date_of_each_line(line):
    """
    Return the date of each line
    """
    data = str.split(line, ',')
    return data[1]


if __name__ == "__main__":

    # Create an instance of spark
    spark = SparkSession.builder.appName('Exercise-34').getOrCreate()

    # Current path
    absolute_path = Path().absolute()

    # Input path
    input_path = os.path.join(absolute_path, 'input.csv')

    # Get a spark context
    sc = spark.sparkContext

    # Input data from CSV file
    lines = sc.textFile(input_path)

    # First get the maximum temperature
    max_temp = lines.map(get_temp_of_each_line).top(1)[0]
    # Filter lines containing the previous temperature
    max_temp_lines = lines.filter(lambda x: get_temp_of_each_line(x) == max_temp)
    
    # Second get the date of the maximum temperature
    date_of_maximum_temperature = max_temp_lines.map(get_date_of_each_line).collect()

    # Print the result on the standard output
    print(date_of_maximum_temperature)
    
    # Stop spark
    spark.stop()