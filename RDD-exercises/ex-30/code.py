import sys
import os
from pathlib import Path

from pyspark.sql import SparkSession


if __name__ == "__main__":

    # Create an instance of spark
    spark = SparkSession.builder.appName('Exercise-30').getOrCreate()

    # Current path
    absolute_path = Path().absolute()

    # Input path
    input_path = os.path.join(absolute_path, 'input.txt')

    # Output path
    output_path = os.path.join(absolute_path, 'output')

    # Input data 
    '''
    This line below will transform to a DataFrame
    spark.read.text(input_path) ==> DataFrame
    To transform DataFrame to RDD
    ==> df.rdd
    '''
    lines = spark.read.text(input_path).rdd.map(lambda x: x[0])

    # Filter out data not containing the work 'google'
    lines_w_google = lines.filter(lambda x: 'google' in x)

    # Save data on the output file
    '''
    The coalesce(1) code in PySpark reduces the number of partitions in an RDD to 1.
    This can be useful for operations that do not need to be performed
    on all of the partitions in an RDD, such as aggregations.
    
    '''
    lines_w_google.coalesce(1).saveAsTextFile(output_path)
    
    # Stop spark
    spark.stop()
