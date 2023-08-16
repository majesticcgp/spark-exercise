from operator import add
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Exercise-33")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    sensor_id = str(fields[0])
    date = str(fields[1])
    pm_10 = float(fields[2])
    return (sensor_id, [date])


def split_data(line):
    """
    Split each line into columns
    """
    data = str.split(line, ',')
    return (data[0], data[1])

def filter_by_threshold(line):
    """
    Just a filter
    """
    temp = float(str.split(line, ',')[2])
    return temp > 50



if __name__ == "__main__":

    lines = sc.textFile("input.csv")
    
    # Filter out all readings below threshold 50 and return (sensorId, date)
    lines_above = lines.filter(filter_by_threshold).map(split_data)

    # Map each reading with a counter
    lines_above_by_key = lines_above.groupByKey().mapValues(list).collect()

    # Print the result on the standard output
    print(lines_above_by_key)
    