from operator import add
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Exercise-33")
sc = SparkContext(conf = conf)

def split_data(line):
    """
    Split each line into columns
    """
    data = str.split(line, ',')
    sensor_id = data[0]
    temp = float(data[2])
    date = data[1] if filter_by_threshold(temp) else None
    return (sensor_id, date)

def filter_by_threshold(temp):
    """
    Just a filter
    """
    return temp > 50

def map_values_func(data_list):
    list_date_data = list(data_list)
    return [ x for x in list_date_data if x != None]
    



if __name__ == "__main__":

    lines = sc.textFile("input.csv")
    lines_above = lines.map(split_data)
    lines_above_by_key = lines_above.groupByKey().mapValues(map_values_func).collect()
    print(lines_above_by_key)