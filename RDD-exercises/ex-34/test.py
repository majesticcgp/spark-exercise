from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Exercise-33")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    sensor_id = str(fields[0])
    date = str(fields[1])
    pm_10 = float(fields[2])
    return (sensor_id,(date,pm_10))

if __name__ == "__main__":
    '''
     If RDD data have two or more same keys, the top() method will sort
     the key first and then the value after.
    '''
    lines = sc.textFile("input.csv")
    temperatures_rdd = lines.map(parseLine)
    get_max_temperatures = temperatures_rdd.reduce(lambda x,y: x if x[1][1] > y[1][1] else y)[1][1]
    max_temp_lines = temperatures_rdd.filter(lambda x: x[1][1] == get_max_temperatures)
    
    print(max_temp_lines.collect())
