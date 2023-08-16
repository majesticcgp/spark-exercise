from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Exercise-33")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    sensor_id = str(fields[0])
    date = str(fields[1])
    pm_10 = float(fields[2])
    return (sensor_id, pm_10)

if __name__ == "__main__":
    
    lines = sc.textFile("input.csv")
    temperatures_rdd = lines.map(parseLine)
    max_temp_for_each_sensor = temperatures_rdd.reduceByKey(lambda x,y: max(x,y))
    print(max_temp_for_each_sensor.collect())