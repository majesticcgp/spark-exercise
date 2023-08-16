from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Exercise-32")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    sensor_id = str(fields[0])
    date = str(fields[1])
    pm_10 = float(fields[2])
    return (sensor_id, pm_10)

if __name__ == "__main__":
    '''
     The values in an RDD can be of any type,
     such as strings, integers, floats, or complex objects.
     It can be a pairRDD like a tuple
    '''
    lines = sc.textFile("input.csv")
    temperatures_rdd = lines.map(parseLine)
    '''
    The `reduce` method applies the `func` function to
    the elements of the RDD in a cumulative way.
    It starts by applying the function to the first two elements,
    then takes the result and applies the function to it and
    the third element, and so on,
    until all the elements have been processed.
    The final result is a single value.
    
    
    The `func` argument is a function that takes two arguments of the same type as the elements
    in the RDD and returns a single value of the same type.
    For example, if you have an RDD of integers, the `func` would be
    a function that takes two integers as input and returns
    another integer.
    '''
    result = temperatures_rdd.reduce(lambda x,y: x if x[1] > y[1] else y)
    getMaxTemp =  result[1]
    print(getMaxTemp)

    
    
