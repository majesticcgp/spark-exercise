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
    
    
    If the code like this
    result = temperatures_rdd.reduce(lambda x,y: x if x > y else y)
    * It'll compare for each line of the RDD
    * First of it'll will compare first element in single line of the RDD if it equals then compare the second element and so on
    
    
    If the code like this
    result = temperatures_rdd.reduce(lambda x,y: x + y)
     * result  = ('s1', 20.5, 's2', 30.1, 's1', 60.2, 's2', 20.4, 's1', 55.5, 's2', 52.5)
     
    If the code like this
    result = temperatures_rdd.reduce(lambda x,y: x[1] + y[1]) ==> ERROR b/c the result format not match the constructed of RDD (sensor_id, pm_10)
    * We must CREATE NEW "FORMATE" construct a new tuple to hold the result
    ==> result = temperatures_rdd.reduce(lambda x, y: (x[0], x[1] + y[1]))
    *It'll take the first ID element to present the id                                                        
    
    
    
    
    The reduce() method it will not change the FORMAT of the RDD it will aggregate
    the things that we want to calculate
    
    In this ex below the result after we use reduce() method like this
    
    (sensor_id, pm_10) ==> ('s1', 60.2)
    '''
    result = temperatures_rdd.reduce(lambda x,y: x if x[1] > y[1] else y)
    getMaxTemp =  result[1]
    print(result)

    
    
