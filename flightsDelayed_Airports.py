from pyspark import SparkConf
from pyspark import SparkContext
 
import csv
 
def extractData(record) :
 
    #split the input record by comma
    splits = record.split(',')
 
    #extract the actual/scheduled departure time and the origin
    actualDepTime = splits[4]
    scheduledDepTime = splits[5]
    origin = splits[16]
 
    #1 delayed
    #0 don't know or not delayed
    delayed = 0
    delay_By15min = 0
 
    # Check if the actual/scheduled departure time is a digit
    if actualDepTime.isdigit() & scheduledDepTime.isdigit():
 
        #if the flight got delayed or not
        if int(actualDepTime) > int(scheduledDepTime) :
	    delay_By15min = int(actualDepTime) - int(scheduledDepTime)
	    if delay_By15min > 15:    
                delayed = 1
 
    #return the origin and delayed status as a tuple
    return origin, delayed
 
#create the SparkConf() instance on which the different configurations can be done
#conf = SparkConf().setMaster("spark://master:7077")
 
#establish a connection to Spark
#sc = SparkContext(conf = conf, appName = "flightDataAnalysis")
sc = SparkContext()
 
#load the input data
lines = sc.textFile("hdfs://localhost:8020/user/training/flights/data/2015.csv")
 
#figure out the delayed flights and cache the data as it is being processed twice later
delayedFlights = lines.map(extractData).cache()
 
#get the delayed flights
delayedFlights.reduceByKey(lambda a, b : a + b).saveAsTextFile("hdfs://localhost:8020/user/training/flights/output/delayedFlights_Airports")
 
#get the total flights
totalFlights = delayedFlights.countByKey()
 
#totalFlights is dictionary. Iterate the same and write to a file
w = csv.writer(open("/home/training/Project_Flights/output/delayedFlights_Airports.csv", "w"))
for key, val in totalFlights.items():
    w.writerow([key, val])
