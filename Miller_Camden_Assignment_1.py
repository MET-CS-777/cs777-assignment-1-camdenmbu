# Camden Miller
# Assignment 1
# 13 September 2024
# CS777 - Big Data

# Read in functions from template
from __future__ import print_function
import os
import sys
import requests
from operator import add
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import functions as func
from pyspark.sql.functions import *


#Exception Handling and removing wrong datalines
def isfloat(value):
    try:
        float(value)
        return True
 
    except:
         return False

#Function - Cleaning
def correctRows(p):
    if(len(p)==17):
        if(isfloat(p[5]) and isfloat(p[11])):
            if(float(p[4])> 60 and float(p[5])>0 and float(p[11])> 0 and float(p[16])> 0):
                return p

#Main
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: main_task1 <file> <output> ", file=sys.stderr)
        exit(-1)
    
    sc = SparkContext(appName="Assignment-1")
    
    rdd = sc.textFile(sys.argv[1])

# Task 1 - Top 10 Taxis (based on number of unique drivers)

    clean_rdd_medallion = rdd.map(lambda line: line.split(',')) \
        .filter(lambda p: correctRows(p)) \
        .map(lambda fields: (fields[0],fields[1])) \
        .distinct() \
        .groupByKey() \
        .mapValues(lambda drivers: len(set(drivers)))
    
    top_medallions = clean_rdd_medallion.sortBy(lambda x: -x[1]).take(10)
    
    sc.parallelize(top_medallions).coalesce(1).saveAsTextFile(sys.argv[2])

#Task 2 - Top Earners (dollars per minute)
    
    clean_rdd_payments = rdd.map(lambda line: line.split(',')) \
        .filter(lambda p: correctRows(p)) \
        .map(lambda fields: (fields[1],
                             (float(fields[4])/60.0,
                             float(fields[16])))) \
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
        
    top_earners = clean_rdd_payments.mapValues(lambda x: x[1] / x[0]) \
        .sortBy(lambda x: -x[1]) \
        .take(10)
        
    sc.parallelize(top_earners).coalesce(1).saveAsTextFile(sys.argv[3])

    sc.stop()