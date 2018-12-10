import os
import json
import re
import datetime
import pyspark.sql as sq
from __builtin__ import file

#call with /usr/local/spark/bin/spark-submit ~/git/DataIntensiveAndCloudComputing/sparkImpl/Main/testSpark.py 

# Configure the environment                                                     
if 'SPARK_HOME' not in os.environ:
    os.environ['SPARK_HOME'] = '/usr/local/spark'

spark = sq.SparkSession.builder.appName("merge_jsons").getOrCreate()

if __name__ == '__main__':
    path = "/home/cis5517/Documents/inputAndOutput/input/"
    files = os.listdir(path)
    jsons = []
    for jsonFile in files:
        strings = re.split("_|\.", jsonFile)
        #get the file name, split it to the ID and timestamp
        articleID = strings[0]
        timeStamp = strings[1]
        print("timestamp is:" +timeStamp)
        #decode the timestamp into a real date so i can work with it
        newDateTime = datetime.datetime.strptime(timeStamp, "%Y%m%d%H%M%S")
        
        fullpath = path + jsonFile
        data = spark.read.json(fullpath)
        data.update("scrapeTimestamp", str(newDateTime))
        jsons.append(data)
    
    #convert to dataframes   
    for item in jsons:
        df = spark.createDataFrame(item)
    
    print 'output!: ', str(union)
