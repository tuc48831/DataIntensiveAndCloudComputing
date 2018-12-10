import os
import json
import re
import datetime
import pyspark.sql as sq
from __builtin__ import file
from pyspark.sql.functions import input_file_name

#call with /usr/local/spark/bin/spark-submit ~/git/DataIntensiveAndCloudComputing/sparkImpl/Main/testSpark.py 

# Configure the environment                                                     
if 'SPARK_HOME' not in os.environ:
    os.environ['SPARK_HOME'] = '/usr/local/spark'

spark = sq.SparkSession.builder.appName("merge_jsons").getOrCreate()

if __name__ == '__main__':
    path = "/home/cis5517/Documents/inputAndOutput/input/"
    files = os.listdir(path)
    data = spark.read.json(path)
    
    
    data = data.withColumn('filePath', input_file_name())
    
    data.show()
    #get the fileName column and expand it out into article ID
    #example filename file:///home/cis5517/Documents/inputAndOutput/input/29654_20181116114436.json
    split_col = sq.functions.split(data['filePath'], '/input/')
    data = data.withColumn('fileName', split_col.getItem(1))
    
    split_col = sq.functions.split(data['fileName'], '_')
    data = data.withColumn('articleId', split_col.getItem(0))
    #then merge on articleID
    data.show()
    #for each article ID
    #newData = data.groupby('articleId').agg(sq.functions.collect_set('response')).collect()
    
    