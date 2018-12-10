#!/usr/bin/env python
import re
import os
import json
from _collections import defaultdict
from ast import literal_eval

cwd = os.getcwd()
path = cwd+"/histoInput/part-00000.txt"
#open file
histoFile = open(path, "r")
#for line in file
for line in histoFile:
    #split line by whitespace
    strings = re.split("[{|}]", line)
    #get the file name, split it to the ID and timestamp
    #first is input filename
    #second is json
    header = strings[0]
    print (header)
    info = strings[1]
    print( info )
    oldDict = literal_eval(info)
    newDict = defaultdict(int)
    for key, value in oldDict.items():
        #combine fields
        newKey = re.sub("_\d+_", "_", key)
        newDict[newKey] += value
    print( str(newDict))