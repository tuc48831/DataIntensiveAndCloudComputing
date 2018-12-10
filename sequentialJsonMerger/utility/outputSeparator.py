#!/usr/bin/env python
import re
import os

cwd = os.getcwd()
path = cwd+"/mergeInput/part-00000"
#open file
realFile = open(path, "r")
#for line in file
for line in realFile:
    #split line by whitespace
    strings = re.split("\s", line, maxsplit=1)
    #get the file name, split it to the ID and timestamp
    #first is input filename
    #second is json
    filepath = strings[0]
    json = strings[1]
    #get article id from filename
    newStrings = re.split("\/", filepath)
    #last / is the article id
    newPath = cwd+"/mergeOutput/"
    newFile = open(newPath+newStrings[-1]+".json","w")
    #write json to new file
    newFile.write(json)