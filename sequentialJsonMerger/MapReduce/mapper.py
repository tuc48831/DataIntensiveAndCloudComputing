#!/usr/bin/env python
import datetime
import json
import sys, os, re

# input comes from STDIN (standard input)???
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into jsons
    jsons = line.split('\n')
    #get the filename, split it to get the ID and timestamp
    filename = os.environ["map_input_file"]
    strings = re.split("_|\.", filename)
    #get the file name, split it to the ID and timestamp
    articleID = strings[0]
    timeStamp = strings[1]
    
    for jsonStr in jsons:
        #convert the timestamp into a datetime object
        newDateTime = datetime.datetime.strptime(timeStamp, "%Y%m%d%H%M%S")
        jsonObj = json.loads(jsonStr)
        #write the timestamp to the file so we can extract it in the reducer
        jsonObj.update({'scrapeTimestamp':str(newDateTime)})
        # write the results to STDOUT (standard output);
        # what we output here will be the input for the Reduce step, i.e. the input for reducer.py
        # tab-delimited; the trivial word count is 1
        print '%s\t%s' % (articleID, json.dumps(jsonObj))