#!/usr/bin/env python
import datetime
import sys
import json
import re

from flatten_json import flatten
from jsondiff import diff

current_articleID = None
current_json = None

diffDict = {}

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    articleID, jsonValue = line.split('\t', 1)
    
    # convert count (currently a string) to int
    try:
        jsonObj = json.loads(jsonValue)
    except TypeError:
        # count was not a number, so silently
        # ignore/discard this line
        continue
    
    if not current_articleID:
        current_articleID = articleID
        current_json = jsonObj

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_articleID == articleID:
        #if the object already exists in the map
        existingTimestamp = current_json['scrapeTimestamp']
        existingTime = datetime.datetime.strptime(existingTimestamp, "%Y-%m-%d %H:%M:%S")
        newTimestamp = jsonObj['scrapeTimestamp']
        newTime = datetime.datetime.strptime(newTimestamp, "%Y-%m-%d %H:%M:%S")
        new_current_json = flatten(current_json)
        jsonObj = flatten(jsonObj)
        if existingTime > newTime:
            #the already existing entry is newer, merge onto the old one with the schema
            diffs = diff(new_current_json, jsonObj)
            #print '%s\t%s' % ('output', diffs)
        elif existingTime < newTime:
            #the new entry is newer, merge onto the new one with the schema
            diffs = diff(jsonObj, new_current_json)
            #print '%s\t%s' % ('output', diffs)
        else:
            diffs = {}
        for key, value in diffs.items():
            if key != 'delete':
                if key not in diffDict:
                    diffDict[key] = 1
                else:
                    diffDict[key] += 1
            else:
                if value not in diffDict:
                    diffDict['delete'+value] = 1
                else:
                    diffDict['delete'+value] += 1
    else:
        current_json = jsonObj
        current_articleID = articleID

# do not forget to output the last word if needed!
print '%s\t%s' % ('output', diffDict)