#!/usr/bin/env python
import datetime
import sys
import json

from jsonmerge import Merger

current_articleID = None
current_json = None

#this schema ensures that we merge the jsons' response array field on their id's
schema = {
    "properties": {
        "response": {
            "mergeStrategy": "arrayMergeById"
            }
        }
    }
merger = Merger(schema)

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
        if existingTime > newTime:
            #the already existing entry is newer, merge onto the old one with the schema
            current_json = merger.merge(current_json, jsonObj)
        elif existingTime < newTime:
            #the new entry is newer, merge onto the new one with the schema
            current_json = merger.merge(jsonObj, current_json)
    else:
        if current_json:
            # write result to STDOUT
            print '%s\t%s' % (current_articleID, current_json)
        current_json = jsonObj
        current_articleID = articleID

# do not forget to output the last word if needed!
if current_articleID == articleID:
    print '%s\t%s' % (current_articleID, current_json)
