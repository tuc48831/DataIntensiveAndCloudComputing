import datetime
import json
import os, sys, re
from pprint import pprint

from jsonmerge import Merger

if __name__ == '__main__':
    
    
    
    print("hello world!!!")
    
    #this schema ensures that we merge the jsons' response array field on their id's
    schema = {
        "properties": {
            "response": {
                "mergeStrategy": "arrayMergeById"
                }
            }
        }
    merger = Merger(schema)
    #make a hashmap of <articleID's, jsonobjects>
    outputMap = {}
    changedFields = {}
    #for file in directory
    path = os.getcwd()+"/inputTest"
    files = os.listdir(path)
    for articleFile in files:
        strings = re.split("_|\.", articleFile)
        #get the file name, split it to the ID and timestamp
        articleID = strings[0]
        timeStamp = strings[1]
        print("timestamp is:" +timeStamp)
        #decode the timestamp into a real date so i can work with it
        newDateTime = datetime.datetime.strptime(timeStamp, "%Y%m%d%H%M%S")
        with open(path+"/"+articleFile, "r") as openFile:
        #get the lines of the file since it might have more than 1 json
            line = openFile.readline()
            while line:
                #try to parse the text to a json object
                jsonObj = json.loads(line)
                #put the timestamp in the object
                jsonObj.update({'scrapeTimestamp':str(newDateTime)})
                if articleID in outputMap:
                    #if the object already exists in the map
                    existingTimestamp = outputMap[articleID]['scrapeTimestamp']
                    existingTime = datetime.datetime.strptime(existingTimestamp, "%Y-%m-%d %H:%M:%S")
                    if existingTime > newDateTime:
                        #the already existing entry is newer, merge onto the old one with the schema
                        outputMap[articleID] = merger.merge(outputMap[articleID], jsonObj)
                    else:
                        #the new entry is newer, merge onto the new one with the schema
                        outputMap[articleID] = merger.merge(jsonObj, outputMap[articleID])
                #else just place it in
                else:
                    outputMap[articleID] = jsonObj
                #go onto next line
                line = openFile.readline()
        
    #after all of that we will have a hasmap with proper json objects
    #write out each object to a file with articleID as the name
    for key, value in outputMap.items():
        with open(os.getcwd()+'/outputTest/'+key+'.txt', 'w') as outFile:
            outFile.write(json.dumps(value))

