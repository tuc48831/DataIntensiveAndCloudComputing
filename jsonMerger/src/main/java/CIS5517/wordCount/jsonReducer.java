package CIS5517.wordCount;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.json.*;

public class jsonReducer extends Reducer<Text,Text,Text,Text> {
//TODO update with proper merging, logging, and output
	private Logger logger = Logger.getLogger(jsonReducer.class);
	private Text result = new Text();
	private Text returnJson = new Text();
	
	public JSONObject jsonHolder;
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException, JSONException{
	    String articleID = key.toString();
		for (Text text : values) {
	    	//convert each value to a json
	    	try {
				JSONObject tempJson = new JSONObject(text.toString());
				//if we currently dont have any return json for this reducer, set it to the json we just decoded
				if(jsonHolder == null ) {
					jsonHolder = tempJson;
					returnJson.set(tempJson.toString());
				//else we should compare them, logging any diffs, and updating
				} else {
					JSONObject temp = logDiffsAndUpdate(jsonHolder, tempJson, context, articleID);
					jsonHolder = temp;
					returnJson.set(temp.toString());
				}
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				logger.debug("The failed json conversion was: " + text.toString());
				e.printStackTrace();
			}
    	}
	    result.set(returnJson.toString());
	    context.write(key, returnJson);
	}
	
	private JSONObject logDiffsAndUpdate(JSONObject currentJson, JSONObject tempJson, Context context, String articleID) {
		//get timestamps from both objects
		int indicator = 1;
		try {
	    	//a date formatter according to the header from the file (20181015141058 for oct 15th 2018 2:10:58 pm)
	    	SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
    		//convert it to a java date so i dont need to implement my own date comparator
	    	Date currentJsonDate = formatter.parse(currentJson.getString("webscrapeTimeStamp"));
	    	Date tempJsonDate = formatter.parse(tempJson.getString("webscrapeTimeStamp"));
	    	//replace returnJson with tempJson if temp is newer, else return the currentJson
			if(!tempJsonDate.before(currentJsonDate) && !tempJsonDate.after(currentJsonDate)) {
				indicator = -1;
			}
	    	if(tempJsonDate.after(currentJsonDate)) {
				indicator = 2;
			}else {
				indicator = 1;						
			}
			JSONObject returnJson = logDiffsAndMerge(currentJson, tempJson, indicator, currentJsonDate, tempJsonDate, articleID);
			return returnJson;
		} catch (ParseException e) {
			logger.debug("currentjson's timestamp is:" + currentJson.getString("webscrapeTimeStamp"));
			e.printStackTrace();
		}
		return null;
	}

	private JSONObject logDiffsAndMerge(JSONObject currentJson, JSONObject tempJson, int indicator, Date currentJsonDate, Date tempJsonDate, String articleID) {
		JSONObject returnJson = new JSONObject();
		//the 3 fields are, cursor, code, and responses. I will hardcode what I want to happen
		boolean cursorIsDifferent = currentJson.get("cursor").equals(tempJson.get("cursor"));
		boolean codeIsDifferent = currentJson.get("code").equals(tempJson.get("code"));
		boolean responseIsDifferent = currentJson.get("response").equals(tempJson.get("response"));
		//use indicator to see which object is newer, and use that as the base case
		if(indicator == -1) {
			logger.debug("the json's have the same timestamp");
		}else if(indicator == 2) { //if indicator is 2, them tempjson is newer
			if(cursorIsDifferent) {
				logger.debug("The \'cursor\' field is different, the old timestamp is: " + currentJsonDate.toString() + " and the new timestamp is: " + tempJsonDate.toString());
				returnJson.put("cursor", tempJson.get("cursor").toString());
				logger.info("field updated, HISTOGRAM:cursor");
			}
			if(codeIsDifferent) {
				logger.debug("The \'code\' field is different, the old timestamp is: " + currentJsonDate.toString() + " and the new timestamp is: " + tempJsonDate.toString());
				returnJson.put("code", tempJson.get("code").toString());
				logger.info("field updated, HISTOGRAM:code");
			}
			if(responseIsDifferent) {
				logger.debug("The \'response\' field is different, the old timestamp is: " + currentJsonDate.toString() + " and the new timestamp is: " + tempJsonDate.toString());
				logger.info("field updated, HISTOGRAM:response");
			}
		} else { //else tempjson is older
			if(cursorIsDifferent) {
				logger.debug("The \'cursor\' field is different, the old timestamp is: " + tempJsonDate.toString() + " and the new timestamp is: " + currentJsonDate.toString());
				returnJson.put("cursor", currentJson.get("cursor").toString());
				logger.info("field updated, HISTOGRAM:cursor");
			}
			if(codeIsDifferent) {
				logger.debug("The \'code\' field is different, the old timestamp is: " + tempJsonDate.toString() + " and the new timestamp is: " + currentJsonDate.toString());
				returnJson.put("code", currentJson.get("code").toString());
				logger.info("field updated, HISTOGRAM:code");
			}
			if(responseIsDifferent) {
				logger.debug("The \'response\' field is different, the old timestamp is: " + tempJsonDate.toString() + " and the new timestamp is: " + currentJsonDate.toString());
				logger.info("field updated, HISTOGRAM:response");
			}
		}
		//merge here outside the if else because it happens regardless of conditions
		returnJson.put("response", mergeResponses(currentJson, tempJson, indicator, articleID));

		return returnJson;
	}
	
	private Collection<JSONObject> mergeResponses(JSONObject currentJson, JSONObject tempJson, int indicator, String articleID) {
		JSONArray currentArray = currentJson.getJSONArray("response");
		JSONArray tempArray = tempJson.getJSONArray("response");
		HashMap<String, JSONObject> responseSet = new HashMap<String, JSONObject>();
		//inside this function i swap temp and current based on the indicator. indicator = 2 means that temp is newer (the default case), 1 means i need to swap
		if(indicator == 1) { //since the temp is older
			JSONArray temp = tempArray;
			tempArray = currentArray;
			currentArray = temp;
		} //this is gross but saves A LOT of duplicate code below
		//iterate through older array first, adding them to a hashmap on the ID field
		for(int i=0; i < currentArray.length(); i++) {
			responseSet.put(currentArray.getJSONObject(i).getString("id"), currentArray.getJSONObject(i));
			//also check if the id from the current array is missing from the new one and log it
			boolean matched = false;
			//this is gross because the double for loop makes it look n^2 but the number of responses per article doesn't ever really get too big
			for(int j=0; j < tempArray.length(); j++) {
				if(currentArray.get(i).equals(tempArray.get(j))) {
					matched = true;
				}
			}
			if(! matched) {
				logger.debug("For the article with ID: " + articleID + " the MISSING response node id:"+ currentArray.getJSONObject(i).getString("id") + 
						". was in Ji but not in Ji+1");
			}
		}
		//iterate through newer array second, if there is a collision, check if theyre different. mark whats different and then replace
		for(int i=0; i < tempArray.length(); i++) {
			if(responseSet.containsKey(tempArray.getJSONObject(i).getString("id"))) {
				//if it contains it, check if thing currently in the set with the id is equal to the new thing with the id
				if( ! responseSet.get(tempArray.getJSONObject(i).getString("id")).equals(tempArray.getJSONObject(i).getString("id"))) {
					//log the diffs and replace it with the newer verison
					JSONObject newObj = responseMergeAndlogDiffs(responseSet.get(tempArray.getJSONObject(i).getString("id")), tempArray.getJSONObject(i), articleID);
					responseSet.replace(tempArray.getJSONObject(i).getString("id"), newObj);
				} //else they're equal and I don't need to do anything
			} else {
				//it didnt contain it, so blindly add it and log that it was added
				responseSet.put(tempArray.getJSONObject(i).getString("id"), tempArray.getJSONObject(i));
				logger.debug("For the article with ID: " + articleID + " the MISSING response node id: "+ tempArray.getJSONObject(i).getString("id") + 
						". was in Ji+1 but not in Ji");
			}
		}
		//convert hashmap into array so its a collection	
		Collection<JSONObject> returnArray = responseSet.values();
		return returnArray;
	}

	private JSONObject responseMergeAndlogDiffs(JSONObject olderObject, JSONObject newerObject, String articleID) {
		JSONObject returnObject = new JSONObject();
		//the input to this function is the JSONObject of the "response", so we will get all the keys and values and compare them
		//not going to recursively get information about the author, it seems inconsequential like avatar, location, etc.
		for(String key: olderObject.keySet()) {
			if(! newerObject.has(key)) {
				logger.debug("For the article with ID: " + articleID + " the response node id: "+ olderObject.getString("id") + 
						" had the field: " + key + " in Ji ("+olderObject.getString(key)+") but not in Ji+1"); 
			}
			//blindly add all the older object stuff, and then it will get overwritten with the new stuff, or kept if the new one doesn't have that key
			returnObject.put(key, olderObject.get(key));
		}
		for(String key: newerObject.keySet()) {
			if(! olderObject.has(key)) {
				logger.debug("For the article with ID: " + articleID + " the response node id: "+ newerObject.getString("id") + 
						" had the field: " + key + " in Ji+1 ("+newerObject.getString(key)+") but not in Ji"); 
				
			}
			if(returnObject.has(key) && !returnObject.get(key).equals(newerObject.get(key))) {
				logger.debug("For the article with ID: " + articleID + " the response node id: "+ newerObject.getString("id") + 
						" had the field: " + key + " in Ji ("+olderObject.getString(key)+") and in ji+1 ("+newerObject.getString(key)+")");
				logger.info("field updated, HISTOGRAM:"+key);
			}
			returnObject.put(key, newerObject.get(key));
		}
		return returnObject;
	}
	
}