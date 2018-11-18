package CIS5517.wordCount;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
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
	private JSONObject returnJson;
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException, JSONException{
	    for (Text text : values) {
	    	//convert each value to a json
	    	try {
				JSONObject tempJson = new JSONObject(text.toString());
				//if we currently dont have any return json for this reducer, set it to the json we just decoded
				if(returnJson == null ) {
					returnJson = tempJson;
				//else we should compare them, logging any diffs, and updating
				} else {
					returnJson = logDiffsAndUpdate(returnJson, tempJson, context);
				}
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
	    result.set(returnJson.toString());
	    context.write(key, result);
	}
	
	private JSONObject logDiffsAndUpdate(JSONObject currentJson, JSONObject tempJson, Context context) {
		//get timestamps from both objects
		Date currentJsonDate = null;
		Date tempJsonDate = null;
		try {
	    	//a date formatter according to the header from the file (20181015141058 for oct 15th 2018 2:10:58 pm)
	    	SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
    		//convert it to a java date so i dont need to implement my own date comparator
	    	currentJsonDate = formatter.parse(currentJson.getString("timeStamp"));
	    	tempJsonDate = formatter.parse(tempJson.getString("timeStamp"));
	    	//remove the timestamp field i added to preserve data integrity
	    	currentJson.remove("timeStamp");
	    	tempJson.remove("timeStamp");
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		//indicator tells which JSON object is the newer one so it can be passed to the logDiffs fx
		int indicator = 0;
		//replace returnJson with tempJson if temp is newer, else return the currentJson
		if(tempJsonDate.after(currentJsonDate)) {
			indicator = 2;
		}else {
			indicator = 1;
		}
		JSONObject returnJson = logDiffsAndMerge(currentJson, tempJson, indicator, currentJsonDate, tempJsonDate);
		
		return returnJson;
	}

	private JSONObject logDiffsAndMerge(JSONObject currentJson, JSONObject tempJson, int indicator, Date currentJsonDate, Date tempJsonDate) {
		JSONObject returnJson = new JSONObject();
		//the 3 fields are, cursor, code, and responses. I will hardcode what I want to happen
		boolean cursorIsDifferent = currentJson.getString("cursor").equals(tempJson.getString("cursor"));
		boolean codeIsDifferent = currentJson.getString("code").equals(tempJson.getString("code"));
		boolean responseIsDifferent = currentJson.getString("response").equals(tempJson.getString("response"));
		//use indicator to see which object is newer, and use that as the base case
		if(indicator == 2) { //if indicator is 2, them tempjson is newer
			if(cursorIsDifferent) {
				logger.info("The \'cursor\' field is different, the old timestamp is: " + currentJsonDate.toString() + " and the new timestamp is: " + tempJsonDate.toString());
				returnJson.put("cursor", tempJson.getString("cursor"));
			}
			if(codeIsDifferent) {
				logger.info("The \'code\' field is different, the old timestamp is: " + currentJsonDate.toString() + " and the new timestamp is: " + tempJsonDate.toString());
				returnJson.put("code", tempJson.getString("code"));
			}
			if(responseIsDifferent) {
				logger.info("The \'response\' field is different, the old timestamp is: " + currentJsonDate.toString() + " and the new timestamp is: " + tempJsonDate.toString());
			}
		} else { //else tempjson is older
			if(cursorIsDifferent) {
				logger.info("The \'cursor\' field is different, the old timestamp is: " + tempJsonDate.toString() + " and the new timestamp is: " + currentJsonDate.toString());
				returnJson.put("cursor", currentJson.getString("cursor"));
			}
			if(codeIsDifferent) {
				logger.info("The \'code\' field is different, the old timestamp is: " + tempJsonDate.toString() + " and the new timestamp is: " + currentJsonDate.toString());
				returnJson.put("code", currentJson.getString("code"));
			}
			if(responseIsDifferent) {
				logger.info("The \'response\' field is different, the old timestamp is: " + tempJsonDate.toString() + " and the new timestamp is: " + currentJsonDate.toString());
			}
		}
		//merge here outside the if else because it happens regardless of conditions
		returnJson.put("response", mergeResponses(currentJson, tempJson, indicator));

		return returnJson;
	}
	
	private JSONObject[] mergeResponses(JSONObject currentJson, JSONObject tempJson, int indicator) {
		JSONArray currentArray = currentJson.getJSONArray("responses");
		JSONArray tempArray = tempJson.getJSONArray("responses");
		//
		if(indicator == 2) { //if indicator is 2, them tempjson is newer
			//iterate through older array first, adding them to a hashmap
			//iterate through newer array second, if there is a collision, check if theyre different. mark whats different and then replace
		} else { //else tempjson is older
			//iterate through older array first, adding them to a hashmap
			//iterate through newer array second, if there is a collision, check if theyre different. mark whats different and then replace
		}
		//convert hashmap into array so its a collection
		
		return null;
	}
	
}