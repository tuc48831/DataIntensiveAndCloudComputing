package CIS5517.wordCount;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.json.*;

public class jsonReducer extends Reducer<Text,Text,Text,Text> {
//TODO update with proper merging, logging, and output
	private Text result = new Text();
	private JSONObject returnJson;
	private MultipleOutputs mos;
	//the article ID used to generate the output file name
	private String articleID;
  
	public void setup(JobConf context) {
		mos = new MultipleOutputs(context);
	}
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException, JSONException{
	    String jsonAsString;
	    for (Text text : values) {
	    	//convert each value to a json
	    	try {
				JSONObject tempJson = new JSONObject(text.toString());
				//if we currently dont have any return json for this reducer, set it to the json we just decoded
				if(returnJson == null ) {
					returnJson = tempJson;
				//else we should compare them, logging any diffs, and updating to the newer json based on the timestamp we added in the map step
				} else {
					returnJson = logDiffsAndUpdate(returnJson, tempJson);
				}
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
	    result.set(returnJson.toString());
	    context.write(key, result);
	}
	
	private JSONObject logDiffsAndUpdate(JSONObject currentJson, JSONObject tempJson) {
		//get timestamps from both objects
		Date currentJsonDate = new Date(currentJson.getString("fileTimeStamp"));
		Date tempJsonDate = new Date(tempJson.getString("fileTimeStamp"));
		JSONObject returnJson;
		//indicator tells which JSON object is the newer one so it can be passed to the logDiffs fx
		int indicator = 0;
		//replace returnJson with tempJson if temp is newer, else return the currentJson
		if(tempJsonDate.after(currentJsonDate)) {
			returnJson = tempJson;
			indicator = 2;
		}else {
			returnJson = currentJson;
			indicator = 1;
		}
		logDiffs(currentJson, tempJson, indicator);
		
		return returnJson;
	}

	private void logDiffs(JSONObject currentJson, JSONObject tempJson, int indicator) {
		//get the fields for both
		
		//check if the sets of fields are equal
			//if not, combine them into a new set
		
		//iterate through the complete set, checking the newer object for the field (using the indicator)
			//if the field is reponse, call the specific response logger
			//else can compare raw
		
	}
	
	private String generateFileName(Text key) {
		String retString = key.toString();
		return retString;
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException{
		mos.close();
	}
}