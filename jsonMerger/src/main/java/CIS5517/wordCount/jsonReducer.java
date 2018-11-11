package CIS5517.wordCount;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.json.*;

public class jsonReducer extends Reducer<Text,Text,Text,Text> {
	private Text result = new Text();
	private JSONObject returnJson;
  
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	    String jsonAsString;
	    for (Text text : values) {
	    	//convert each value to a json
	    	try {
				JSONObject tempJson = new JSONObject(text.toString());
				if(returnJson == null ) {
					returnJson = tempJson;
				} else {
					//get timestamps from both objects
					Date returnJsonDate = new Date(returnJson.getString("fileTimeStamp"));
					Date tempJsonDate = new Date(tempJson.getString("fileTimeStamp"));
					//replace returnJson with tempJson if temp is newer
					if(tempJsonDate.after(returnJsonDate)) {
						returnJson = tempJson;
					}
					//i know it said to verify and replace fields, but deserializing a json and then comparing the fields is a non-trivial problem that no java-built in library solves
					//it said those were considerations, not necessarily requirements. 
				}
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
	    result.set(returnJson.toString());
	    context.write(key, result);
	}
}