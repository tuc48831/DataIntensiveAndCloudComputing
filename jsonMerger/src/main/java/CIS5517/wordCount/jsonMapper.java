package CIS5517.wordCount;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.json.JSONException;
import org.json.*;

public class jsonMapper extends Mapper<Object, Text, Text, Text>{
//no to do
	private Text articleIdKey = new Text();
	private Text jsonValue = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	//decode file (it's text, and the only newlines occur between json objects!, easy to work with!)
    	String totalJson = value.toString();
    	//the only newlines occur between objects from what i could see opening the first few files
    	String[] jsons = totalJson.split("\\n");
		//get the string of the input file articleID_timestamp
		String filepath = ( (FileSplit) context.getInputSplit()).getPath().getName();
		String[] strings = getIdAndTimestamp(filepath);
		String articleID = strings[0];
		String timeStamp = strings[1];
    	//not sure if there is ever more than 1 json per file but will do this to make it abstract
    	for(String json: jsons) {
    		JSONObject tempJson = null;
    		try {
    			//take the string, convert it to a json and add the timestamp as a field
				tempJson = new JSONObject(json);
	    		tempJson.put("webscrapeTimeStamp",timeStamp);
			} catch (JSONException e) {
				e.printStackTrace();
			}
        	//write the key (the id string) and the value (the total json) to the context for the reducers
        	articleIdKey.set(articleID.trim());
        	jsonValue.set(json.trim());
        	context.write(articleIdKey, jsonValue);
    	}
    	//the reduce will then
    		//check all other fields, 
    		//if there is a mismatch, log what we're replacing, and update to the newer info?
    }

	String[] getIdAndTimestamp(String filepath) {
		//"splits" the filename using _ and .txt so we get an array 2 long with the article ID as index 0 and the timestamp as 1
		String pattern = "_|\\.txt";
		Pattern p = Pattern.compile(pattern);
		String[] retStrings = p.split(filepath);
		return retStrings;
	}
    
  }