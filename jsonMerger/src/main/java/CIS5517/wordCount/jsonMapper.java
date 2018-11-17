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

public class jsonMapper extends Mapper<Text, Text, Text, Text>{
//no to do
	private final static IntWritable one = new IntWritable(1);
	private Text articleIdKey = new Text();
	private Text jsonValue = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException, JSONException, ParseException {
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
				tempJson = new JSONObject(json);
				Date lastUpdated;
		    	//a date formatter according to the header from the file (20181015141058 for oct 15th 2018 2:10:58 pm)
		    	SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
		    	//try to get the date according to the format
		    	try {
		    		//convert it to a java date so i dont need to implement my own date comparator in reducer
					lastUpdated = formatter.parse(timeStamp);
		    		tempJson.put("fileTimeStamp",lastUpdated.toString());
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        	//write the key (the id string) and the value (the total json) to the context for the reducers
        	articleIdKey.set(articleID);
        	jsonValue.set(json);
        	context.write(articleIdKey, jsonValue);
    	}
    	//the reduce will then
    		//check all other fields, 
    		//if there is a mismatch, log what we're replacing, and update to the newer info?
    }

	String[] getIdAndTimestamp(String filepath) {
		String pattern = "_|\\.txt";
		Pattern p = Pattern.compile(pattern);
		String[] retStrings = p.split(filepath);
		return retStrings;
	}
    
  }