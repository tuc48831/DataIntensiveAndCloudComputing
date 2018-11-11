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
import org.json.*;

public class wordMapper extends Mapper<Text, Text, Text, Text>{
	  
    private final static IntWritable one = new IntWritable(1);
    private Text idStringKey = new Text();
    private Text jsonValue = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	
    	//decode file (it's text, and the only newlines occur between json objects!, easy to work with!)
    	String totalJson = value.toString();
    	//the only newlines occur between objects from what i could see opening the first few files
    	String[] jsons = totalJson.split("\\n");
    	
    	//not sure if there is ever more than 1 json per file but will do this to make it abstract
    	for(String json: jsons) {
    		JSONObject tempJson = null;
    		try {
				tempJson = new JSONObject(value.toString());
				Date lastUpdated;
		    	//a date formatter according to the header from the file (Sat Oct 20 15:34:27 EDT 2018)
		    	SimpleDateFormat formatter = new SimpleDateFormat("EEE MMM DD HH:mm:ss zzz yyyy");
		    	//try to get the date according to the format
		    	try {
					lastUpdated = formatter.parse(jsons[0]);
		    		tempJson.put("fileTimeStamp",lastUpdated.toString());
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    		//use ID field as unique identifier to map on (use regex to get the id)
    		String pattern = "\"id\":(\\d+),";
    		Pattern p = Pattern.compile(pattern);
        	Matcher m = p.matcher(json);
        	String idString = m.group(0);
        	//write the key (the id string) and the value (the total json) to the context for the reducers
        	idStringKey.set(idString);
        	jsonValue.set(json);
        	context.write(idStringKey, jsonValue);
    	}
    	//the reduce will then
    		//check all other fields, 
    		//if there is a mismatch, log what we're replacing, and update to the newer info?
    	
    }
    
  }