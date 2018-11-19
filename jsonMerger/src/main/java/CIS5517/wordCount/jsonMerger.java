package CIS5517.wordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;

public class jsonMerger {
//TODO handle input and output
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "jsonMerge");
		job.setJarByClass(jsonMerger.class);
		job.setMapperClass(jsonMapper.class);
		job.setCombinerClass(jsonReducer.class);
		job.setReducerClass(jsonReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		Path input = new Path("./input");
		FileInputFormat.addInputPath(job, input);

		Path output = new Path("./output");
		
		FileSystem fs = FileSystem.get(conf);
		fs.delete(output);
		
		FileOutputFormat.setOutputPath(job, output);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		//do i need to get all the article ids at the start to map them to files?
	}
}