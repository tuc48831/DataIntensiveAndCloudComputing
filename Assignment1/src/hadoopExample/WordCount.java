package hadoopExample;

public class WordCount{
//update with structure of pseudocode from notes
	
//map
	//key: document name
	//value: text of the document
	//for each word w in value
		//emit (w,1)
	
//reduce
	//key: a word
	//value: an iterator over counts
	//result = 0
	//for each count v in values:
		//result += v
	//emit(key, result)
	

	public static void main(String args[]) {
		//load file
		
		//call mapreduce somehow? 
			//Do I do that from inside the program?
			//is it from the command line like the example program?
		
		//write output to file somewhere
	}
	
}
