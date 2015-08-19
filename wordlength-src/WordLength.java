

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.util.Iterator;
import java.util.StringTokenizer;



public class WordLength {

    public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output) throws IOException {
      String line = value.toString();

      StringTokenizer tokenizer = new StringTokenizer(line);
      

      while (tokenizer.hasMoreTokens()) {
    	Text word = new Text();
    	LongWritable longWritable = new LongWritable();
        String temp = tokenizer.nextToken();
    	word.set(temp);
    	longWritable.set(Integer.toString(temp.length()));
    	output.collect(longWritable, word);
      }
    }
    
  	 
  public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<LongWritable, LongWritable> output) throws IOException {

	  int sum = 0;
	  while (values.hasNext()) {
		  Text dummy = values.next();
		  sum += 1;
	  }
	  LongWritable ret = new LongWritable();
	  ret.set(Integer.toString(sum));
	  output.collect(key, ret);
  }
  
  public static void main(String[] args) throws IOException {
	 
  }
  
}