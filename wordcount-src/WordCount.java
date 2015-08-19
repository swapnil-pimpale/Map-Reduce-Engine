

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.util.Iterator;
import java.util.StringTokenizer;



public class WordCount {

    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output) throws IOException {
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);
      
      IntWritable one = new IntWritable();
      one.set("1");
      while (tokenizer.hasMoreTokens()) {
        Text word = new Text();
        word.set(tokenizer.nextToken());
        output.collect(word, one);
      }
    }
    
  	 
  public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output) throws IOException {
	  int sum = 0;
	  while (values.hasNext()) {
		  sum += values.next().get();
	  }
	  IntWritable ret = new IntWritable();
	  ret.set(Integer.toString(sum));
	  output.collect(key, ret);
  }
  
  public static void main(String[] args) throws IOException {
	 
  }
  
}