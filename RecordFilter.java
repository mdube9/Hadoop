//package org.myorg;
import java.io.IOException;
import java.util.*;
import java.util.Scanner;
import java.io.BufferedReader;
import java.io.FileReader;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class RecordFilter {

	public static class Map  extends MapReduceBase implements Mapper<LongWritable, Text, Text, NullWritable> {
		//private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private int key1=1;
		public void map(LongWritable key, Text value, OutputCollector <Text,NullWritable> output,Reporter reporter) throws IOException {
			System.out.print("Inside  map");
			/*String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				output.collect(word, one);*/
				String csvFile = "abc.csv";
				String line = "";
				String cvsSplitBy = ",";
				//try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {

						//while ((line = br.readLine()) != null) {
							
							// use comma as separator
							String[] splitString = value.toString().split(cvsSplitBy);
							int field2=Integer.parseInt(splitString[1]);
							int field3=Integer.parseInt(splitString[2]);
							int field4=Integer.parseInt(splitString[3]);
							int field5=Integer.parseInt(splitString[4]);
							if(check(field2,field3,field4,field5)){
								//context.write(key,line);
								
								output.collect(value,NullWritable.get());	
							}

						
       
			
}	

	
	public boolean check(int a,int b, int c, int d){
		if(a <c && b>d)	
		return true;
		else
		return false;
	}
}
	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text,Text> output,Reporter reporter)  throws IOException {
			/*int sum = 0;
			while (values.hasNext()) {i
				sum += values.next().get();
			}*/
			//output.collect(key,new Text("hello") );
			//context.write(values);
	}
	
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(RecordFilter.class);
		conf.setJobName("Record");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(NullWritable.class);

		conf.setMapperClass(Map.class);
		conf.setNumReduceTasks(0);
		//conf.setCombinerClass(Reduce.class);
	//	conf.setReducerClass(NullWritable.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
	
	public boolean check(int field2, int field3, int field4, int field5)
	{
		if(field2 < field4 && field3 >field4){
			return true;
		}
		else{
			return false;
		}
	}

}
