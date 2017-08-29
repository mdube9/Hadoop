import org.apache.hadoop.fs.Path;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import java.util.*;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class SampleData{
	public static class SRSMapper extends
			Mapper<Object, Text, NullWritable, Text> {

		private Random rands = new Random();
		private int percentage = 10 ;// default

		
		
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			if (rands.nextInt(100) < percentage) {
				context.write(NullWritable.get(), value);
			}
		}
	}
public static void main(String[] args) throws Exception {
  Configuration conf = new Configuration();
  System.out.print("Main Start Executing");
  Job job = new Job(conf, "Min");
  job.setJarByClass(SampleData.class);

  job.setOutputKeyClass(NullWritable.class);
  job.setNumReduceTasks(0);
  job.setOutputValueClass(Text.class);
  //job.setOutputValueClass(Text.class);
  job.setMapperClass(SRSMapper.class);
  //ob.setReducerClass(ColReducer.class);
  //job.setInputFormatClass(TextInputFormat.class);
  //job.setOutputFormatClass(TextOutputFormat.class);
  FileInputFormat.addInputPath(job, new Path(args[0]));
  FileOutputFormat.setOutputPath(job, new Path(args[1]));
  System.exit(job.waitForCompletion(true) ? 0 : 1);
 }}
