
//new File
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;

public class MinMaxMean {
    public static class CustomMinMax implements Writable {
        
        Integer min;
	Integer max;
	Double mean;
	
	// constructor
	public CustomMinMax() {
		min=0;
		max=0;
		mean=0.0;
	}
	//set method
	void setMin(Integer duration){
		this.min=duration;
	}
	void setMax(Integer duration){
		this.max=duration;
	}
	//get method
	Integer getMin() {
		return min;
	}
	Integer getMax(){
		return max;
	}
	
	//set method
	void setMean(Double value)
	{
		this.mean=value;
	}
	
	Double getMean(){
		return mean;
	}
        	public void write(DataOutput out) throws IOException {
			// what order we want to write !
				out.writeInt(min);
				out.writeInt(max);
				out.writeDouble(mean);
		}
        

	public void readFields(DataInput in) throws IOException {
			min=new Integer(in.readInt());
			max=new Integer(in.readInt());
			mean=new Double(in.readInt());
		}

		public String toString() {
			return min + "\t" + max + "\t" +mean;
		}
    }
    
  public static class TokenizerMapper 
       extends Mapper<LongWritable,Text,Text,CustomMinMax> {
    
    private final static IntWritable one = new IntWritable(1);
    private CustomMinMax customMinMax = new CustomMinMax();
      Text word=new Text();
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String cols[] = value.toString().split(",");
	  word.set(cols[0]);
	  Integer field4=Integer.parseInt(cols[3]);
	  customMinMax.setMin(field4);
	  customMinMax.setMax(field4);
	  
      
        context.write(word, customMinMax);
      }
    }

    
    public static class MyCombiner
    extends Reducer<Text,CustomMinMax,Text,CustomMinMax> {
        private CustomMinMax result = new CustomMinMax();
        
        public void reduce(Text key, Iterable<CustomMinMax> values,
                           Context context
                           ) throws IOException, InterruptedException {
            /*int sum = 0;
            for (WordAndCount val : values) {
                sum += val.getCount().get();
            }
            result.setCount(new IntWritable(sum));
            result.setWord(key);
			*/
			int min=-100,max=0,sum=0,count=0;
			for (Object val : values) {
					CustomMinMax obj = (CustomMinMax) val;
					if(min== -100)
					{ 	
						min=obj.getMin();
					}
					else if(obj.getMin()<min){
						min=obj.getMin();
					}
					if(max==0)
                                        {
                                                max=obj.getMax();
                                        }
                                        else if(obj.getMax()>max){
                                                max=obj.getMax();
                                        }
					sum+=obj.getMin();
					count++;

				}
			result.setMin(min);
			result.setMax(max);
			result.setMean((double)sum/count);
			/*
			int max=0;
                        for (Object val : values) {
                                        CustomMinMax obj = (CustomMinMax) val;
                                        if(max==0)
                                        {
                                                max=obj.getMax();
                                        }
                                        else if(obj.getMax()>max){
                                                max=obj.getMax();
                                        }
                                }
                        //result.setMin(min);
			result.setMax(max);
	
				*/
            context.write(key, result);
        }
    }

  
  /*public static class IntSumReducer 
       extends Reducer<Text,WordAndCount,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<WordAndCount> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (WordAndCount val : values) {
          sum += val.getCount().get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }*/

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: WordCountv3 <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "word count v3");
    job.setJarByClass(MinMaxMean.class);
      
      job.setMapperClass(TokenizerMapper.class);
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(CustomMinMax.class);
      
//    job.setCombinerClass(MyCombiner.class);
      
    job.setReducerClass(MyCombiner.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
