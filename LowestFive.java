import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class LowestFive {

 public static class LowMapper extends Mapper<Object, Text, NullWritable, Text> {

   //private int N = 5; 
   private SortedMap<Integer, Text> top = new TreeMap<Integer, Text>();

   @Override
   public void map(Object key, Text value, Context context)throws IOException, InterruptedException {
      String arr[]=value.toString().split(",");
      String field1=arr[0].toString();
      int field4=Integer.parseInt(arr[3]);
      String newString = field1 + "," + field4;
      top.put(field4, new Text(newString));
      
      if (top.size() > 5) {//Size of map should not exceed 5
         top.remove(top.lastKey()); //Removing the object at last key
      }
   }
   
   //Clean up will be called after mapper
   @Override
    protected void cleanup(Context context) throws IOException,
         InterruptedException {
      for (Text value : top.values()) {
         context.write(NullWritable.get(), value);
      }
   }

 }

 public static class LowReducer  extends Reducer<NullWritable, Text, NullWritable, Text> {
   //private int N = 5; 
   private SortedMap<Integer, Text> top = new TreeMap<Integer, Text>();

   @Override
   public void reduce(NullWritable key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
      
      for (Text value : values) {
         String valString[]=value.toString().split(",");
         String combinedString = valString[0] + "," + valString[1];
         int freq =  Integer.parseInt(valString[1]);
         top.put(freq, new Text(combinedString));
         
         if (top.size() > 5) {
            top.remove(top.lastKey());
         }
      }
      
      List<Integer> keys = new ArrayList<Integer>(top.keySet());
        for(int i=0; i<=keys.size()-1; i++){
         context.write(NullWritable.get(), new Text(top.get(keys.get(i))));
      }
   }
   
 }

 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      //System.err.println("Usage: LowestFive <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "LowestFive");
    job.setJarByClass(LowestFive.class);
    job.setMapperClass(LowMapper.class);
    job.setCombinerClass(LowReducer.class);
    job.setReducerClass(LowReducer.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

