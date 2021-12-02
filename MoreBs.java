// input gs://coc105-gutenburg-10000books/
// output gs://biggerthanlksbucket/outputbsfinal/
import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;

public class MoreBs {

  public static class WCMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    // contains the three words as a Text
    private Text wordText = new Text();
    // contains the three words as a StringBuffer
    StringBuffer wordSB = new StringBuffer("");

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      // replaceAll \\s+ to get rid of extra blank spaces
      // trim to get rid of leading blank spaces
      String[] words = value.toString().replaceAll("\\p{Punct}","").toLowerCase().replaceAll("\\s+", " ").trim().split("\\s"); 
      
      
      // creating an n-gram for each three consecutive words
      for (int i=0; i<words.length - 3; i++) {
        // create the n-gram
    	  wordSB.append(words[i]);
    	  wordSB.append(" ");
    	  wordSB.append(words[i+1]);
    	  wordSB.append(" ");
    	  wordSB.append(words[i+2]);
        
        // add the n-gram to context, reset the buffer
    	  wordText.set(wordSB.toString());
    	  context.write(wordText, one);
    	  wordSB.setLength(0);
      }
    }
  }

  public static class WCReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    // adjusting filesize to get good number of splits, aiming for 400 cus i have 4 nodes
    conf.set("mapreduce.input.fileinputformat.split.maxsize", "8000000");

    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(MoreBs.class);
    job.setMapperClass(WCMapper.class);
    job.setReducerClass(WCReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setInputFormatClass(CombineTextInputFormat.class);
    
    // combiner added
    job.setCombinerClass(WCReducer.class);
    
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
