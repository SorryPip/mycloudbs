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

public class MyBs {

  public static class WCMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    // word will hold the iteration added to the list
    private String word;
    // word3 will hold the 3 ngram
    private Text word3 = new Text();
    // parts will hold the words making up the ngram
    private ArrayList<String> parts = new ArrayList<String>();
    

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      
      if (itr.hasMoreTokens()) parts.add(itr.nextToken());
      if (itr.hasMoreTokens()) parts.add(itr.nextToken());
      if (itr.hasMoreTokens()) parts.add(itr.nextToken());
      if (!parts.isEmpty()) {
    	  word3.set(String.join(" ", parts));
          context.write(word3, one);
      }
      
      while (itr.hasMoreTokens()) {
        parts.remove(0);
        word = itr.nextToken();
        parts.add(word);
        word3.set(String.join(" ", parts));
        context.write(word3, one);
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
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(MyBs.class);
    job.setMapperClass(WCMapper.class);
    job.setReducerClass(WCReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
