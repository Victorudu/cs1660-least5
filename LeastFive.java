import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LeastFive {

  public static class LeastFiveMapper
       extends Mapper<Object, Text, Text, IntWritable>{
   
   private TreeMap<Long, String> tmap;

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String[] tokens = value.toString().split(",");
      String item = tokens[0];
      long retroItem = Integer.parseInt(tokens[1]) * -1;
      tmap.put(retroItem, item);

      if (tmap.size() > 5)
        tmap.remove(tmap.firstKey());
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
      for(Map.Entry<Long, String> entry : tmap.entrySet())
      {
        long retroTally = entry.getKey();
        String name = entry.getValue();
        context.write(new Text(name), new IntWritable((int)retroTally));
      }
    }
  }

  public static class LeastFiveReducer
       extends Reducer<Text,IntWritable,IntWritable,Text> {
    private TreeMap<Integer, String> tmap2 = new TreeMap<>();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      String name = key.toString();
      int retroTally = 0;

      for (IntWritable val : values)
        retroTally = val.get();

      tmap2.put(retroTally, name);

      if (tmap2.size() > 5)
        tmap2.remove(tmap2.firstKey());
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
      for (Map.Entry<Integer, String> entry : tmap2.entrySet()) {
        int retroTally = entry.getKey();
        int count = -1 * retroTally;
        String name = entry.getValue();
        context.write(new IntWritable(count), new Text(name));
      }
    }
  }

  public static void main(String[] args) throws Exception {
    long startTime = System.currentTimeMillis();

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "least 5");
    job.setJarByClass(LeastFive.class);
    job.setMapperClass(LeastFiveMapper.class);
    job.setReducerClass(LeastFiveReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    int exitStatus = job.waitForCompletion(true) ? 0 : 1;

    long endTime = System.currentTimeMillis();
    long timeDiff = endTime - startTime;
    System.out.printf("Time elapsed in milliseconds: %d\n", timeDiff);

    System.exit(exitStatus);
  }
}
