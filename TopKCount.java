import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class TopKCount {
        
 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
           
            word.set(tokenizer.nextToken());
            if(word.getLength()==7){
                context.write(word, one);
            }
        }
    }
 } 
        
 public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum -= val.get();
        }
        context.write(key, new IntWritable(sum));
    }
 }

 public static class Map1 extends Mapper<LongWritable, Text, IntWritable, Text> {
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String values[] = line.split("\t");
        IntWritable word = new IntWritable(Integer.parseInt(values[1].trim()));
        context.write(word, new Text(values[0].trim()));
    }
 } 
        
 public static class Reduce1 extends Reducer<IntWritable, Text, Text, IntWritable> {
    int num = 0;
    public void reduce(IntWritable key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
        for (Text val : values) {
            num ++;
            if(num<=100) {
                int kTime = key.get();
                kTime = kTime * (-1);
                context.write(val, new IntWritable(kTime));
            }
            else 
                break;
        }
    }
 }
        
 public static void main(String[] args) throws Exception {
    
    Configuration conf1 = new Configuration();
        
        Job job1 = new Job(conf1, "TopKCount");
    
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);
        
    job1.setMapperClass(Map.class);
    job1.setReducerClass(Reduce.class);
        
    job1.setInputFormatClass(TextInputFormat.class);
    job1.setOutputFormatClass(TextOutputFormat.class);

    job1.setNumReduceTasks(1);
    job1.setJarByClass(TopKCount.class);
        
    FileInputFormat.addInputPath(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, new Path(args[1]));

    job1.waitForCompletion(true);

    Configuration conf2 = new Configuration();
    
        Job job2 = new Job(conf2, "TopKCount");
    
    job2.setOutputKeyClass(IntWritable.class);
    job2.setOutputValueClass(Text.class);
        
    job2.setMapperClass(Map1.class);
    job2.setReducerClass(Reduce1.class);
        
    job2.setInputFormatClass(TextInputFormat.class);
    job2.setOutputFormatClass(TextOutputFormat.class);

    job2.setNumReduceTasks(1);
    job2.setJarByClass(TopKCount.class);
        
    FileInputFormat.addInputPath(job2, new Path(args[1]));
    FileOutputFormat.setOutputPath(job2, new Path(args[2]));

    System.exit(job2.waitForCompletion(true) ? 0 : 1);
        
 }
        
}