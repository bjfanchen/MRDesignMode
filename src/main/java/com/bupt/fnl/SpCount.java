package com.bupt.fnl;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class SpCount {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {    	

        public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException{
            String line = value.toString();
            String[] itr=line.split("\t");
            try{
            if(itr.length<7){return;}
            String userId=itr[1];
            String spName=itr[4];
            long upTraffic=Long.parseLong(itr[5]);
            long downTraffic=Long.parseLong(itr[6]);        
            context.write(new Text(userId+"\t"+spName), new Text(upTraffic+"\t"+downTraffic)); 
            }catch(Exception e){
            	
            }
            
    }
    }
  
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException{
        	long totalUp=0;
            long totalDown=0;
            long num=0;
            for(Text value:values){
            	String[] itr=value.toString().split("\t");
                num++;
                totalUp +=Long.parseLong(itr[0]);
                totalDown +=Long.parseLong(itr[1]);
                		
            }
        	String valueOut=num+"\t"+totalUp+"\t"+totalDown;
        	context.write(key, new Text(valueOut));   
    }
    }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: SpCount <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "SpCount");
    job.setJarByClass(SpCount.class);
    job.setNumReduceTasks(7);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
