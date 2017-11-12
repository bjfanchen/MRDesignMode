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
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;

public class PairSpCount {
    private static Logger logger = Logger.getLogger(PairSpCount.class);
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split("\t");
//            logger.info("words length: " + words.length);
            if (words.length < 7) {
                logger.error("bad line: [ " + Arrays.toString(words) + " ]");
                return;
            }
            String userId = words[1];
            String spName = words[4];
            long upFlow = Long.parseLong(words[5]);
            long downFlow = Long.parseLong(words[6]);

            String pairKey = userId + "\t" + spName;
            String pairValue = upFlow + "\t" + downFlow;

            context.write(new Text(pairKey), new Text(pairValue));
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long accessNum = 0;
            long upTotal = 0;
            long downTotal = 0;

            for (Text value:values) {
                accessNum++;
                String[] words = value.toString().split("\t");
                upTotal += Long.parseLong(words[0]);
                downTotal += Long.parseLong(words[1]);
            }

            String resultValue = accessNum + "\t" + upTotal + "\t" + downTotal;

            context.write(key, new Text(resultValue));
        }
    }

    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        try {
            String[] userArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();

            if (userArgs.length < 2) {
                System.out.println("Usage: PairSpCount inPath outPath");
                System.exit(-1);
            }
            Job job = new Job(configuration, "PairSpCount");
            job.setJarByClass(PairSpCount.class);
            job.setMapperClass(Map.class);
            job.setReducerClass(Reduce.class);
            job.setNumReduceTasks(10);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(userArgs[0]));
            FileOutputFormat.setOutputPath(job, new Path(userArgs[1]));

            job.waitForCompletion(true);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        } catch (ClassNotFoundException e) {
            logger.error(e.getMessage(), e);
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
    }
}
