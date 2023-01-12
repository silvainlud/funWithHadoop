package edu.reduce.map.fun.BestOpening;

import edu.reduce.map.fun.CountReducer;
import edu.reduce.map.fun.GameInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class OpeningApp {

    public static void main(final String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        final Job job = configureJob("funWithMapReduce");
        GameInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }

    private static Job configureJob(final String jobName) throws IOException {
        Job job = Job.getInstance(new Configuration(), jobName);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(GameInputFormat.class);

        job.setMapperClass(BestOpeningMapper.class);
        job.setReducerClass(CountReducer.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setJarByClass(BestOpeningMapper.class);
        return job;
    }
}
