package edu.reduce.map.fun.elo;

import edu.reduce.map.fun.GameInputFormat;
import edu.reduce.map.fun.PairWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class NewEloApp {

    public static void main(final String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        final Job job = configureJob("funWithMapReduce");
        GameInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }

    protected static Job configureJob(final String jobName) throws IOException {
        Job job = Job.getInstance(new Configuration(), jobName);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(EloPairWritable.class);

        job.setInputFormatClass(GameInputFormat.class);

        job.setMapperClass(EloMapper.class);
        //job.setReducerClass(NewEloReducer.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setJarByClass(EloMapper.class);
        return job;
    }
}
