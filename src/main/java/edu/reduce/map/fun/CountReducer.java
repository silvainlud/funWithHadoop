package edu.reduce.map.fun;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int counter = 0;
        while (values.iterator().hasNext()) {
            values.iterator().next();
            counter++;
        }

        context.write(key, new IntWritable(counter));
    }
}
