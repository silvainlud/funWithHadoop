package edu.reduce.map.fun.BestOpening;

import edu.reduce.map.fun.writable.TextArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class BestOpeningReducer extends Reducer<LongWritable, TextArrayWritable, LongWritable, Text> {
    @Override
    protected void reduce(LongWritable key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
        int counter = 0;
        while (values.iterator().hasNext()) {
            context.write(key, new Text(values.iterator().next().toString()));
        }
    }
}
