package edu.reduce.map.fun.transform;

import edu.reduce.map.fun.GameWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AvroMap extends Mapper<LongWritable, GameWritable, LongWritable, GameWritable> {
    @Override
    protected void map(LongWritable key, GameWritable value, Context context) throws IOException, InterruptedException {
        if (key.get() == 0)
            return;
        context.write(key, value);
    }
}
