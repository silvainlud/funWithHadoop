package edu.reduce.map.fun.BestOpening;

import edu.reduce.map.fun.GameWritable;
import edu.reduce.map.fun.writable.TextArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class BestOpeningMapper extends Mapper<LongWritable, GameWritable, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, GameWritable value, Context context) throws IOException, InterruptedException {
        if (key.get() == 0)
            return;

        context.write(new Text(value.getOpening_name()), new IntWritable(1));
    }
}
