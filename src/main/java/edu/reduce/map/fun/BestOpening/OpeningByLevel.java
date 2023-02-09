package edu.reduce.map.fun.BestOpening;

import edu.reduce.map.fun.GameWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OpeningByLevel extends Mapper<LongWritable, GameWritable, LevelWritable, Text> {
    @Override
    protected void map(LongWritable key, GameWritable value, Context context) throws IOException, InterruptedException {
        context.write(new LevelWritable(Math.max(value.getBlack_rating().get(), value.getWhite_rating().get())), value.getOpening_name());
    }
}
