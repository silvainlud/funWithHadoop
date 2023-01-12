package edu.reduce.map.fun.elo;

import edu.reduce.map.fun.PairWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NewEloReducer extends Reducer<LongWritable, PairWritable<EloWritable, EloWritable>, Text, IntWritable> {
    @Override
    protected void reduce(LongWritable key, Iterable<PairWritable<EloWritable, EloWritable>> values, Reducer<LongWritable, PairWritable<EloWritable, EloWritable>, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        for (PairWritable<EloWritable, EloWritable> v : values) {
            context.write(v.getFirst().getId(), v.getFirst().getElo());
            context.write(v.getSecond().getId(), v.getSecond().getElo());
        }
    }
}
