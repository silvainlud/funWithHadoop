package edu.reduce.map.fun.transform;

import edu.reduce.map.fun.GameWritable;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AvroReducer extends Reducer<LongWritable, GameWritable, AvroKey<LongWritable>, AvroValue<GameWritable>> {

    @Override
    protected void reduce(LongWritable key, Iterable<GameWritable> values, Context context) throws IOException, InterruptedException {
        for (GameWritable g : values
        ) {
            context.write(new AvroKey<>(key), new AvroValue<>(g));
        }
    }
}
