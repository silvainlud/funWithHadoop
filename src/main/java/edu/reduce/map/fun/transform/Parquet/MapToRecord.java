package edu.reduce.map.fun.transform.Parquet;

import edu.reduce.map.fun.GameWritable;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapToRecord extends Mapper<LongWritable, GameWritable, Void, GenericRecord> {

    @Override
    protected void map(LongWritable key, GameWritable value, Context context) throws IOException, InterruptedException {
        if (key.get() != 0) {
            context.write(null, GameWritables.record(value));
        }
    }
}