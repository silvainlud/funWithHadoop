package edu.reduce.map.fun;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DavidOrGoliathMapper extends Mapper<LongWritable, GameWritable, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, GameWritable value, Context context) throws IOException, InterruptedException {

        if (key.get() == 0)
            return;

        String state = value.getVictory_status().toString();
        int whiteRating = value.getWhite_rating().get();
        int blackRating = value.getBlack_rating().get();


        if (state.equalsIgnoreCase("mate") && whiteRating < blackRating) {
            context.write(new Text("D"), new IntWritable(1));
        } else if (state.equalsIgnoreCase("mate") && whiteRating > blackRating) {
            context.write(new Text("G"), new IntWritable(1));
        }
    }
}
