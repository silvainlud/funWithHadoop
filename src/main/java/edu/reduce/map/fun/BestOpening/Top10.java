package edu.reduce.map.fun.BestOpening;

import org.apache.hadoop.mapreduce.Reducer;

import javax.xml.soap.Text;
import java.io.IOException;
import java.util.HashMap;
import java.util.TreeMap;

public class Top10 extends Reducer<LevelWritable, Text, LevelWritable, Text> {
    @Override
    protected void reduce(LevelWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        TreeMap<String, Integer> counter = new TreeMap<>();
        for (Text i : values) {
            if (!counter.containsKey(i.getValue()))
                counter.put(i.getValue(), 1);
            else
                counter.put(i.getValue(), counter.get(i.getValue()) + 1);
        }



    }
}
