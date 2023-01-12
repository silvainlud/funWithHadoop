package edu.reduce.map.fun.writable;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

import java.util.Arrays;
import java.util.stream.Collectors;

public class TextArrayWritable extends ArrayWritable {
    public TextArrayWritable(Text[] values) {
        super(Text.class, values);
    }

    @Override
    public Text[] get() {
        return (Text[]) super.get();
    }

    @Override
    public String toString() {
        Text[] values = get();
        return String.join(",", Arrays.stream(values).map(Text::toString).collect(Collectors.toList()));

    }
}
