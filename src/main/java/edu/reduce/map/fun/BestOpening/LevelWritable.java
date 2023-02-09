package edu.reduce.map.fun.BestOpening;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LevelWritable implements WritableComparable<LevelWritable> {
    private Level level;

    public LevelWritable(int i){
        level = Level.fromLevel(i);
    }

    public LevelWritable(){
        level = Level.OK;
    }

    public Level getLevel() {
        return level;
    }

    @Override
    public int compareTo(LevelWritable o) {
        return o.getLevel().compareTo(this.getLevel());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        new IntWritable(level.ordinal()).write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        IntWritable ordinal = new IntWritable();
        ordinal.readFields(dataInput);
        this.level = Level.values()[ordinal.get()];
    }

    @Override
    public String toString() {
        return level.toString();
    }
}
