package edu.reduce.map.fun.elo;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class EloWritable implements Writable {
    private Text id;
    private IntWritable elo;

    public EloWritable() {
        id = new Text();
        elo = new IntWritable();
    }

    public EloWritable(Text id, IntWritable elo) {
        this.id = id;
        this.elo = elo;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        id.write(dataOutput);
        elo.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        id.readFields(dataInput);
        elo.readFields(dataInput);
    }

    @Override
    public String toString() {
        return id + "\t" + elo;
    }

    public IntWritable getElo() {
        return elo;
    }

    public Text getId() {
        return id;
    }
}
