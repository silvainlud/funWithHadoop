package edu.reduce.map.fun;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.ParameterizedType;

public abstract class PairWritable<T extends Writable, U extends Writable> implements Writable {

    protected T first;
    protected U second;

    public PairWritable() {

    }


    public PairWritable(T first, U second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        first.write(dataOutput);
        second.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        first.readFields(dataInput);
        second.readFields(dataInput);
    }

    public T getFirst() {
        return first;
    }

    public U getSecond() {
        return second;
    }

    @Override
    public String toString() {
        return getFirst().toString() + "\t" + getSecond().toString();
    }
}
