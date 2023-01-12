package edu.reduce.map.fun.elo;

import edu.reduce.map.fun.PairWritable;

public class EloPairWritable extends PairWritable<EloWritable, EloWritable> {
    public EloPairWritable() {
        first = new EloWritable();
        second = new EloWritable();
    }

    public EloPairWritable(EloWritable first, EloWritable second) {
        super(first, second);
    }
}
