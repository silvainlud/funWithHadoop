package edu.reduce.map.fun;


import org.apache.hadoop.io.Text;

public enum Player {
    white(new Text("white")), black(new Text("black"));

    final Text value;

    Player(Text value) {
        this.value = value;
    }

    public Text getValue() {
        return value;
    }
}
