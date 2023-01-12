package edu.reduce.map.fun;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

public class GameRecordReader extends RecordReader<LongWritable, GameWritable> {

    LineRecordReader lineRecordReader;

    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        lineRecordReader = new LineRecordReader();
        lineRecordReader.initialize(inputSplit, taskAttemptContext);
    }

    public boolean nextKeyValue() throws IOException, InterruptedException {
        return lineRecordReader.nextKeyValue();
    }

    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return lineRecordReader.getCurrentKey();
    }

    public GameWritable getCurrentValue() throws IOException, InterruptedException {


        String[] split = lineRecordReader.getCurrentValue().toString().split(",");

        GameWritable gameWritable = new GameWritable();

        if (lineRecordReader.getCurrentKey().get() == 0) {
            return null;
        }

        gameWritable.setId(new Text(split[0]));
        gameWritable.setRated(new BooleanWritable(Boolean.parseBoolean(split[1])));
        gameWritable.setCreated_at(new Text(split[2]));
        gameWritable.setLast_move_at(new Text(split[3]));
        gameWritable.setTurns(new IntWritable(Integer.parseInt(split[4])));
        gameWritable.setVictory_status(new Text(split[5]));
        gameWritable.setWinner(new Text(split[6]));
        gameWritable.setIncrement_code(new Text(split[7]));
        gameWritable.setWhite_id(new Text(split[8]));
        gameWritable.setWhite_rating(new IntWritable(Integer.parseInt(split[9])));
        gameWritable.setBlack_id(new Text(split[10]));
        gameWritable.setBlack_rating(new IntWritable(Integer.parseInt(split[11])));
        gameWritable.setMoves(new Text(split[12]));
        gameWritable.setOpening_eco(new Text(split[13]));
        gameWritable.setOpening_name(new Text(split[14]));
        try {
            gameWritable.setOpening_ply(new IntWritable(Integer.parseInt(split[14])));
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }

        return gameWritable;
    }


    public float getProgress() throws IOException, InterruptedException {
        return lineRecordReader.getProgress();
    }

    public void close() throws IOException {
        lineRecordReader.close();
    }
}
