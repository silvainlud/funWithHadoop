package edu.reduce.map.fun.transform.Parquet;

import edu.reduce.map.fun.GameWritable;
import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

import static edu.reduce.map.fun.transform.Parquet.CSV2Parquet.loadSchema;


public class GameWritables {
    public static final GameWritable EMPTY = new GameWritable();


    public static Record record(GameWritable gameWritable) throws IOException {
        Record record = new Record(loadSchema());
        record.put("id", gameWritable.getId().toString());
        record.put("rated", gameWritable.getRated().toString());
        record.put("created_at", gameWritable.getCreated_at().toString());
        record.put("last_move_at", gameWritable.getLast_move_at().toString());
        record.put("turns", gameWritable.getTurns().get());
        record.put("victory_status", gameWritable.getVictory_status().toString());
        record.put("winner", gameWritable.getWinner().toString());
        record.put("increment_code", gameWritable.getIncrement_code().toString());
        record.put("white_id", gameWritable.getWhite_id().toString());
        record.put("white_rating", gameWritable.getWhite_rating().get());
        record.put("black_id", gameWritable.getBlack_id().toString());
        record.put("black_rating", gameWritable.getBlack_rating().get());
        record.put("moves", String.join(" ", gameWritable.getAllMoves()));
        record.put("opening_eco", gameWritable.getOpening_eco().toString());
        record.put("opening_name", gameWritable.getOpening_name().toString());
        record.put("opening_ply", 0);
        return record;
    }

    private static Text parseText(String[] columns, int x) {
        return new Text(columns[x]);
    }

    private static IntWritable parseInt(String[] columns, int x) {
        return new IntWritable(Integer.parseInt(columns[x]));
    }

    private static BooleanWritable parseBoolean(String[] columns, int x) {
        return new BooleanWritable(Boolean.parseBoolean(columns[x]));
    }
}
