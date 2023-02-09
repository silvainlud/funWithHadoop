package edu.reduce.map.fun;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class GameWritable implements Writable {

    private Text id;
    private Text rated;
    private Text created_at;
    private Text last_move_at;
    private IntWritable turns;
    private Text victory_status;
    private Text winner;
    private Text increment_code;
    private Text white_id;
    private IntWritable white_rating;
    private Text black_id;
    private IntWritable black_rating;
    private Text moves;
    private Text opening_eco;
    private Text opening_name;
    private IntWritable opening_ply;


    public void write(DataOutput dataOutput) throws IOException {
        id.write(dataOutput);
        rated.write(dataOutput);
        created_at.write(dataOutput);
        last_move_at.write(dataOutput);
        turns.write(dataOutput);
        victory_status.write(dataOutput);
        winner.write(dataOutput);
        increment_code.write(dataOutput);
        white_id.write(dataOutput);
        white_rating.write(dataOutput);
        black_id.write(dataOutput);
        black_rating.write(dataOutput);
        moves.write(dataOutput);
        opening_eco.write(dataOutput);
        opening_name.write(dataOutput);
        opening_ply.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {

        id.readFields(dataInput);
        rated.readFields(dataInput);
        created_at.readFields(dataInput);
        last_move_at.readFields(dataInput);
        turns.readFields(dataInput);
        victory_status.readFields(dataInput);
        winner.readFields(dataInput);
        increment_code.readFields(dataInput);
        white_id.readFields(dataInput);
        white_rating.readFields(dataInput);
        black_id.readFields(dataInput);
        black_rating.readFields(dataInput);
        moves.readFields(dataInput);
        opening_eco.readFields(dataInput);
        opening_name.readFields(dataInput);
        opening_ply.readFields(dataInput);
    }

    public Text getId() {
        return id;
    }

    public Text getRated() {
        return rated;
    }

    public Text getCreated_at() {
        return created_at;
    }

    public Text getLast_move_at() {
        return last_move_at;
    }

    public IntWritable getTurns() {
        return turns;
    }

    public Text getVictory_status() {
        return victory_status;
    }

    public Text getWinner() {
        return winner;
    }

    public Text getIncrement_code() {
        return increment_code;
    }

    public Text getWhite_id() {
        return white_id;
    }

    public IntWritable getWhite_rating() {
        return white_rating;
    }

    public Text getBlack_id() {
        return black_id;
    }

    public IntWritable getBlack_rating() {
        return black_rating;
    }

    public Text getMoves() {
        return moves;
    }

    public String[] getAllMoves() {
        return moves.toString().split(" ");
    }

    public Text getOpening_eco() {
        return opening_eco;
    }

    public Text getOpening_name() {
        return opening_name;
    }

    public IntWritable getOpening_ply() {
        return opening_ply;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof GameWritable) {
            GameWritable otherGame = (GameWritable) obj;
            return otherGame.getId().equals(this.id);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    public void setId(Text id) {
        this.id = id;
    }

    public void setRated(Text rated) {
        this.rated = rated;
    }

    public void setCreated_at(Text created_at) {
        this.created_at = created_at;
    }

    public void setLast_move_at(Text last_move_at) {
        this.last_move_at = last_move_at;
    }

    public void setTurns(IntWritable turns) {
        this.turns = turns;
    }

    public void setVictory_status(Text victory_status) {
        this.victory_status = victory_status;
    }

    public void setWinner(Text winner) {
        this.winner = winner;
    }

    public void setIncrement_code(Text increment_code) {
        this.increment_code = increment_code;
    }

    public void setWhite_id(Text white_id) {
        this.white_id = white_id;
    }

    public void setWhite_rating(IntWritable white_rating) {
        this.white_rating = white_rating;
    }

    public void setBlack_id(Text black_id) {
        this.black_id = black_id;
    }

    public void setBlack_rating(IntWritable black_rating) {
        this.black_rating = black_rating;
    }

    public void setMoves(Text moves) {
        this.moves = moves;
    }

    public void setOpening_eco(Text opening_eco) {
        this.opening_eco = opening_eco;
    }

    public void setOpening_name(Text opening_name) {
        this.opening_name = opening_name;
    }

    public void setOpening_ply(IntWritable opening_ply) {
        this.opening_ply = opening_ply;
    }
}
