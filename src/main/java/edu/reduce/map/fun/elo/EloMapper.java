package edu.reduce.map.fun.elo;

import edu.reduce.map.fun.GameWritable;
import edu.reduce.map.fun.Player;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class EloMapper extends Mapper<LongWritable, GameWritable, Text, EloPairWritable> {

    private static final int K = 15;

    @Override
    protected void map(LongWritable key, GameWritable value, Context context) throws IOException, InterruptedException {
        if (key.get() == 0)
            return;
        context.write(value.getId(), computeNewElo(value));

    }

    protected static EloPairWritable computeNewElo(GameWritable game) {
        if (Player.black.getValue().equals(game.getWinner())) {
            return newElo(game.getBlack_id(), game.getBlack_rating().get(), game.getWhite_id(), game.getWhite_rating().get());
        } else if (Player.white.getValue().equals(game.getWinner())) {
            return newElo(game.getWhite_id(), game.getWhite_rating().get(), game.getBlack_id(), game.getBlack_rating().get());
        }

        return newElo(game.getWhite_id(), game.getWhite_rating().get(), game.getBlack_id(), game.getBlack_rating().get(), false);

    }

    protected static EloPairWritable newElo(Text winnerId, int winnerElo, Text loseId, int loseElo) {
        return newElo(winnerId, winnerElo, loseId, loseElo, false);
    }

    protected static EloPairWritable newElo(Text winnerId, int winnerElo, Text loseId, int loseElo, boolean equality) {
        int elo1 = newElo(winnerElo, loseElo, equality ? 0.5 : 1.0);
        int elo2 = newElo(loseElo, winnerElo, equality ? 0.5 : 0.0);

        return new EloPairWritable(new EloWritable(winnerId, new IntWritable(elo1)), new EloWritable(loseId, new IntWritable(elo2)));
    }


    protected static int newElo(int elo1, int elo2, double W) {
        return Math.toIntExact(Math.round(elo1 + K * (W - gainProbability(elo1, elo2))));
    }

    protected static double gainProbability(int elo1, int elo2) {
        double diff = elo1 - elo2;
        return 1.0 / (1.0 + Math.pow(10.0, (-diff / 400.0)));
    }
}
