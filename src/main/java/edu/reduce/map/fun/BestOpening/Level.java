package edu.reduce.map.fun.BestOpening;

public enum Level {
    GOOD,
    OK,
    BAD;

    public static Level fromLevel(int elo) {
        if (elo > 2400)
            return GOOD;
        else if (elo > 1200) {
            return OK;
        }
        return BAD;
    }

}
