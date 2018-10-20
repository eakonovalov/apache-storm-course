package com.eakonovalov.storm.reconciliation;

import java.io.Serializable;

public class Result implements Serializable {

    private static final long serialVersionUID = 5065254460103105738L;

    int matches;
    int preDuplicates;
    int postDuplicates;
    int preOrphans;
    int postOrphans;

    @Override
    public String toString() {
        return "Result{" +
                "matches=" + matches +
                ", preDuplicates=" + preDuplicates +
                ", postDuplicates=" + postDuplicates +
                ", preOrphans=" + preOrphans +
                ", postOrphans=" + postOrphans +
                '}';
    }

}
