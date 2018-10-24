package com.eakonovalov.storm.ex22;

import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

/**
 * Created by ekonovalov on 2018-10-24.
 */
public class ErrorAggregator extends BaseAggregator<ErrorAggregator.State> {

    private static final long serialVersionUID = -2457051324873210339L;

    @Override
    public State init(Object batchId, TridentCollector collector) {
        return new State();
    }

    @Override
    public void aggregate(State state, TridentTuple tuple, TridentCollector collector) {
        if (tuple.getString(0).equals("Failure")) {
            state.count++;
        }
    }

    @Override
    public void complete(State state, TridentCollector collector) {
        collector.emit(new Values(new Long[]{state.count}));
    }

    static class State {

        Long count = 0L;

    }

}
