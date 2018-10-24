package com.eakonovalov.storm.ex17;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class SimpleFunction extends BaseFunction {

    private static final long serialVersionUID = -7716829723096066262L;

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        collector.emit(new Values(tuple.getString(0) + " processed"));
    }

}
