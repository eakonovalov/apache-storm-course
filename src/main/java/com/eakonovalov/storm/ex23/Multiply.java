package com.eakonovalov.storm.ex23;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

/**
 * Created by ekonovalov on 2018-10-24.
 */
public class Multiply extends BaseFunction {

    private static final long serialVersionUID = -1283487489125160408L;

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        collector.emit(new Values(tuple.getInteger(0) * tuple.getInteger(1)));
    }

}
