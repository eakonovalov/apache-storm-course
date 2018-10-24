package com.eakonovalov.storm.ex18;

import org.apache.storm.trident.operation.FlatMapFunction;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Split implements FlatMapFunction {

    private static final long serialVersionUID = -7667994164815456062L;

    @Override
    public Iterable<Values> execute(TridentTuple input) {
        List<Values> result = new ArrayList<>();
        for(String word : input.getString(0).split("\\s+")) {
            result.add(new Values(word));
        }

        return result;
    }

}
