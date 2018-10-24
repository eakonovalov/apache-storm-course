package com.eakonovalov.storm.ex18;

import org.apache.storm.trident.operation.MapFunction;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class ToLowercase implements MapFunction {

    private static final long serialVersionUID = 3510914347371786388L;

    @Override
    public Values execute(TridentTuple input) {
        return new Values(input.getString(0).toLowerCase());
    }

}
