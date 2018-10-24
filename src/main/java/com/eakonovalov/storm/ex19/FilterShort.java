package com.eakonovalov.storm.ex19;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;

public class FilterShort extends BaseFilter {

    private static final long serialVersionUID = -5272125092162402876L;

    @Override
    public boolean isKeep(TridentTuple tuple) {
        return tuple.getString(0).length() > 3;
    }

}
