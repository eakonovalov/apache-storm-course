package com.eakonovalov.storm.ex24;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import twitter4j.HashtagEntity;
import twitter4j.Status;

/**
 * Created by ekonovalov on 2018-10-24.
 */
public class HashtagExtractor extends BaseFunction {

    private static final long serialVersionUID = -8011804994129374127L;

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Status status = (Status) tuple.get(0);
        for (HashtagEntity hashtag : status.getHashtagEntities()) {
            collector.emit(new Values(hashtag.getText()));
        }
    }

}
