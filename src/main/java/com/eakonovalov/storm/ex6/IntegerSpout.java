package com.eakonovalov.storm.ex6;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by ekonovalov on 2018-10-17.
 */
public class IntegerSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private Integer i = 0;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        while (i <= 100) {
            Integer bucket = i / 10;
            collector.emit(new Values(String.valueOf(i), String.valueOf(bucket)));
            i++;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("integer", "bucket"));
    }

}
