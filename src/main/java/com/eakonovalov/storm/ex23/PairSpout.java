package com.eakonovalov.storm.ex23;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

/**
 * Created by ekonovalov on 2018-10-24.
 */
public class PairSpout extends BaseRichSpout {

    private static final long serialVersionUID = 8892627630378932814L;

    private Random r = new Random(System.currentTimeMillis());
    private SpoutOutputCollector collector;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        collector.emit(new Values(r.nextInt(100), r.nextInt(100)));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("x1", "x2"));
    }

}
