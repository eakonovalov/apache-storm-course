package com.eakonovalov.storm.ex22;

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
public class LogSpout extends BaseRichSpout {

    private static final long serialVersionUID = 8432965313954264473L;

    private static final Integer MAX_PERCENT_OF_FAILILURES = 80;

    private Random r = new Random(System.currentTimeMillis());
    private SpoutOutputCollector collector;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        Integer i = r.nextInt(100);
        if (i < MAX_PERCENT_OF_FAILILURES) {
            collector.emit(new Values("Success"));
        } else {
            collector.emit(new Values("Failure"));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("log"));
    }

}
