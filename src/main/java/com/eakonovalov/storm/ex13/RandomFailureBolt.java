package com.eakonovalov.storm.ex13;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

public class RandomFailureBolt extends BaseRichBolt {

    private static final long serialVersionUID = 1998197408597147062L;

    private static final Integer MAX_PERCENT_OF_FAILILURES = 80;

    private Random r = new Random(System.currentTimeMillis());
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        Integer i = r.nextInt(100);
        if(i < MAX_PERCENT_OF_FAILILURES) {
            collector.emit(input, new Values(input.getString(0), input.getString(1)));
            collector.ack(input);
        }
        else {
            collector.fail(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("int", "bucket"));
    }

}
