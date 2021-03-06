package com.eakonovalov.storm.ex2;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class SimpleBolt extends BaseBasicBolt {

    private static final long serialVersionUID = 988616440115807766L;

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        collector.emit(new Values(input.getString(0).toLowerCase()));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }

}
