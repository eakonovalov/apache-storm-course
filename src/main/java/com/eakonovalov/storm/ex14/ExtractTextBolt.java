package com.eakonovalov.storm.ex14;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import twitter4j.Status;

public class ExtractTextBolt extends BaseBasicBolt {

    private static final long serialVersionUID = -2157721839956456016L;

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        Status status = (Status) input.getValueByField("tweet");
        String text = status.getText();
        collector.emit(new Values(text));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet"));
    }

}
