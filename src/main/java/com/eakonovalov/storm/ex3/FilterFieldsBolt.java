package com.eakonovalov.storm.ex3;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class FilterFieldsBolt extends BaseBasicBolt {

    private static final long serialVersionUID = 8391372298023120857L;

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String firstName = input.getStringByField("first_name");
        String lastName = input.getString(2);

        collector.emit(new Values(firstName, lastName));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("first_name", "last_name"));
    }

}
