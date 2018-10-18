package com.eakonovalov.storm.reconciliation;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * Created by ekonovalov on 2018-10-18.
 */
public class SplitBolt extends BaseBasicBolt {

    private static final long serialVersionUID = 7601434584803893809L;

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        Long id = (Long) input.getValue(0);
        String value = input.getString(1);
        int index = value.indexOf(";");
        String fileName = value.substring(0, index);
        collector.emit(new Values(id, "PRE", fileName));
        value = value.substring(index + 1);
        while ((index = value.indexOf(";")) != -1) {
            fileName = value.substring(0, index);
            collector.emit(new Values(id, "POST", fileName));
            value = value.substring(index + 1);
        }
        collector.emit(new Values(id, "POST", value));

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "fileType", "fileName"));
    }

}
