package com.eakonovalov.storm.reconciliation;

import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBatchBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by ekonovalov on 2018-10-18.
 */
public class CollectorBolt extends BaseBatchBolt<Long> {

    private static final long serialVersionUID = -4217203080033204045L;

    private BatchOutputCollector collector;
    private Long id;

    private Map<String, Map<String, String>> preValues = new HashMap<>();
    private Map<String, Map<String, String>> postValues = new HashMap<>();

    @Override
    public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Long id) {
        this.collector = collector;
        this.id = id;
    }

    @Override
    public void execute(Tuple tuple) {
        String fileType = tuple.getString(1);
        String fileName = tuple.getString(2);
        String keys = tuple.getString(3);
        String values = tuple.getString(4);

        Map<String, Map<String, String>> target = null;
        if(fileType.equals("PRE")) {
            target = preValues;
        }
        else if(fileType.equals("POST")) {
            target = postValues;
        }

        if(target != null) {
            Map<String, String> map = target.get(fileName);
            if(map == null) {
                map = new HashMap<>();
                target.put(fileName, map);
            }
            map.put(keys, values);
        }
    }

    @Override
    public void finishBatch() {
        int count = 0;
        for(Map<String, String> values : preValues.values()) {
            count += values.size();
        }
        for(Map<String, String> values : postValues.values()) {
            count += values.size();
        }
        collector.emit(new Values(id, count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "result"));
    }

}
