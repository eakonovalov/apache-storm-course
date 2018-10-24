package com.eakonovalov.storm.reconciliation;

import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBatchBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*;

/**
 * Created by ekonovalov on 2018-10-18.
 */
public class ReconciliationBolt extends BaseBatchBolt<Long> {

    private static final long serialVersionUID = -4217203080033204045L;

    private BatchOutputCollector collector;
    private Long id;

    private Map<List<String>, List<String>> preValues;
    private Map<List<String>, List<String>> postValues;
    private Set<List<String>> matches;
    private int preDuplicates = 0;
    private int postDuplicates = 0;

    @Override
    public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Long id) {
        this.collector = collector;
        this.id = id;
        preValues = new HashMap<>();
        postValues = new HashMap<>();
        matches = new HashSet<>();
    }

    @Override
    public void execute(Tuple tuple) {
        String fileType = tuple.getString(1);
        List<String> keys = (List<String>) tuple.getValue(3);
        List<String> values = (List<String>) tuple.getValue(4);

        if (matches.contains(keys)) {
            if ("PRE".equals(fileType)) {
                preDuplicates++;
            } else {
                postDuplicates++;
            }
        } else {
            Map<List<String>, List<String>> self = "PRE".equals(fileType) ? preValues : postValues;
            Map<List<String>, List<String>> target = "PRE".equals(fileType) ? postValues : preValues;

            if (self.containsKey(keys)) {
                if ("PRE".equals(fileType)) {
                    preDuplicates++;
                } else {
                    postDuplicates++;
                }
            } else {
                if (target.get(keys) != null) {
                    target.remove(keys);
                    matches.add(keys);
                } else {
                    self.putIfAbsent(keys, values);
                }
            }

        }

    }

    @Override
    public void finishBatch() {
        Result r = new Result();
        r.matches = matches.size();
        r.preDuplicates = preDuplicates;
        r.postDuplicates = postDuplicates;
        r.preOrphans = preValues.size();
        r.postOrphans = postValues.size();
        collector.emit(new Values(id, r));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "result"));
    }

}
