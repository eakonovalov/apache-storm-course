package com.eakonovalov.storm.reconciliation;

import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBatchBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by ekonovalov on 2018-10-18.
 */
public class ResultBolt extends BaseBatchBolt<Long> {

    private static final long serialVersionUID = 1504784961482004166L;

    private BatchOutputCollector collector;
    private Long id;
    private Result result = new Result();

    @Override
    public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Long id) {
        this.collector = collector;
        this.id = id;
    }

    @Override
    public void execute(Tuple tuple) {
        Result result = (Result) tuple.getValue(1);
        this.result.matches += result.matches;
        this.result.preDuplicates += result.preDuplicates;
        this.result.postDuplicates += result.postDuplicates;
        this.result.preOrphans += result.preOrphans;
        this.result.postOrphans += result.postOrphans;
    }

    @Override
    public void finishBatch() {
        collector.emit(new Values(id, result));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "count"));
    }

}
