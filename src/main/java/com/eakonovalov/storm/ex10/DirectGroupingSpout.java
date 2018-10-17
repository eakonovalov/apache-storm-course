package com.eakonovalov.storm.ex10;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.List;
import java.util.Map;

public class DirectGroupingSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private List<Integer> boltIds;
    private Integer i = 0;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.boltIds = context.getComponentTasks("Write-To-File-Bolt");
    }

    @Override
    public void nextTuple() {
        while (i <= 100) {
            int bucket = i / 10;
            collector.emitDirect(boltIds.get(getBoltId(bucket)), new Values(String.valueOf(i), String.valueOf(bucket)));
            i++;
        }
    }

    private int getBoltId(int bucket) {
        return bucket % boltIds.size();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("integer", "bucket"));
    }

}
