package com.eakonovalov.storm.ex13;

import org.apache.log4j.Logger;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IntegerSpout extends BaseRichSpout {

    private static final long serialVersionUID = 8457122708504822183L;

    private static final Logger LOG = Logger.getLogger(IntegerSpout.class);

    private static final Integer MAX_FAILS = 3;

    private SpoutOutputCollector collector;
    private Map<Integer, Integer> failureCount;
    private List<Integer> toSend;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        failureCount = new HashMap<>();
        toSend = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            toSend.add(i);
        }
    }

    @Override
    public void nextTuple() {
        if (!toSend.isEmpty()) {
            for (Integer i : toSend) {
                int bucket = i / 10;
                collector.emit(new Values(Integer.toString(i), Integer.toString(bucket)), i);
            }
            toSend.clear();
        }
    }

    @Override
    public void ack(Object msgId) {
        LOG.info(msgId + " successfully processed");
    }

    @Override
    public void fail(Object msgId) {
        Integer count = 1;
        Integer failedId = (Integer) msgId;

        if (failureCount.containsKey(failedId)) {
            count = failureCount.get(failedId);
        }

        if (count < MAX_FAILS) {
            LOG.info("Resending message [" + failedId + "] for the " + count + " time");
            failureCount.put(failedId, ++count);
            toSend.add(failedId);
        } else {
            LOG.info("Sending message [" + failedId + "] failed after " + MAX_FAILS + " retries!");
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("int", "bucket"));
    }

}
