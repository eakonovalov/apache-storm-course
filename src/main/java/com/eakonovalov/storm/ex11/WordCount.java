package com.eakonovalov.storm.ex11;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

public class WordCount extends BaseBasicBolt {

    private Map<String, Integer> counters;
    private String fileName;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        int id = context.getThisTaskId();
        String name = context.getThisComponentId();
        counters = new HashMap<>();
        fileName = String.valueOf(stormConf.get("folder")) + "output-" + id + "-" + name + ".txt";
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String word = input.getString(0);
        if (!counters.containsKey(word)) {
            counters.put(word, 1);
        } else {
            counters.put(word, counters.get(word) + 1);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void cleanup() {
        try {
            PrintWriter writer = new PrintWriter(fileName, "UTF-8");
            for (Map.Entry<String, Integer> entry : counters.entrySet()) {
                writer.println(entry.getKey() + ": " + entry.getValue());
            }
            writer.close();
        } catch (FileNotFoundException | UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
}
