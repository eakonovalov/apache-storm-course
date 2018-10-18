package com.eakonovalov.storm.ex2;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

public class FileReaderSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private BufferedReader reader;
    private boolean completed = false;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        try {
            reader = new BufferedReader(new FileReader(conf.get("file").toString()));
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Error reading file [" + conf.get("file") + "]", e);
        }
    }

    @Override
    public void nextTuple() {
        if (!completed) {
            try {
                String line = reader.readLine();
                if (line != null) {
                    collector.emit(new Values(line));
                } else {
                    completed = true;
                    reader.close();
                }
            } catch (IOException e) {
                throw new RuntimeException("Error reading tuple", e);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }

}
