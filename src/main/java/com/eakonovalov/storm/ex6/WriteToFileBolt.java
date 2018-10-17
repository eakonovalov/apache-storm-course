package com.eakonovalov.storm.ex6;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Map;

public class WriteToFileBolt extends BaseBasicBolt {

    private PrintWriter writer;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        String fileName = "output-" + context.getThisTaskId() + "-" + context.getThisComponentId() + ".txt";
        try {
            writer = new PrintWriter(stormConf.get("folder").toString() + fileName, "UTF-8");
        } catch (FileNotFoundException | UnsupportedEncodingException e) {
            throw new RuntimeException("Exception", e);
        }
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String s = input.getStringByField("integer") + "-" + input.getStringByField("bucket");
        writer.println(s);
        collector.emit(new Values(s));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("result"));
    }

    @Override
    public void cleanup() {
        writer.close();
    }

}
