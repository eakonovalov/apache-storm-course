package com.eakonovalov.storm.ex4;

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
        String firstName = input.getStringByField("first_name");
        String lastName = input.getString(2);

        writer.println(firstName + "," + lastName);
        writer.flush();
        collector.emit(new Values(firstName, lastName));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("first_name", "last_name"));
    }

}
