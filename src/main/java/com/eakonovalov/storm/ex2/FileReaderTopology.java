package com.eakonovalov.storm.ex2;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

public class FileReaderTopology {

    public static StormTopology createTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("File-Reader-Spout", new FileReaderSpout());
        builder.setBolt("Simple-Bolt", new SimpleBolt()).shuffleGrouping("File-Reader-Spout");

        return builder.createTopology();
    }

}
