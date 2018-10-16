package com.eakonovalov.storm.ex3;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

public class ReadFieldsTopology {

    public static StormTopology createTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("Read-Fields-Spout", new ReadFieldsSpout());
        builder.setBolt("Filter-Fields-Bolt", new FilterFieldsBolt()).shuffleGrouping("Read-Fields-Spout");

        return builder.createTopology();
    }

}
