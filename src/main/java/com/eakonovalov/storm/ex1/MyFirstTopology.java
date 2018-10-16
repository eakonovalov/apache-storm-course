package com.eakonovalov.storm.ex1;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Created by ekonovalov on 2018-10-16.
 */
public class MyFirstTopology {

    public static StormTopology createTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("My-First-Spout", new MyFirstSpout());
        builder.setBolt("My-First-Bolt", new MyFirstBolt()).shuffleGrouping("My-First-Spout");

        return builder.createTopology();
    }

}
