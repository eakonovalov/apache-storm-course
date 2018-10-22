package com.eakonovalov.storm.ex13;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

public class RandomFailureTopology {

    public static StormTopology createTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("Integer-Spout", new IntegerSpout());
        builder.setBolt("Random-Failure-Bolt", new RandomFailureBolt()).shuffleGrouping("Integer-Spout");

        return builder.createTopology();
    }

    public static Config createConfig() {
        Config config = new Config();
        config.setDebug(true);

        return config;
    }

}
