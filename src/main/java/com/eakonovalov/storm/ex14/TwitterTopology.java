package com.eakonovalov.storm.ex14;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

public class TwitterTopology {

    public static StormTopology createTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("Twitter-Spout", new TwitterSpout());
        builder.setBolt("Extract-Text-Bolt", new ExtractTextBolt()).shuffleGrouping("Twitter-Spout");

        return builder.createTopology();
    }

    public static Config createConfig() {
        Config config = new Config();
        config.setDebug(true);

        return config;
    }

}
