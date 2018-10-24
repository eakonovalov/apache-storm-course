package com.eakonovalov.storm.ex17;

import org.apache.storm.Config;
import org.apache.storm.LocalDRPC;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.tuple.Fields;

public class SimpleTopology {

    public static StormTopology createTopology(LocalDRPC drpc) {
        TridentTopology topology = new TridentTopology();
        topology.newDRPCStream("Simple", drpc)
                .each(new Fields("args"), new SimpleFunction(), new Fields("processed_word"));

        return topology.build();
    }

    public static Config createConfig() {
        Config config = new Config();
        config.setDebug(true);

        return config;
    }

}
