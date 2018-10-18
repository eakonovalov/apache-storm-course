package com.eakonovalov.storm.ex12;

import org.apache.storm.Config;
import org.apache.storm.LocalDRPC;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;
import org.apache.storm.generated.StormTopology;

public class DRPCTopology {

    public static StormTopology createTopology(LocalDRPC drpc) {
        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("Plus-Ten-Function");
        builder.addBolt(new PlusTenBolt(), 3);

        return builder.createLocalTopology(drpc);
    }

    public static Config createConfig() {
        Config config = new Config();
        //config.setDebug(true);

        return config;
    }

}
