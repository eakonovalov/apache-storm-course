package com.eakonovalov.storm.ex18;

import org.apache.storm.Config;
import org.apache.storm.LocalDRPC;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentTopology;

public class MapTopology {

    public static StormTopology createTopology(LocalDRPC drpc) {
        TridentTopology topology = new TridentTopology();
        topology.newDRPCStream("Map", drpc)
                .map(new ToLowercase())
                .flatMap(new Split());

        return topology.build();
    }

    public static Config createConfig() {
        Config config = new Config();
        config.setDebug(true);

        return config;
    }

}
