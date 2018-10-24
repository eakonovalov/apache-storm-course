package com.eakonovalov.storm.ex19;

import com.eakonovalov.storm.ex18.Split;
import com.eakonovalov.storm.ex18.ToLowercase;
import org.apache.storm.Config;
import org.apache.storm.LocalDRPC;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentTopology;

public class FilterTopology {

    public static StormTopology createTopology(LocalDRPC drpc) {
        TridentTopology topology = new TridentTopology();
        topology.newDRPCStream("Filter", drpc)
                .map(new ToLowercase())
                .flatMap(new Split())
                .filter(new FilterShort());

        return topology.build();
    }

    public static Config createConfig() {
        Config config = new Config();
        config.setDebug(true);

        return config;
    }

}
