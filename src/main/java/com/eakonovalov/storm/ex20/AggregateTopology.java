package com.eakonovalov.storm.ex20;

import com.eakonovalov.storm.ex18.Split;
import com.eakonovalov.storm.ex18.ToLowercase;
import org.apache.storm.Config;
import org.apache.storm.LocalDRPC;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.tuple.Fields;

public class AggregateTopology {

    public static StormTopology createTopology(LocalDRPC drpc) {
        TridentTopology topology = new TridentTopology();
        topology.newDRPCStream("Aggregate", drpc)
                .map(new ToLowercase())
                .flatMap(new Split())
                .aggregate(new Count(), new Fields("count"));

        return topology.build();
    }

    public static Config createConfig() {
        Config config = new Config();
        config.setDebug(true);

        return config;
    }

}
