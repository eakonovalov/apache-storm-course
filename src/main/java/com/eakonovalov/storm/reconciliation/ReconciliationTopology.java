package com.eakonovalov.storm.reconciliation;

import org.apache.storm.Config;
import org.apache.storm.LocalDRPC;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.tuple.Fields;

/**
 * Created by ekonovalov on 2018-10-18.
 */
public class ReconciliationTopology {

    public static StormTopology createTopology(LocalDRPC drpc) {
        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("Reconciliation-Function");
        builder.addBolt(new SplitBolt());
        builder.addBolt(new ReadFileBolt(), 2).shuffleGrouping();
        builder.addBolt(new CollectorBolt(), 2).fieldsGrouping(new Fields("id", "keys"));
        builder.addBolt(new ResultBolt(), 1).fieldsGrouping(new Fields("id"));

        return builder.createLocalTopology(drpc);
    }

    public static Config createConfig() {
        Config config = new Config();
        config.setDebug(true);

        return config;
    }

}
