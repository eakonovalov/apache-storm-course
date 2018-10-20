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
        builder.addBolt(new ReadFileBolt(), 4).shuffleGrouping();
        builder.addBolt(new ReconciliationBolt(), 128).fieldsGrouping(new Fields("id", "keys"));
        builder.addBolt(new ResultBolt(), 2).fieldsGrouping(new Fields("id"));

        return builder.createLocalTopology(drpc);
    }

    public static Config createConfig() {
        Config config = new Config();
        config.put("storm.thrift.transport", "org.apache.storm.security.auth.plain.PlainSaslTransportPlugin");
        config.put(Config.STORM_NIMBUS_RETRY_TIMES, 3);
        config.put(Config.STORM_NIMBUS_RETRY_INTERVAL, 10);
        config.put(Config.STORM_NIMBUS_RETRY_INTERVAL_CEILING, 20);

        return config;
    }

}
