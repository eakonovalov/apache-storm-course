package com.eakonovalov.storm.ex21;

import com.eakonovalov.storm.ex18.Split;
import com.eakonovalov.storm.ex18.ToLowercase;
import org.apache.storm.Config;
import org.apache.storm.LocalDRPC;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class StateTopology {

    public static StormTopology createTopology(LocalDRPC drpc) {
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3,
                new Values("the cow jumped over the moon and the man"),
                new Values("the man is a great cow"),
                new Values("the cow will come home"));

        spout.setCycle(true);

        TridentTopology topology = new TridentTopology();

        TridentState state = topology.newStream("Spout", spout)
                .map(new ToLowercase())
                .flatMap(new Split())
                .groupBy(new Fields("sentence"))
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"));

        topology.newDRPCStream("State", drpc)
                .stateQuery(state, new Fields("args"), new MapGet(), new Fields("count"));

        return topology.build();
    }

    public static Config createConfig() {
        Config config = new Config();
        config.setDebug(true);

        return config;
    }

}
