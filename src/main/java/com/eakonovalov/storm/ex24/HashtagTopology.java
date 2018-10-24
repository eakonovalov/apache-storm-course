package com.eakonovalov.storm.ex24;

import com.eakonovalov.storm.ex14.TwitterSpout;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.trident.operation.builtin.FirstN;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.tuple.Fields;

/**
 * Created by ekonovalov on 2018-10-24.
 */
public class HashtagTopology {

    public static StormTopology createTopology() {
        TridentTopology topology = new TridentTopology();

        topology.newStream("Spout", new TwitterSpout())
                .each(new Fields("tweet"), new HashtagExtractor(), new Fields("hashtag"))
                .groupBy(new Fields("hashtag"))
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
                .newValuesStream()
                .applyAssembly(new FirstN(10, "count"))
                .each(new Fields("hashtag", "count"), new Debug());

        return topology.build();
    }

    public static Config createConfig() {
        Config config = new Config();
        config.setDebug(true);

        return config;
    }

}
