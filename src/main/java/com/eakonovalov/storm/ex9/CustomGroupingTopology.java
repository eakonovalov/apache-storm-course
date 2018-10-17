package com.eakonovalov.storm.ex9;

import com.eakonovalov.storm.ex6.IntegerSpout;
import com.eakonovalov.storm.ex6.WriteToFileBolt;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Created by ekonovalov on 2018-10-17.
 */
public class CustomGroupingTopology {

    public static StormTopology createTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("Integer-Spout", new IntegerSpout());
        builder.setBolt("Write-To-File-Bolt", new WriteToFileBolt(), 2)
                .customGrouping("Integer-Spout", new BucketGrouping());

        return builder.createTopology();
    }

    public static Config createConfig() {
        Config config = new Config();
        config.setDebug(true);
        config.put("folder", "target/");

        return config;
    }

}
