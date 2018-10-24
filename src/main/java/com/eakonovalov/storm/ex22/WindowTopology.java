package com.eakonovalov.storm.ex22;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.trident.windowing.InMemoryWindowsStoreFactory;
import org.apache.storm.trident.windowing.WindowsStoreFactory;
import org.apache.storm.trident.windowing.config.*;
import org.apache.storm.tuple.Fields;

import java.util.concurrent.TimeUnit;

import static org.apache.storm.topology.base.BaseWindowedBolt.Duration;

public class WindowTopology {

    public static StormTopology createTopology() {
        WindowsStoreFactory store = new InMemoryWindowsStoreFactory();
        WindowConfig config =
                //SlidingCountWindow.of(100, 10);
                //TumblingCountWindow.of(100);
                //SlidingDurationWindow.of(new Duration(100, TimeUnit.MILLISECONDS), new Duration(10, TimeUnit.MILLISECONDS));
                TumblingDurationWindow.of(new Duration(100, TimeUnit.MILLISECONDS));

        TridentTopology topology = new TridentTopology();
        topology.newStream("Spout", new LogSpout())
                .window(config, store, new Fields("log"), new ErrorAggregator(), new Fields("count"))
                .each(new Fields("count"), new Debug());

        return topology.build();
    }

    public static Config createConfig() {
        Config config = new Config();
        config.setDebug(true);

        return config;
    }

}
