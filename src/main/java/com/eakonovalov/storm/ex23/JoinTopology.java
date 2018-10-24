package com.eakonovalov.storm.ex23;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.Consumer;
import org.apache.storm.tuple.Fields;

/**
 * Created by ekonovalov on 2018-10-24.
 */
public class JoinTopology {

    public static StormTopology createTopology() {
        TridentTopology topology = new TridentTopology();

        Stream source = topology.newStream("Spout", new PairSpout());
        Stream sum = source.each(new Fields("x1", "x2"), new Add(), new Fields("sum"));
        Stream product = source.each(new Fields("x1", "x2"), new Multiply(), new Fields("product"));

        topology.merge(sum, product).peek((Consumer) input -> System.out.println("From merge stream : " + input));

        topology.join(sum, new Fields("x1", "x2"), product, new Fields("x1", "x2"), new Fields("x1", "x2", "sum", "product"))
                .peek((Consumer) input -> System.out.println("From join stream : " + input));

        return topology.build();
    }

    public static Config createConfig() {
        Config config = new Config();
        config.setDebug(true);

        return config;
    }

}
