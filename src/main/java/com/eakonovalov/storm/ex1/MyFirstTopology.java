package com.eakonovalov.storm.ex1;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Created by ekonovalov on 2018-10-16.
 */
public class MyFirstTopology {

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("My-First-Spout", new MyFirstSpout());
        builder.setBolt("My-First-Bolt", new MyFirstBolt()).shuffleGrouping("My-First-Spout");

        Config config = new Config();
        config.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("My-First-Topology", config, builder.createTopology());
            Thread.sleep(11000);
        }
        catch (InterruptedException e) {
            System.out.println(e.getMessage());
        }
        finally {
            cluster.shutdown();
        }

    }

}
