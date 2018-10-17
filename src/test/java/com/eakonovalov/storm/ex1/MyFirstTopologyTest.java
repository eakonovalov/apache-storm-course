package com.eakonovalov.storm.ex1;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.junit.Test;

import static org.junit.Assert.*;

public class MyFirstTopologyTest {

    @Test
    public void test() {
        Config config = new Config();
        config.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("My-First-Topology", config, MyFirstTopology.createTopology());
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
