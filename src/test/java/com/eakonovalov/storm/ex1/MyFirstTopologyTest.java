package com.eakonovalov.storm.ex1;

import org.apache.storm.LocalCluster;
import org.junit.Test;

public class MyFirstTopologyTest {

    @Test
    public void test() {
        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("My-First-Topology", MyFirstTopology.createConfig(), MyFirstTopology.createTopology());
            Thread.sleep(11000);
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
        } finally {
            cluster.shutdown();
        }

    }

}
