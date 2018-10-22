package com.eakonovalov.storm.ex13;

import org.apache.storm.LocalCluster;
import org.junit.Test;

public class RandomFailureTopologyTest {
    @Test
    public void test() {
        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("Random-Failure-Topology", RandomFailureTopology.createConfig(),
                    RandomFailureTopology.createTopology());
            Thread.sleep(15000);
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
        } finally {
            cluster.shutdown();
        }
    }

}
