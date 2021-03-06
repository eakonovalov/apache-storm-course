package com.eakonovalov.storm.ex14;

import org.apache.storm.LocalCluster;
import org.junit.Test;

public class TwitterTopologyTest {
    @Test
    public void test() {
        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("Twitter-Topology", TwitterTopology.createConfig(), TwitterTopology.createTopology());
            Thread.sleep(15000);
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
        } finally {
            cluster.shutdown();
        }
    }

}
