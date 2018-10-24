package com.eakonovalov.storm.ex22;

import org.apache.storm.LocalCluster;
import org.junit.Test;

public class WindowTopologyTest {

    @Test
    public void test() {
        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("Window-Topology", WindowTopology.createConfig(), WindowTopology.createTopology());
            Thread.sleep(25000);
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
        } finally {
            cluster.shutdown();
        }

    }

}
