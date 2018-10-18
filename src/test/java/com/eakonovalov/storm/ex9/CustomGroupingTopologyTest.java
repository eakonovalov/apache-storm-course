package com.eakonovalov.storm.ex9;

import org.apache.storm.LocalCluster;
import org.junit.Test;

public class CustomGroupingTopologyTest {
    @Test
    public void test() {
        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("Custom-Grouping-Topology", CustomGroupingTopology.createConfig(),
                    CustomGroupingTopology.createTopology());
            Thread.sleep(15000);
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
        } finally {
            cluster.shutdown();
        }
    }

}
