package com.eakonovalov.storm.ex8;

import org.apache.storm.LocalCluster;
import org.junit.Test;

public class AllGroupingTopologyTest {
    @Test
    public void test() {
        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("All-Grouping-Topology", AllGroupingTopology.createConfig(), AllGroupingTopology.createTopology());
            Thread.sleep(15000);
        }
        catch (InterruptedException e) {
            System.out.println(e.getMessage());
        }
        finally {
            cluster.shutdown();
        }
    }

}
