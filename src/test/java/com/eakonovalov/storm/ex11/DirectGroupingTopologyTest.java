package com.eakonovalov.storm.ex11;

import com.eakonovalov.storm.ex10.DirectGroupingTopology;
import org.apache.storm.LocalCluster;
import org.junit.Test;

public class DirectGroupingTopologyTest {
    @Test
    public void test() {
        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("Direct-Grouping-Topology", DirectGroupingTopology.createConfig(),
                    DirectGroupingTopology.createTopology());
            Thread.sleep(15000);
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
        } finally {
            cluster.shutdown();
        }
    }

}
