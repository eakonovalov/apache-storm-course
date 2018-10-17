package com.eakonovalov.storm.ex7;

import org.apache.storm.LocalCluster;
import org.junit.Test;

public class FieldsGroupingTopologyTest {
    @Test
    public void test() {
        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("Fields-Grouping-Topology", FieldsGroupingTopology.createConfig(),
                    FieldsGroupingTopology.createTopology());
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
