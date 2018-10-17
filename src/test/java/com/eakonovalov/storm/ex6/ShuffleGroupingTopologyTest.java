package com.eakonovalov.storm.ex6;

import org.apache.storm.LocalCluster;
import org.junit.Test;

/**
 * Created by ekonovalov on 2018-10-17.
 */
public class ShuffleGroupingTopologyTest {

    @Test
    public void test() {
        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("Shuffle-Grouping-Topology", ShuffleGroupingTopology.createConfig(), ShuffleGroupingTopology.createTopology());
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
