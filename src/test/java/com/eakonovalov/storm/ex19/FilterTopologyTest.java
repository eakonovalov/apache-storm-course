package com.eakonovalov.storm.ex19;

import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.junit.Test;

public class FilterTopologyTest {

    @Test
    public void test() {
        LocalDRPC drpc = new LocalDRPC();
        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("Filter-Topology", FilterTopology.createConfig(), FilterTopology.createTopology(drpc));

        for (String sentence : new String[]{"First Page", "Second Line", "Third word in the Book"}) {
            System.out.println("Result for '" + sentence + "' : " + drpc.execute("Filter", sentence));
        }

        cluster.shutdown();
        drpc.shutdown();

    }

}
