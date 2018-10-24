package com.eakonovalov.storm.ex18;

import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.junit.Test;

public class MapTopologyTest {

    @Test
    public void test() {
        LocalDRPC drpc = new LocalDRPC();
        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("Map-Topology", MapTopology.createConfig(), MapTopology.createTopology(drpc));

        for(String sentence : new String[] {"First Page", "Second Line", "Third word in the Book"}) {
            System.out.println("Result for '" + sentence + "' : " + drpc.execute("Map", sentence));
        }

        cluster.shutdown();
        drpc.shutdown();

    }

}
