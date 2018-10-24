package com.eakonovalov.storm.ex21;

import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.junit.Test;

public class StateTopologyTest {

    @Test
    public void test() {
        LocalDRPC drpc = new LocalDRPC();
        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("State-Topology", StateTopology.createConfig(), StateTopology.createTopology(drpc));

        for (String word : new String[]{"cow"}) {
            System.out.println("Result for '" + word + "' : " + drpc.execute("State", word));
        }

        cluster.shutdown();
        drpc.shutdown();

    }

}
