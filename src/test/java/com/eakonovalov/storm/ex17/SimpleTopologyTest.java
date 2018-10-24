package com.eakonovalov.storm.ex17;

import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.junit.Test;

public class SimpleTopologyTest {

    @Test
    public void test() {
        LocalDRPC drpc = new LocalDRPC();
        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("Simple-Topology", SimpleTopology.createConfig(), SimpleTopology.createTopology(drpc));

        for (String word : new String[]{"word1", "word2", "word3"}) {
            System.out.println("Result for " + word + " : " + drpc.execute("Simple", word));
        }

        cluster.shutdown();
        drpc.shutdown();

    }

}
