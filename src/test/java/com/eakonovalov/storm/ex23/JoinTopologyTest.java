package com.eakonovalov.storm.ex23;

import org.apache.storm.LocalCluster;
import org.junit.Test;

public class JoinTopologyTest {

    @Test
    public void test() {
        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("Join-Topology", JoinTopology.createConfig(), JoinTopology.createTopology());
            Thread.sleep(25000);
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
        } finally {
            cluster.shutdown();
        }

    }

}
