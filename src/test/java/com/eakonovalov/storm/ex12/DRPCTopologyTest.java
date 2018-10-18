package com.eakonovalov.storm.ex12;

import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.junit.Test;

public class DRPCTopologyTest {

    @Test
    public void test() {
        LocalDRPC drpc = new LocalDRPC();
        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("Plus-Ten-Topology", DRPCTopology.createConfig(), DRPCTopology.createTopology(drpc));

            for(Integer i : new Integer[] {53, 62, 70}) {
                System.out.println("Result for " + i + " = " + drpc.execute("Plus-Ten-Function", String.valueOf(i)));
            }

            Thread.sleep(15000);
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
        } finally {
            cluster.shutdown();
            drpc.shutdown();
        }
    }

}
