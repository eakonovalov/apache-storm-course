package com.eakonovalov.storm.reconciliation;

import org.apache.log4j.PropertyConfigurator;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.junit.Test;

public class ReconciliationTopologyTest {

    @Test
    public void test() {
        LocalDRPC drpc = new LocalDRPC();
        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("Reconciliation-Topology", ReconciliationTopology.createConfig(), ReconciliationTopology.createTopology(drpc));

            System.out.println("=============================================================================> " + drpc.execute("Reconciliation-Function", "src/test/resources/com/eakonovalov/storm/reconciliation/20101104.csv;src/test/resources/com/eakonovalov/storm/reconciliation/20101108.csv"));

            Thread.sleep(15000000);
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
        } finally {
            cluster.shutdown();
            drpc.shutdown();
        }
    }

}
