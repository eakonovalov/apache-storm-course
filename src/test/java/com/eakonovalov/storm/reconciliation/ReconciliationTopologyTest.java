package com.eakonovalov.storm.reconciliation;

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

            long start = System.currentTimeMillis();

            //System.out.println(drpc.execute("Reconciliation-Function", "196608;196609"));
            System.out.println(drpc.execute("Reconciliation-Function", "196610;196611"));
            System.out.println(System.currentTimeMillis() - start);
            start = System.currentTimeMillis();
            //System.out.println(drpc.execute("Reconciliation-Function", "196608;196609"));
            System.out.println(drpc.execute("Reconciliation-Function", "196610;196611"));
            System.out.println(System.currentTimeMillis() - start);
            start = System.currentTimeMillis();
            //System.out.println(drpc.execute("Reconciliation-Function", "196608;196609"));
            System.out.println(drpc.execute("Reconciliation-Function", "196610;196611"));
            System.out.println(System.currentTimeMillis() - start);
            start = System.currentTimeMillis();
            //System.out.println(drpc.execute("Reconciliation-Function", "196608;196609"));
            System.out.println(drpc.execute("Reconciliation-Function", "196610;196611"));
            System.out.println(System.currentTimeMillis() - start);
            start = System.currentTimeMillis();
            //System.out.println(drpc.execute("Reconciliation-Function", "196608;196609"));
            System.out.println(drpc.execute("Reconciliation-Function", "196610;196611"));
            System.out.println(System.currentTimeMillis() - start);
/*
            System.out.println(drpc.execute("Reconciliation-Function", "1;2"));
            System.out.println(System.currentTimeMillis() - start);
            start = System.currentTimeMillis();
            System.out.println(drpc.execute("Reconciliation-Function", "1;2"));
            System.out.println(System.currentTimeMillis() - start);
            start = System.currentTimeMillis();
            System.out.println(drpc.execute("Reconciliation-Function", "1;2"));
            System.out.println(System.currentTimeMillis() - start);
            start = System.currentTimeMillis();
            System.out.println(drpc.execute("Reconciliation-Function", "1;2"));
            System.out.println(System.currentTimeMillis() - start);
            start = System.currentTimeMillis();
            System.out.println(drpc.execute("Reconciliation-Function", "1;2"));
            System.out.println(System.currentTimeMillis() - start);
*/
            Thread.sleep(150000000);
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
        } finally {
            cluster.shutdown();
            drpc.shutdown();
        }
    }

}
