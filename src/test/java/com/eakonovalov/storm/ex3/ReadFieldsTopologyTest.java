package com.eakonovalov.storm.ex3;

import org.apache.storm.LocalCluster;
import org.junit.Test;

public class ReadFieldsTopologyTest {

    @Test
    public void test() {
        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("Read-Fields-Topology", ReadFieldsTopology.createConfig(), ReadFieldsTopology.createTopology());
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
        } finally {
            cluster.shutdown();
        }
    }

}
