package com.eakonovalov.storm.ex15;

import com.eakonovalov.storm.ex3.ReadFieldsTopology;
import org.apache.storm.LocalCluster;
import org.junit.Test;

public class HdfsTopologyTest {

    @Test
    public void test() {
        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("Hdfs-Topology", HdfsTopology.createConfig(), HdfsTopology.createTopology());
            Thread.sleep(15000);
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
        } finally {
            cluster.shutdown();
        }
    }

}
