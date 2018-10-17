package com.eakonovalov.storm.ex4;

import org.apache.storm.LocalCluster;
import org.junit.Test;

public class WriteToFileTopologyTest {

    @Test
    public void test() {
        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("Write-To-File-Topology", WriteToFileTopology.createConfig(), WriteToFileTopology.createTopology());
            Thread.sleep(5000);
        }
        catch (InterruptedException e) {
            System.out.println(e.getMessage());
        }
        finally {
            cluster.shutdown();
        }
    }

}
