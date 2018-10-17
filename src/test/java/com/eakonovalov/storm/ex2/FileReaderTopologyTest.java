package com.eakonovalov.storm.ex2;

import org.apache.storm.LocalCluster;
import org.junit.Test;

public class FileReaderTopologyTest {

    @Test
    public void test() {
        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("File-Reader-Topology", FileReaderTopology.createConfig(), FileReaderTopology.createTopology());
            Thread.sleep(10000);
        }
        catch (InterruptedException e) {
            System.out.println(e.getMessage());
        }
        finally {
            cluster.shutdown();
        }
    }

}
