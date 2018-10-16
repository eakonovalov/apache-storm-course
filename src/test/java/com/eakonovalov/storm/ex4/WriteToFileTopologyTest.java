package com.eakonovalov.storm.ex4;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.junit.Test;

import static org.junit.Assert.*;

public class WriteToFileTopologyTest {

    @Test
    public void test() {
        Config config = new Config();
        config.setDebug(true);
        config.put("file", "src/test/resources/com/eakonovalov/storm/ex3/list.txt");
        config.put("folder", "target/");

        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("Write-To-File-Topology", config, WriteToFileTopology.createTopology());
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
