package com.eakonovalov.storm.ex3;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.junit.Test;

public class ReadFieldsTopologyTest {

    @Test
    public void test() {
        Config config = new Config();
        config.setDebug(true);
        config.put("file", "src/test/resources/com/eakonovalov/storm/ex3/list.txt");

        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("Read-Fields-Topology", config, ReadFieldsTopology.createTopology());
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
