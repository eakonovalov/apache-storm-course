package com.eakonovalov.storm.ex2;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.junit.Test;

public class FileReaderTopologyTest {

    @Test
    public void test() {
        Config config = new Config();
        config.setDebug(true);
        config.put("file", "src/test/resources/com/eakonovalov/storm/ex2/words.txt");

        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("File-Reader-Topology", config, FileReaderTopology.createTopology());
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
