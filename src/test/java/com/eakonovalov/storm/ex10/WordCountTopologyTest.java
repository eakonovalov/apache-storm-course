package com.eakonovalov.storm.ex10;

import com.eakonovalov.storm.ex11.WordCountTopology;
import org.apache.storm.LocalCluster;
import org.junit.Test;

public class WordCountTopologyTest {
    @Test
    public void test() {
        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("Word-Count-Topology", WordCountTopology.createConfig(),
                    WordCountTopology.createTopology());
            Thread.sleep(15000);
        }
        catch (InterruptedException e) {
            System.out.println(e.getMessage());
        }
        finally {
            cluster.shutdown();
        }
    }

}
