package com.eakonovalov.storm.ex24;

import org.apache.storm.LocalCluster;
import org.junit.Test;

/**
 * Created by ekonovalov on 2018-10-24.
 */
public class HashtagTopologyTest {

    @Test
    public void test() {
        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("Hashtag-Topology", HashtagTopology.createConfig(), HashtagTopology.createTopology());
            Thread.sleep(25000);
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
        } finally {
            cluster.shutdown();
        }

    }

}
