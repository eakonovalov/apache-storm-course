package com.eakonovalov.storm.ex20;

import com.eakonovalov.storm.ex18.MapTopology;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.junit.Test;

import static org.junit.Assert.*;

public class AggregateTopologyTest {

    @Test
    public void test() {
        LocalDRPC drpc = new LocalDRPC();
        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("Aggregate-Topology", AggregateTopology.createConfig(), AggregateTopology.createTopology(drpc));

        for(String sentence : new String[] {"First Page", "Second Line", "Third word in the Book"}) {
            System.out.println("Result for '" + sentence + "' : " + drpc.execute("Aggregate", sentence));
        }

        cluster.shutdown();
        drpc.shutdown();

    }

}