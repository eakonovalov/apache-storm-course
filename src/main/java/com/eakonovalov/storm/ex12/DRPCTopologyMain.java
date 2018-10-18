package com.eakonovalov.storm.ex12;

import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;

/**
 * Created by ekonovalov on 2018-10-17.
 */
public class DRPCTopologyMain {

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        LocalDRPC drpc = new LocalDRPC();
        StormSubmitter.submitTopology("Plus-Ten-Topology", DRPCTopology.createConfig(),
                DRPCTopology.createTopology(drpc));
    }

}
