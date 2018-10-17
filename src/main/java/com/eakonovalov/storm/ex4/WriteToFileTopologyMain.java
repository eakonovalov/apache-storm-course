package com.eakonovalov.storm.ex4;

import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;

/**
 * Created by ekonovalov on 2018-10-17.
 */
public class WriteToFileTopologyMain {

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        StormSubmitter.submitTopology("Write-To-File-Topology", WriteToFileTopology.createConfig(), WriteToFileTopology.createTopology());
    }

}
