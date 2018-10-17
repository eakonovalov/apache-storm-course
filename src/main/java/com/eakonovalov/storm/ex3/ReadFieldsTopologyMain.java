package com.eakonovalov.storm.ex3;

import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;

/**
 * Created by ekonovalov on 2018-10-17.
 */
public class ReadFieldsTopologyMain {

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        StormSubmitter.submitTopology("Read-Fields-Topology", ReadFieldsTopology.createConfig(), ReadFieldsTopology.createTopology());
    }

}
