package com.eakonovalov.storm.ex11;

import com.eakonovalov.storm.ex10.DirectGroupingTopology;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;

/**
 * Created by ekonovalov on 2018-10-17.
 */
public class WordCountTopologyMain {

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        StormSubmitter.submitTopology("Word-Count-Topology", WordCountTopology.createConfig(),
                WordCountTopology.createTopology());
    }

}
