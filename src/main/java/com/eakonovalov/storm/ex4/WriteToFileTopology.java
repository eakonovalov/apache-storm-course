package com.eakonovalov.storm.ex4;

import com.eakonovalov.storm.ex3.ReadFieldsSpout;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

public class WriteToFileTopology {

    public static StormTopology createTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("Read-Fields-Spout", new ReadFieldsSpout());
        builder.setBolt("Write-To-File-Bolt", new WriteToFileBolt()).shuffleGrouping("Read-Fields-Spout");

        return builder.createTopology();
    }

}
