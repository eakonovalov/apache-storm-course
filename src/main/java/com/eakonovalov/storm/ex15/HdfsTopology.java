package com.eakonovalov.storm.ex15;

import com.eakonovalov.storm.ex3.FilterFieldsBolt;
import com.eakonovalov.storm.ex3.ReadFieldsSpout;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.topology.TopologyBuilder;

public class HdfsTopology {

    public static StormTopology createTopology() {
        HdfsBolt hdfsBolt = new HdfsBolt().withFsUrl("hdfs://192.168.99.100:50070")
                .withFileNameFormat(new DefaultFileNameFormat()
                        .withPath("/storm")
                        .withPrefix("records-")
                        .withExtension(".csv"))
                .withRecordFormat(new DelimitedRecordFormat()
                        .withFieldDelimiter(","))
                .withRotationPolicy(new TimedRotationPolicy(10, TimedRotationPolicy.TimeUnit.SECONDS))
                .withSyncPolicy(new CountSyncPolicy(1));

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("Read-Fields-Spout", new ReadFieldsSpout());
        builder.setBolt("Filter-Fields-Bolt", new FilterFieldsBolt()).shuffleGrouping("Read-Fields-Spout");
        builder.setBolt("Hdfs-Bolt", hdfsBolt).shuffleGrouping("Filter-Fields-Bolt");

        return builder.createTopology();
    }

    public static Config createConfig() {
        Config config = new Config();
        config.setDebug(true);
        config.put("file", "src/test/resources/com/eakonovalov/storm/ex3/list.txt");

        return config;
    }

}
