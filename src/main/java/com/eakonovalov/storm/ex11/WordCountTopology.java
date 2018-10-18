package com.eakonovalov.storm.ex11;

import com.eakonovalov.storm.ex2.FileReaderSpout;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class WordCountTopology {

    public static StormTopology createTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("File-Reader-Spout", new FileReaderSpout());
        builder.setBolt("Word-Normalizer", new WordNormalizer()).shuffleGrouping("File-Reader-Spout");
        builder.setBolt("Word-Count", new WordCount(), 2)
                .fieldsGrouping("Word-Normalizer", new Fields("word"));

        return builder.createTopology();
    }

    public static Config createConfig() {
        Config config = new Config();
        config.setDebug(true);
        config.put("file", "src/test/resources/com/eakonovalov/storm/ex11/words.txt");
        config.put("folder", "target/");

        return config;
    }

}
