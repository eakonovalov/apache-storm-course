package com.eakonovalov.storm.ex15;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;

public class Example {

    public static void main(String[] args) throws IOException, URISyntaxException {
        Configuration configuration = new Configuration();
        FileSystem hdfs = FileSystem.get(new URI("hdfs://192.168.99.100:8020"), configuration);
        Path file = new Path("hdfs://192.168.99.100:8020/storm/table.html");
        if (hdfs.exists(file)) {
            hdfs.delete(file, true);
        }
        OutputStream os = hdfs.create(file);
        BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os, StandardCharsets.UTF_8));
        br.write("Hello World");
        br.close();
        hdfs.close();
    }

}
