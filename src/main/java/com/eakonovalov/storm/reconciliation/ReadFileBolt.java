package com.eakonovalov.storm.reconciliation;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

/**
 * Created by ekonovalov on 2018-10-18.
 */
public class ReadFileBolt extends BaseBasicBolt {

    private static final long serialVersionUID = -2032033332184262650L;

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        Long id = (Long) input.getValue(0);
        String fileType = input.getString(1);
        String fileName = input.getString(2);

        try (Reader in = new FileReader(fileName)) {
            Iterable<CSVRecord> records = CSVFormat.EXCEL.withFirstRecordAsHeader().parse(in);
            for (CSVRecord record : records) {
                StringBuilder keys = new StringBuilder();
                StringBuilder values = new StringBuilder();
                for(int i = 0; i < 9; i++) {
                    keys.append(record.get(i)).append(",");
                }
                keys.deleteCharAt(keys.length() - 1);
                for(int i = 9; i < 68; i++) {
                    values.append(record.get(i)).append(",");
                }
                values.deleteCharAt(keys.length() - 1);
                collector.emit(new Values(id, fileType, fileName, keys.toString(), values.toString()));
            }
        } catch (IOException e) {
            throw new RuntimeException("Error reading file [" + fileName + "]", e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "fileType", "fileName", "keys", "values"));
    }

}
