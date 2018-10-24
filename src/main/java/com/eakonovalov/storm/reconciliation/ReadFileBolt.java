package com.eakonovalov.storm.reconciliation;

import com.eakonovalov.storm.reconciliation.beans.ColumnType;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import javax.sql.DataSource;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * Created by ekonovalov on 2018-10-18.
 */
public class ReadFileBolt extends BaseBasicBolt {

    private static final long serialVersionUID = -2032033332184262650L;

    private DataSource ds;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        ds = ConnectionPool.getInstance().getDataSource();
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String fileType = input.getString(1);
        String fileName = input.getString(2);
        Integer columnSetId = input.getInteger(3);

        Set<Integer> keyColumns = new HashSet<>();
        Set<Integer> valueColumns = new HashSet<>();
        try (Connection con = ds.getConnection();
             PreparedStatement pstmt = con.prepareStatement(Query.GET_COLUMN_SET)) {

            pstmt.setInt(1, columnSetId);
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    int columnType = rs.getInt("type");
                    if (columnType == ColumnType.KEY.getCode()) {
                        keyColumns.add(rs.getInt("no"));
                    } else if (columnType == ColumnType.VALUE.getCode()) {
                        valueColumns.add(rs.getInt("no"));
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Error connecting to database [" + fileName + "]", e);
        }

        try (Reader in = new FileReader(fileName)) {
            Iterable<CSVRecord> records = CSVFormat.EXCEL.withFirstRecordAsHeader().parse(in);
            for (CSVRecord record : records) {
                List<String> keys = new ArrayList<>();
                List<String> values = new ArrayList<>();
                for (Integer i : keyColumns) {
                    keys.add(record.get(i));
                }
                for (Integer i : valueColumns) {
                    values.add(record.get(i));
                }
                collector.emit(new Values(input.getLong(0), fileType, fileName, keys, values));
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
