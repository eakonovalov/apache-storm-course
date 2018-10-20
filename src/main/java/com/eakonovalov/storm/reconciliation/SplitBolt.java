package com.eakonovalov.storm.reconciliation;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Map;

/**
 * Created by ekonovalov on 2018-10-18.
 */
public class SplitBolt extends BaseBasicBolt {

    private static final long serialVersionUID = 7601434584803893809L;

    private DataSource ds;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        ds = ConnectionPool.getInstance().getDataSource();
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String ids = Arrays.toString(input.getString(1).split(";"));

        try (Connection con = ds.getConnection();
             Statement stmt = con.createStatement();
             ResultSet rs = stmt.executeQuery(Query.GET_FILES.replace("${IDs}", ids.substring(1, ids.length() - 1)))) {

            while (rs.next()) {
                boolean isPreFile = rs.getBoolean("isPreFile");
                String fileName = rs.getString("fileName");
                Integer columnSetId = rs.getInt("columnSetId");
                collector.emit(new Values(input.getLong(0), isPreFile ? "PRE" : "POST", fileName, columnSetId));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Error connecting to database [" + ids + "]", e);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "fileType", "fileName", "columnSetId"));
    }

}
