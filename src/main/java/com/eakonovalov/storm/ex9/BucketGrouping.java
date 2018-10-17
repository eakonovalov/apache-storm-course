package com.eakonovalov.storm.ex9;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class BucketGrouping implements CustomStreamGrouping, Serializable {

    private List<Integer> targetTasks;

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        this.targetTasks = targetTasks;
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        List<Integer> boltIds = new ArrayList<>();
        int boltNum = Integer.parseInt(String.valueOf(values.get(1))) % targetTasks.size();
        boltIds.add(targetTasks.get(boltNum));

        return boltIds;
    }

}
