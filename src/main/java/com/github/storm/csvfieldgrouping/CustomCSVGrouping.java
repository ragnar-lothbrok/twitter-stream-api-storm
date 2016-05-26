package com.github.storm.csvfieldgrouping;

import java.io.Serializable;
import java.util.List;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;

public class CustomCSVGrouping implements CustomStreamGrouping, Serializable {

	private static final long serialVersionUID = 1L;
	private int tasks = 0;

	public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
		 tasks = targetTasks.size();
	}

	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		// TODO Auto-generated method stub
		return null;
	}

}
