package com.github.storm;

import java.util.List;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class EmailExtractorBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 1L;

	public void execute(Tuple tuple, BasicOutputCollector outputCollector) {
		List<Object> tupleList = tuple.getValues();
		GitCommit gitCommit = null;
		for (Object obj : tupleList) {
			gitCommit = (GitCommit) obj;
			outputCollector.emit("commit-count-stream", new Values(gitCommit.getEmailId()));
			outputCollector.emit("commit-detail-count-stream", new Values(gitCommit));
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer outputCollector) {
		outputCollector.declareStream("commit-count-stream", new Fields("email"));
		outputCollector.declareStream("commit-detail-count-stream", new Fields("email"));
	}

}
