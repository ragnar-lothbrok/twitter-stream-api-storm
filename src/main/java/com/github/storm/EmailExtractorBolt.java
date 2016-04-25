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
			outputCollector.emit("count-stream-bolt", new Values(gitCommit.getEmailId()));
//			outputCollector.emit("map-commit-stream", new Values(gitCommit));
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer outputCollector) {
		outputCollector.declareStream("count-stream-bolt", new Fields("email"));
//		outputCollector.declareStream("map-commit-stream", new Fields("email"));
	}

}
