package com.github.storm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class GitCommitCountBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 1L;

	private final static Logger logger = LoggerFactory.getLogger(GitCommitCountBolt.class);

	public void execute(Tuple tuple, BasicOutputCollector outputCollector) {
		String emailId = tuple.getStringByField("email");
		if (InMemoryMap.emailCountMap.containsKey(emailId)) {
			InMemoryMap.emailCountMap.put(emailId, InMemoryMap.emailCountMap.get(emailId) + 1);
		} else {
			InMemoryMap.emailCountMap.put(emailId, 1);
		}
		logger.info("Map {} " + InMemoryMap.emailCountMap);
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {

	}

}
