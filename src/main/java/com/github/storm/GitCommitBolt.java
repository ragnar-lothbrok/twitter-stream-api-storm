package com.github.storm;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class GitCommitBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 1L;

	private final static Logger logger = LoggerFactory.getLogger(GitCommitBolt.class);

	public void execute(Tuple tuple, BasicOutputCollector outputCollector) {
		List<Object> tupleList = tuple.getValues();
		GitCommit gitCommit = null;
		for (Object obj : tupleList) {
			gitCommit = (GitCommit) obj;
		}
		List<GitCommit> commitList = new ArrayList<GitCommit>();
		if (InMemoryMap.emailCommitMap.containsKey(gitCommit.getEmailId())) {
			InMemoryMap.emailCommitMap.get(gitCommit.getEmailId()).add(gitCommit);
		} else {
			commitList.add(gitCommit);
			InMemoryMap.emailCommitMap.put(gitCommit.getEmailId(), commitList);
		}
		logger.info("Map {} " + InMemoryMap.emailCommitMap);
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {

	}

}
