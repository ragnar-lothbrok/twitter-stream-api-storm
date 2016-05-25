package com.github.storm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class GitCommitBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 1L;

	private final static Logger logger = LoggerFactory.getLogger(GitCommitBolt.class);

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Map<String, Object> conf = new HashMap<String, Object>();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 10);
		return conf;
	}

	protected static boolean isTickTuple(Tuple tuple) {
		return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
	}

	public void execute(Tuple tuple, BasicOutputCollector outputCollector) {
		if (isTickTuple(tuple)) {
			System.out.println("lollllll");
		} else {
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
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {

	}

}
