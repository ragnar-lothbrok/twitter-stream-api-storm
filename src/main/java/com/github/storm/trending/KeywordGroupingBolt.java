package com.github.storm.trending;

import com.github.storm.GitCommit;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class KeywordGroupingBolt extends BaseBasicBolt {
	public static final String SEARCHTERM = "SEARCHTERM";
	private static final long serialVersionUID = 4931640198501530202L;
	public static final String SEARCH_KEYWORD_STREAM = "searchKeywordStream";
	public static final String ALL_EVENT_STREAM = "allEventStream";

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(SEARCHTERM, "EVENT"));
	}

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		GitCommit event = (GitCommit) tuple.getValue(0);
		collector.emit(new Values(event.getEmailId(), event));
	}
}
