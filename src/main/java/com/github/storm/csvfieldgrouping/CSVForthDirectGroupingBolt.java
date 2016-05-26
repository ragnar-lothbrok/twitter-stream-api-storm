package com.github.storm.csvfieldgrouping;

import java.util.LinkedHashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class CSVForthDirectGroupingBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 1L;
	private final static Logger logger = LoggerFactory.getLogger(CSVForthDirectGroupingBolt.class);
	Set<String> uniquePogIds = new LinkedHashSet<String>();

	public void execute(Tuple input, BasicOutputCollector collector) {
		if (input.getFields().contains("pogid")) {
			uniquePogIds.add((String) input.getValueByField("pogid"));
		}
		if (uniquePogIds.size() == 50) {
			logger.info(Thread.currentThread().getName() + "@@@@@@@@####" + uniquePogIds);
			uniquePogIds.clear();
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

}
