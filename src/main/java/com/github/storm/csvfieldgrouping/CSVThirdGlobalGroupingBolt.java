package com.github.storm.csvfieldgrouping;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class CSVThirdGlobalGroupingBolt extends BaseBasicBolt {

	private final static Logger logger = LoggerFactory.getLogger(CSVThirdGlobalGroupingBolt.class);

	private static final long serialVersionUID = 1L;
	Set<String> uniquePogIds = new LinkedHashSet<String>();

	private List<Integer> numOfTasks;

	public void execute(Tuple input, BasicOutputCollector collector) {
		if (input.getFields().contains("pogid")) {
			uniquePogIds.add((String) input.getValueByField("pogid"));
		}
		if (uniquePogIds.size() == 50) {
			logger.info(Thread.currentThread().getName() + "~~~~~~~~~####" + uniquePogIds);
			uniquePogIds.clear();
		}
		collector.emitDirect(this.numOfTasks.get(new Random().nextInt(this.numOfTasks.size())), new Values((String) input.getValueByField("pogid")));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("third-csv-bolt-stream", true, new Fields("pogid"));
	}

	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context) {
		this.numOfTasks = context.getComponentTasks("csv-third-bolt");
	}

}
