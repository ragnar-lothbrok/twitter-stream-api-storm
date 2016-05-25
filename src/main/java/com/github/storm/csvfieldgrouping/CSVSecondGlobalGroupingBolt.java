package com.github.storm.csvfieldgrouping;

import java.util.LinkedHashSet;
import java.util.Set;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class CSVSecondGlobalGroupingBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 1L;

	Set<String> uniquePogIds = new LinkedHashSet<String>();

	public void execute(Tuple input, BasicOutputCollector collector) {
		if (input.getFields().contains("pogid")) {
			uniquePogIds.add((String) input.getValueByField("pogid"));
			collector.emit("second-csv-bolt-stream",new Values((String) input.getValueByField("pogid")));
		}
		if (uniquePogIds.size() == 50) {
			System.out.println(Thread.currentThread().getName() + "####" + uniquePogIds);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("second-csv-bolt-stream", new Fields("pogid"));
	}

}
