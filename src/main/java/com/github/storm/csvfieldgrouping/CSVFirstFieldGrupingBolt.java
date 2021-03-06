package com.github.storm.csvfieldgrouping;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class CSVFirstFieldGrupingBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 1L;
	private final static Logger logger = LoggerFactory.getLogger(CSVFirstFieldGrupingBolt.class);

	public void execute(Tuple input, BasicOutputCollector collector) {
		try {
			if (input.getFields().size() > 0 && input.getFields().contains("supcrecord") && input.getValueByField("supcrecord") != null) {
				POGSUPC pogsupc = (POGSUPC) input.getValueByField("supcrecord");
				collector.emit("first-csv-bolt-stream",new Values(pogsupc.getPog()));
			}
		} catch (Exception exception) {
			logger.error("pogsupc :>> " + exception.getMessage());
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("first-csv-bolt-stream", new Fields("pogid"));
	}

}
