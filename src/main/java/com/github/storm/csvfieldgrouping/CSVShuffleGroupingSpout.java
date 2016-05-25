package com.github.storm.csvfieldgrouping;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.csvreader.CsvReader;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class CSVShuffleGroupingSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;
	private final static Logger logger = LoggerFactory.getLogger(BaseRichSpout.class);
	List<POGSUPC> pogSupcList = new ArrayList<POGSUPC>();
	private SpoutOutputCollector spoutOutputCollector;

	@SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		try {
			this.spoutOutputCollector = collector;
			CsvReader csvReader = new CsvReader("/home/raghunandangupta/Downloads/output-in-2016-05-17.csv");
			csvReader.readHeaders();
			while (csvReader.readRecord()) {
				pogSupcList.add(new POGSUPC(csvReader.get("pog_id"), csvReader.get("supc")));
			}
		} catch (Exception exception) {
			logger.error("Exception occured " + exception.getMessage());
		}
	}

	public void nextTuple() {
		for (POGSUPC pOGSUPC : pogSupcList) {
			this.spoutOutputCollector.emit(new Values(pOGSUPC));
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("supcrecord"));
	}

}
