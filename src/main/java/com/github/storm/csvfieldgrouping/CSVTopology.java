package com.github.storm.csvfieldgrouping;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class CSVTopology {

	public static void main(String[] args) {

		CSVSpout csvSpout = new CSVSpout();
		CSVFirstBolt csvFirstBolt = new CSVFirstBolt();
		CSVSecondBolt csvSecondBolt = new CSVSecondBolt();

		TopologyBuilder topologyBuilder = new TopologyBuilder();
		topologyBuilder.setSpout("csv-spout", csvSpout);
		topologyBuilder.setBolt("csv-first-bolt", csvFirstBolt,2).shuffleGrouping("csv-spout");
		topologyBuilder.setBolt("csv-second-bolt", csvSecondBolt,4).fieldsGrouping("csv-first-bolt","emit-pogid-stream", new Fields("pogid"));

		Config config = new Config();
		config.setDebug(true);
		config.put(Config.TOPOLOGY_DEBUG, false);

		StormTopology stormTopology = topologyBuilder.createTopology();

		LocalCluster localCluster = new LocalCluster();
		localCluster.submitTopology("csv-topology", config, stormTopology);

	}
}
