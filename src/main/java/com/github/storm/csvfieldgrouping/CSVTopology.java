package com.github.storm.csvfieldgrouping;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class CSVTopology {

	public static void main(String[] args) {

		CSVShuffleGroupingSpout csvSpout = new CSVShuffleGroupingSpout();
		CSVFirstFieldGrupingBolt cSVFirstFieldGrupingBolt = new CSVFirstFieldGrupingBolt();
		CSVSecondGlobalGroupingBolt cSVSecondGlobalGroupingBolt = new CSVSecondGlobalGroupingBolt();
		CSVThirdGlobalGroupingBolt cSVThirdGlobalGroupingBolt = new CSVThirdGlobalGroupingBolt();

		TopologyBuilder topologyBuilder = new TopologyBuilder();
		topologyBuilder.setSpout("csv-spout", csvSpout);
		topologyBuilder.setBolt("csv-first-bolt", cSVFirstFieldGrupingBolt, 2).shuffleGrouping("csv-spout");
		topologyBuilder.setBolt("csv-second-bolt", cSVSecondGlobalGroupingBolt, 4).fieldsGrouping("csv-first-bolt", "first-csv-bolt-stream",
				new Fields("pogid"));
		topologyBuilder.setBolt("csv-third-bolt", cSVThirdGlobalGroupingBolt, 4).globalGrouping("csv-second-bolt", "second-csv-bolt-stream");

		Config config = new Config();
		config.setDebug(true);
		config.put(Config.TOPOLOGY_DEBUG, false);

		StormTopology stormTopology = topologyBuilder.createTopology();

		LocalCluster localCluster = new LocalCluster();
		localCluster.submitTopology("csv-topology", config, stormTopology);

	}
}
