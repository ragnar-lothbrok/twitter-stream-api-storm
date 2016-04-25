package com.github.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * In this we will be caculating number of commit count for any mail.
 * 
 * @author raghunandangupta
 *
 */
public class GitHubCommitTopology {

	public static void main(String[] args) {

		TopologyBuilder topologyBuilder = new TopologyBuilder();

		topologyBuilder.setSpout("commit-feed-listener", new GitCommitFeedSpout());

		topologyBuilder.setBolt("email-extractor-bolt", new EmailExtractorBolt()).shuffleGrouping("commit-feed-listener");
		topologyBuilder.setBolt("count-stream-bolt", new GitCommitCountBolt()).fieldsGrouping("email-extractor-bolt", new Fields("email"));
//		topologyBuilder.setBolt("map-commit-stream", new GitCommitBolt()).fieldsGrouping("email-extractor-bolt", new Fields("email"));

		Config config = new Config();
		config.setDebug(true);

		StormTopology stormTopology = topologyBuilder.createTopology();

		LocalCluster localCluster = new LocalCluster();
		localCluster.submitTopology("github-commit-count-topology", config, stormTopology);

	}
}
