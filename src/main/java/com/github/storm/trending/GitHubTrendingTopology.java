package com.github.storm.trending;

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
public class GitHubTrendingTopology {

	private static final int DEFAULT_WINDOW_IN_SECONDS = 60;
	private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = 20;
	private static final int TOP_N = 3;

	public static void main(String[] args) {

		String spoutId = "wordGenerator";
		String keywordBoltId = "keyword";
		String counterId = "counter";
		String intermediateRankerId = "intermediateRanker";
		String totalRankerId = "finalRanker";

		TopologyBuilder builder = new TopologyBuilder();

		Config config = new Config();
		config.setDebug(true);

		builder.setSpout(spoutId, new GitCommitFeedSpout(), 1);
		builder.setBolt(keywordBoltId, new KeywordGroupingBolt(), 1).shuffleGrouping(spoutId);
		builder.setBolt(counterId, new RollingCountBolt(DEFAULT_WINDOW_IN_SECONDS, DEFAULT_EMIT_FREQUENCY_IN_SECONDS), 4)
				.fieldsGrouping(keywordBoltId, new Fields(KeywordGroupingBolt.SEARCHTERM));
		builder.setBolt(intermediateRankerId, new IntermediateRankingsBolt(TOP_N), 4).fieldsGrouping(counterId, new Fields("obj"));
		builder.setBolt(totalRankerId, new TotalRankingsBolt(TOP_N)).globalGrouping(intermediateRankerId);

		StormTopology stormTopology = builder.createTopology();

		LocalCluster localCluster = new LocalCluster();
		localCluster.submitTopology("github-commit-count-topology", config, stormTopology);

	}
}
