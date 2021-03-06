package com.github.storm.trending;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * @author  This bolt merges incoming {@link Rankings}.
 *         <p/>
 *         It can be used to merge intermediate rankings generated by
 *         {@link IntermediateRankingsBolt} into a final, consolidated ranking.
 *         To do so, configure this bolt with a globalGrouping on
 *         {@link IntermediateRankingsBolt}.
 */
public final class TotalRankingsBolt extends AbstractRankerBolt {

	private static final long serialVersionUID = -8447525895532302198L;
	private static final Logger LOG = Logger.getLogger(TotalRankingsBolt.class);
	public static final Integer timeout = 100;

	public TotalRankingsBolt() {
		super();
	}

	public TotalRankingsBolt(int topN) {
		super(topN);
	}

	public TotalRankingsBolt(int topN, int emitFrequencyInSeconds) {
		super(topN, emitFrequencyInSeconds);
	}

	@Override
	void updateRankingsWithTuple(Tuple tuple) {
		Rankings rankingsToBeMerged = (Rankings) tuple.getValue(0);
		super.getRankings().updateWith(rankingsToBeMerged);
		super.getRankings().pruneZeroCounts();
	}

	public void emitRankings(BasicOutputCollector collector) {
		collector.emit(new Values(rankings.copy()));
		getLogger().info(this.getClass().getSimpleName() + " " + rankings);
		sendPost();
	}

	private void sendPost() {
		try {
			if (this.rankings != null && this.rankings.getRankings().size() > 0) {
				List<Rankable> rankables = this.rankings.getRankings();
				Iterator<Rankable> iterator = rankables.iterator();
				Map<String, Long> dataMap = new HashMap<String, Long>();
				while (iterator.hasNext()) {
					RankableObjectWithFields obj = (RankableObjectWithFields) iterator.next();
					dataMap.put(obj.getObject().toString(), obj.getCount());
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	Logger getLogger() {
		return LOG;
	}

}
