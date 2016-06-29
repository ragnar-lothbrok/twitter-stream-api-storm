package com.github.storm.trending;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * @author This abstract bolt provides the basic behavior of bolts
 *         that rank objects according to their count.
 *         <p/>
 *         It uses a template method design pattern for
 *         {@link AbstractRankerBolt#execute(Tuple, BasicOutputCollector)} to
 *         allow actual bolt implementations to specify how incoming tuples are
 *         processed, i.e. how the objects embedded within those tuples are
 *         retrieved and counted.
 */
public abstract class AbstractRankerBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 4931640198501530202L;
	private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = 1000;
	private static final int DEFAULT_COUNT = 10;

	private final int emitFrequencyInSeconds;
	private final int count;
	protected final Rankings rankings;

	public AbstractRankerBolt() {
		this(DEFAULT_COUNT, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
	}

	public AbstractRankerBolt(int topN) {
		this(topN, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
	}

	public AbstractRankerBolt(int topN, int emitFrequencyInSeconds) {
		if (topN < 1) {
			throw new IllegalArgumentException("topN must be >= 1 (you requested " + topN + ")");
		}
		if (emitFrequencyInSeconds < 1) {
			throw new IllegalArgumentException("The emit frequency must be >= 1 seconds (you requested " + emitFrequencyInSeconds + " seconds)");
		}
		count = topN;
		this.emitFrequencyInSeconds = emitFrequencyInSeconds;
		rankings = new Rankings(count);
	}

	protected Rankings getRankings() {
		return rankings;
	}

	/**
	 * This method functions as a template method (design pattern).
	 */
	public final void execute(Tuple tuple, BasicOutputCollector collector) {
		if (tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID)) {
			getLogger().debug("Received tick tuple, triggering emit of current rankings");
			emitRankings(collector);
		} else {
			updateRankingsWithTuple(tuple);
		}
	}

	abstract void updateRankingsWithTuple(Tuple tuple);

	public void emitRankings(BasicOutputCollector collector) {
		collector.emit(new Values(rankings.copy()));
		// getLogger().info(this.getClass().getSimpleName()+" "+ rankings);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("rankings"));
	}

	public Map<String, Object> getComponentConfiguration() {
		Map<String, Object> conf = new HashMap<String, Object>();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
		return conf;
	}

	abstract Logger getLogger();
}
