package com.github.storm;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class GitCommitFeedSpout extends BaseRichSpout {

	private final static Logger logger = LoggerFactory.getLogger(GitCommitFeedSpout.class);

	private static final long serialVersionUID = 1L;

	private SpoutOutputCollector spoutOutputCollector;
	private List<GitCommit> commits = new ArrayList<GitCommit>();

	public void nextTuple() {
		for (GitCommit gitCommit : commits) {
			spoutOutputCollector.emit(new Values(gitCommit));
		}
	}

	@SuppressWarnings({ "resource", "rawtypes" })
	public void open(Map configMap, TopologyContext context, SpoutOutputCollector collector) {
		this.spoutOutputCollector = collector;

		/**
		 * Fetching commits information from GIT
		 */

		try {
			FileRepositoryBuilder builder = new FileRepositoryBuilder();
			Repository repo = builder.setGitDir(new File("/home/raghunandangupta/mygit/services-aggregator/.git")).setMustExist(true).build();
			Git git = new Git(repo);
			if (git != null) {
				Iterable<RevCommit> log = git.log().call();
				GitCommit gitCommit = null;
				for (Iterator<RevCommit> iterator = log.iterator(); iterator.hasNext();) {
					RevCommit commit = iterator.next();
					gitCommit = new GitCommit(commit.getAuthorIdent().getEmailAddress(), commit.getId().toString(), commit.getAuthorIdent().getWhen(),
							commit.getShortMessage());
					commits.add(gitCommit);
				}
			}
		} catch (Exception exception) {
			logger.error("Exception occured {}", exception);
		}
	}

	/**
	 * Emitting commits one by one.
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("commit"));
	}

}
