package com.github.storm;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public interface InMemoryMap {
	Map<String, Integer> emailCountMap = new LinkedHashMap<String, Integer>();
	Map<String, List<GitCommit>> emailCommitMap = new LinkedHashMap<String, List<GitCommit>>();
}
