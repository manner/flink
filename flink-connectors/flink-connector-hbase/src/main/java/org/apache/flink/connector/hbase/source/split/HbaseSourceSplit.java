package org.apache.flink.connector.hbase.source.split;

import org.apache.flink.api.connector.source.SourceSplit;

/**
 * A {@link SourceSplit} for a Hbase.
 */
public class HbaseSourceSplit implements SourceSplit {
	private final String id;

	public HbaseSourceSplit(String id) {
		this.id = id;
	}

	@Override
	public String splitId() {
		return id;
	}
}
