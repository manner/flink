package org.apache.flink.connector.hbase.source.split;

import org.apache.flink.api.connector.source.SourceSplit;

import org.apache.hadoop.conf.Configuration;

import java.io.Serializable;

/**
 * A {@link SourceSplit} for a Hbase.
 */
public class HbaseSourceSplit implements SourceSplit, Serializable {

	private static final long serialVersionUID = 1L;

	private final String id;

	private final String table;

	private final String host;

	private final Configuration hbaseConf;

	public HbaseSourceSplit(String id, String host, String table, Configuration hbaseConf) {
		this.id = id;
		this.host = host;
		this.table = table;
		this.hbaseConf = hbaseConf;
	}

	public String getTable() {
		return table;
	}

	public String getHost() {
		return host;
	}

	public Configuration getHbaseConf() {
		return hbaseConf;
	}

	@Override
	public String splitId() {
		return id;
	}

	@Override
	public String toString() {
		return String.format("HbaseSourceSplit: %s %s", getHost(), getTable());
	}
}
