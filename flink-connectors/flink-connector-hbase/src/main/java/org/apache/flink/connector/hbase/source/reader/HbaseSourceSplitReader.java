package org.apache.flink.connector.hbase.source.reader;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.hbase.source.split.HbaseSourceSplit;

import java.io.IOException;

/**
 * A {@link SplitReader} implementation for Hbase.
 */
public class HbaseSourceSplitReader<T> implements SplitReader<Tuple3<T, Long, Long>, HbaseSourceSplit> {
	@Override
	public RecordsWithSplitIds<Tuple3<T, Long, Long>> fetch() throws IOException {
		return null;
	}

	@Override
	public void handleSplitsChanges(SplitsChange<HbaseSourceSplit> splitsChanges) {

	}

	@Override
	public void wakeUp() {

	}

	@Override
	public void close() throws Exception {

	}
}
