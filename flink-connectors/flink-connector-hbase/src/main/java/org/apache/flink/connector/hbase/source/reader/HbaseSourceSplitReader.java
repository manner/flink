package org.apache.flink.connector.hbase.source.reader;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.hbase.source.split.HbaseSourceSplit;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * A {@link SplitReader} implementation for Hbase.
 */
public class HbaseSourceSplitReader<T> implements SplitReader<Tuple3<T, Long, Long>, HbaseSourceSplit> {
	private List<Tuple3<T, Long, Long>> records;

	public HbaseSourceSplitReader() {
		records = Arrays.asList(new Tuple3((T) "a", 1, 1), new Tuple3((T) "a", 1, 1));
	}

	@Override
	public RecordsWithSplitIds<Tuple3<T, Long, Long>> fetch() throws IOException {
		System.out.println("fetching in Split Reader");
		HbaseSplitRecords<Tuple3<T, Long, Long>> records = new HbaseSplitRecords<>(this.records);
		return records;
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

	private static class HbaseSplitRecords<T> implements RecordsWithSplitIds<T> {
		private final Collection<T> records;

		private HbaseSplitRecords(Collection<T> records) {
			this.records = records;
		}

		@Nullable
		@Override
		public String nextSplit() {
			return null;
		}

		@Nullable
		@Override
		public T nextRecordFromSplit() {
			if (records.iterator().hasNext()) {
				return null;
			} else {
				return records.iterator().next();
			}
		}

		@Override
		public Set<String> finishedSplits() {
			return null;
		}
	}

}
