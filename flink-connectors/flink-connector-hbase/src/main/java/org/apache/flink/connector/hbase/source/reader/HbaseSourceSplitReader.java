package org.apache.flink.connector.hbase.source.reader;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.hbase.source.split.HbaseSourceSplit;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Set;

/**
 * A {@link SplitReader} implementation for Hbase.
 */
public class HbaseSourceSplitReader implements SplitReader<byte[], HbaseSourceSplit> {

	private final Queue<HbaseSourceSplit> splits;
	@Nullable
	private String currentSplitId;

	public HbaseSourceSplitReader() {
		System.out.println("constructing Split Reader");
		this.splits = new ArrayDeque<>();
	}

	@Override
	public RecordsWithSplitIds<byte[]> fetch() throws IOException {
		System.out.println("fetching in Split Reader");

		final HbaseSourceSplit nextSplit = splits.poll();
		if (nextSplit == null) {
			throw new IOException("Cannot fetch from another split - no split remaining");
		}

		currentSplitId = nextSplit.splitId();

		byte[] data = "Hello World!".getBytes();
		List<byte[]> records = Arrays.asList(data, data);

		return new HbaseSplitRecords(currentSplitId, records.iterator());
	}

	@Override
	public void handleSplitsChanges(SplitsChange<HbaseSourceSplit> splitsChanges) {
		splits.addAll(splitsChanges.splits());
	}

	@Override
	public void wakeUp() {

	}

	@Override
	public void close() throws Exception {

	}

	private static class HbaseSplitRecords implements RecordsWithSplitIds<byte[]> {
		private final Set<String> finishedSplits;
		private Iterator<byte[]> recordsForSplit;

		private String splitId;

		private HbaseSplitRecords(String splitId, Iterator<byte[]> recordsForSplit) {
			this.splitId = splitId;
			this.recordsForSplit = recordsForSplit;
			this.finishedSplits = Collections.emptySet();
		}

		@Nullable
		@Override
		public String nextSplit() {
			final String nextSplit = this.splitId;
			this.splitId = null;
			this.recordsForSplit = nextSplit != null ? this.recordsForSplit : null;

			return nextSplit;
		}

		@Nullable
		@Override
		public byte[] nextRecordFromSplit() {
			if (recordsForSplit != null) {
				return recordsForSplit.next();
			} else {
				throw new IllegalStateException();
			}
		}

		@Override
		public Set<String> finishedSplits() {
			return finishedSplits;
		}
	}
}
