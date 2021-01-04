package org.apache.flink.connector.hbase.source.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.hbase.source.split.HbaseSourceSplit;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * The enumerator class for Hbase source.
 */
@Internal
public class HbaseSourceEnumerator implements SplitEnumerator<HbaseSourceSplit, HbaseSourceEnumState> {
	private final SplitEnumeratorContext<HbaseSourceSplit> context;

	public HbaseSourceEnumerator(SplitEnumeratorContext<HbaseSourceSplit> context) {
		this.context = context;
	}

	@Override
	public void start() {
		System.out.println("Starting HbaseSourceEnumerator");
		context.callAsync(
			this::discoverStuff,
			this::handleStuff
		);
	}

	private List<HbaseSourceSplit> discoverStuff() {
		System.out.println("discoverStuff");
		return Arrays.asList(new HbaseSourceSplit("1"));
	}

	private void handleStuff(List<HbaseSourceSplit> splits, Throwable t) {
		System.out.println("handleStuff");
		for (HbaseSourceSplit split : splits) {
			context.assignSplit(split, 0);
		}
	}

	@Override
	public void handleSplitRequest(
		int subtaskId,
		@Nullable String requesterHostname) {

	}

	@Override
	public void addSplitsBack(List<HbaseSourceSplit> splits, int subtaskId) {

	}

	@Override
	public void addReader(int subtaskId) {

	}

	@Override
	public HbaseSourceEnumState snapshotState() throws Exception {
		return null;
	}

	@Override
	public void close() throws IOException {

	}
}
