package org.apache.flink.connector.hbase.source.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.connector.hbase.source.split.HbaseSourceSplit;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

/**
 * The enumerator class for Hbase source.
 */
@Internal
public class HbaseSourceEnumerator implements SplitEnumerator<HbaseSourceSplit, HbaseSourceEnumState> {

	@Override
	public void start() {

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
