package org.apache.flink.connector.hbase.source;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.hbase.source.enumerator.HbaseSourceEnumState;
import org.apache.flink.connector.hbase.source.split.HbaseSourceSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;

/**
 * A connector for Hbase.
 */
public class HbaseSource<OUT> implements Source<OUT, HbaseSourceSplit, HbaseSourceEnumState> {

	private final Boundedness boundedness;

	public HbaseSource(Boundedness boundedness) {
		this.boundedness = boundedness;
	}

	@Override
	public Boundedness getBoundedness() {
		return boundedness;
	}

	@Override
	public SourceReader<OUT, HbaseSourceSplit> createReader(SourceReaderContext readerContext) throws Exception {
		return null;
	}

	@Override
	public SplitEnumerator<HbaseSourceSplit, HbaseSourceEnumState> restoreEnumerator(
		SplitEnumeratorContext<HbaseSourceSplit> enumContext,
		HbaseSourceEnumState checkpoint) throws Exception {
		return null;
	}

	@Override
	public SplitEnumerator<HbaseSourceSplit, HbaseSourceEnumState> createEnumerator(
		SplitEnumeratorContext<HbaseSourceSplit> enumContext) throws Exception {
		return null;
	}

	@Override
	public SimpleVersionedSerializer<HbaseSourceSplit> getSplitSerializer() {
		return null;
	}

	@Override
	public SimpleVersionedSerializer<HbaseSourceEnumState> getEnumeratorCheckpointSerializer() {
		return null;
	}
}
