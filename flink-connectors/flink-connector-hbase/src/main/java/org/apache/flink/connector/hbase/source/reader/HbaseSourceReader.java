package org.apache.flink.connector.hbase.source.reader;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.hbase.source.split.HbaseSourceSplit;
import org.apache.flink.connector.hbase.source.split.HbaseSourceSplitState;

import java.util.Map;

/**
 * The source reader for Hbase.
 */
public class HbaseSourceReader
	extends SingleThreadMultiplexSourceReaderBase<byte[], byte[], HbaseSourceSplit, HbaseSourceSplitState> {
	public HbaseSourceReader(Configuration config, SourceReaderContext context) {
		super(
			HbaseSourceSplitReader::new,
			new HbaseRecordEmitter(),
			config,
			context);
		System.out.println("constructing in Source Reader");
	}

	@Override
	protected void onSplitFinished(Map<String, HbaseSourceSplitState> finishedSplitIds) {
		context.sendSplitRequest();
	}

	@Override
	protected HbaseSourceSplitState initializedState(HbaseSourceSplit split) {
		System.out.println("initializedState");
		return new HbaseSourceSplitState(split);
	}

	@Override
	protected HbaseSourceSplit toSplitType(String splitId, HbaseSourceSplitState splitState) {
		return splitState.getSplit();
	}
}
