package org.apache.flink.connector.hbase.source.reader;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.hbase.source.split.FixedSizeSplitFetcherManager;
import org.apache.flink.connector.hbase.source.split.HbaseSourceSplit;
import org.apache.flink.connector.hbase.source.split.HbaseSourceSplitState;

import java.util.Map;
import java.util.function.Supplier;

/**
 * The source reader for Hbase.
 */
public class HbaseSourceReader<T> extends SingleThreadMultiplexSourceReaderBase<Tuple3<T, Long, Long>, T, HbaseSourceSplit, HbaseSourceSplitState> {
	public HbaseSourceReader(
		FutureCompletingBlockingQueue<RecordsWithSplitIds<Tuple3<T, Long, Long>>> elementsQueue,
		Supplier<HbaseSourceSplitReader<T>> splitReaderSupplier,
		RecordEmitter<Tuple3<T, Long, Long>, T, HbaseSourceSplitState> recordEmitter,
		Configuration config,
		SourceReaderContext context) {
		super(
			elementsQueue,
			new FixedSizeSplitFetcherManager<>(elementsQueue, splitReaderSupplier::get),
			recordEmitter,
			config,
			context);
	}

	@Override
	public void start() {
	}

	@Override
	protected void onSplitFinished(Map<String, HbaseSourceSplitState> finishedSplitIds) {
	}

	@Override
	protected HbaseSourceSplitState initializedState(HbaseSourceSplit split) {
		return null;
	}

	@Override
	protected HbaseSourceSplit toSplitType(String splitId, HbaseSourceSplitState splitState) {
		return null;
	}
}
