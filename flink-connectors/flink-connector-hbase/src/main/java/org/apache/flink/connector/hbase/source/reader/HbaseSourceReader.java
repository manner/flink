package org.apache.flink.connector.hbase.source.reader;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherManager;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.hbase.source.split.HbaseSourceSplit;

import java.util.Map;

/**
 * The source reader for Hbase.
 */
public class HbaseSourceReader<T> extends SourceReaderBase<Tuple3<T, Long, Long>, T, HbaseSourceSplit, HbaseSourceSplit> {
	public HbaseSourceReader(
		FutureCompletingBlockingQueue<RecordsWithSplitIds<Tuple3<T, Long, Long>>> elementsQueue,
		SplitFetcherManager<Tuple3<T, Long, Long>, HbaseSourceSplit> splitFetcherManager,
		RecordEmitter<Tuple3<T, Long, Long>, T, HbaseSourceSplit> recordEmitter,
		Configuration config,
		SourceReaderContext context) {
		super(elementsQueue, splitFetcherManager, recordEmitter, config, context);
	}

	@Override
	protected void onSplitFinished(Map<String, HbaseSourceSplit> finishedSplitIds) {

	}

	@Override
	protected HbaseSourceSplit initializedState(HbaseSourceSplit split) {
		return null;
	}

	@Override
	protected HbaseSourceSplit toSplitType(String splitId, HbaseSourceSplit splitState) {
		return null;
	}
}
