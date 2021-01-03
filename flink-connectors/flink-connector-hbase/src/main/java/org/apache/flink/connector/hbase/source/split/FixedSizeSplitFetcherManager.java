package org.apache.flink.connector.hbase.source.split;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import java.util.function.Supplier;

/**
 * A SplitFetcherManager that has a fixed size of split fetchers and assign splits
 * to the split fetchers based on the hash code of split IDs.
 */
public class FixedSizeSplitFetcherManager<T>
	extends SingleThreadFetcherManager<Tuple3<T, Long, Long>, HbaseSourceSplit> {

	public FixedSizeSplitFetcherManager(
		FutureCompletingBlockingQueue<RecordsWithSplitIds<Tuple3<T, Long, Long>>> elementsQueue,
		Supplier<SplitReader<Tuple3<T, Long, Long>, HbaseSourceSplit>> splitReaderSupplier) {
		super(elementsQueue, splitReaderSupplier);
	}
}
