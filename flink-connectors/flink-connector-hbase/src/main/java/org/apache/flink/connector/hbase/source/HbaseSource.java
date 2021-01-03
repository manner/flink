package org.apache.flink.connector.hbase.source;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.hbase.source.enumerator.HbaseSourceEnumState;
import org.apache.flink.connector.hbase.source.reader.HbaseRecordEmitter;
import org.apache.flink.connector.hbase.source.reader.HbaseSourceReader;
import org.apache.flink.connector.hbase.source.reader.HbaseSourceSplitReader;
import org.apache.flink.connector.hbase.source.split.HbaseSourceSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.util.function.Supplier;

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
		FutureCompletingBlockingQueue<RecordsWithSplitIds<Tuple3<OUT, Long, Long>>> elementsQueue =
			new FutureCompletingBlockingQueue<>();
		Supplier<HbaseSourceSplitReader<OUT>> splitReaderSupplier = HbaseSourceSplitReader::new;
		HbaseRecordEmitter<OUT> recordEmitter = new HbaseRecordEmitter<>();
		System.out.println("createReader");
		return new HbaseSourceReader<>(elementsQueue, splitReaderSupplier, recordEmitter,
			new Configuration(), readerContext);
	}

	@Override
	public SplitEnumerator<HbaseSourceSplit, HbaseSourceEnumState> restoreEnumerator(
		SplitEnumeratorContext<HbaseSourceSplit> enumContext,
		HbaseSourceEnumState checkpoint) throws Exception {
		System.out.println("restoreEnumerator");

		return null;
	}

	@Override
	public SplitEnumerator<HbaseSourceSplit, HbaseSourceEnumState> createEnumerator(
		SplitEnumeratorContext<HbaseSourceSplit> enumContext) throws Exception {
		System.out.println("createEnumerator");

		return null;
	}

	@Override
	public SimpleVersionedSerializer<HbaseSourceSplit> getSplitSerializer() {
		System.out.println("getSplitSerializer");

		return null;
	}

	@Override
	public SimpleVersionedSerializer<HbaseSourceEnumState> getEnumeratorCheckpointSerializer() {
		return null;
	}
}
