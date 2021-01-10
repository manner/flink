package org.apache.flink.connector.hbase.source;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.hbase.source.enumerator.HbaseSourceEnumerator;
import org.apache.flink.connector.hbase.source.reader.HbaseSourceReader;
import org.apache.flink.connector.hbase.source.split.HbaseSourceSplit;
import org.apache.flink.connector.hbase.source.split.HbaseSourceSplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A connector for Hbase.
 */
public class HbaseSource implements Source<byte[], HbaseSourceSplit, Collection<HbaseSourceSplit>> {

	private final Boundedness boundedness;

	public HbaseSource(Boundedness boundedness) {
		this.boundedness = boundedness;
	}

	@Override
	public Boundedness getBoundedness() {
		return boundedness;
	}

	@Override
	public SourceReader<byte[], HbaseSourceSplit> createReader(SourceReaderContext readerContext) throws Exception {
		System.out.println("createReader");
		return new HbaseSourceReader(new Configuration(), readerContext);
	}

	@Override
	public SplitEnumerator<HbaseSourceSplit, Collection<HbaseSourceSplit>> restoreEnumerator(
		SplitEnumeratorContext<HbaseSourceSplit> enumContext,
		Collection<HbaseSourceSplit> checkpoint) throws Exception {
		System.out.println("restoreEnumerator");

		return null;
	}

	@Override
	public SplitEnumerator<HbaseSourceSplit, Collection<HbaseSourceSplit>> createEnumerator(
		SplitEnumeratorContext<HbaseSourceSplit> enumContext) throws Exception {
		System.out.println("createEnumerator");
		List<HbaseSourceSplit> splits = new ArrayList<>();
		splits.add(new HbaseSourceSplit(
			"1234",
			"localhost",
			"test",
			new org.apache.hadoop.conf.Configuration()));
		return new HbaseSourceEnumerator(enumContext, splits);
	}

	@Override
	public SimpleVersionedSerializer<HbaseSourceSplit> getSplitSerializer() {
		System.out.println("getSplitSerializer");

		return new HbaseSourceSplitSerializer();
	}

	@Override
	public SimpleVersionedSerializer<Collection<HbaseSourceSplit>> getEnumeratorCheckpointSerializer() {
		System.out.println("getEnumeratorCheckpointSerializer");

		return null;
	}
}
