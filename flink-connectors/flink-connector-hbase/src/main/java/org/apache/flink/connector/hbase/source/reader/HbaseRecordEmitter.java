package org.apache.flink.connector.hbase.source.reader;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.hbase.source.split.HbaseSourceSplit;


/**
 * The {@link RecordEmitter} implementation for {@link HbaseSourceReader}.
 */
public class HbaseRecordEmitter<T> implements RecordEmitter<Tuple3<T, Long, Long>, T, HbaseSourceSplit> {

	@Override
	public void emitRecord(
		Tuple3<T, Long, Long> element,
		SourceOutput<T> output,
		HbaseSourceSplit splitState) throws Exception {
		output.collect(element.f0, element.f2);

	}
}
