package org.apache.flink.connector.hbase.source.reader;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.hbase.source.split.HbaseSourceSplitState;


/**
 * The {@link RecordEmitter} implementation for {@link HbaseSourceReader}.
 */
public class HbaseRecordEmitter implements RecordEmitter<Tuple3<byte[], Long, Long>, byte[], HbaseSourceSplitState> {

	@Override
	public void emitRecord(
		Tuple3<byte[], Long, Long> element,
		SourceOutput<byte[]> output,
		HbaseSourceSplitState splitState) {
		System.out.println("emitRecord");
		output.collect(element.f0, element.f2);
	}
}
