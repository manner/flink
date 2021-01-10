package org.apache.flink.connector.hbase.source.reader;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.hbase.source.split.HbaseSourceSplitState;

import java.util.Arrays;


/**
 * The {@link RecordEmitter} implementation for {@link HbaseSourceReader}.
 */
public class HbaseRecordEmitter implements RecordEmitter<byte[], byte[], HbaseSourceSplitState> {

	@Override
	public void emitRecord(
		byte[] element,
		SourceOutput<byte[]> output,
		HbaseSourceSplitState splitState) {
		System.out.println("emitRecord: " + Arrays.toString(element));
		output.collect(element);
	}
}
