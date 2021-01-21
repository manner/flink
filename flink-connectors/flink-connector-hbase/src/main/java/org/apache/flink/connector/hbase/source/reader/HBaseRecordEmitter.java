package org.apache.flink.connector.hbase.source.reader;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.hbase.source.split.HBaseSourceSplitState;

/** The {@link RecordEmitter} implementation for {@link HBaseSourceReader}. */
public class HBaseRecordEmitter<T> implements RecordEmitter<T, T, HBaseSourceSplitState> {

    @Override
    public void emitRecord(
            T element, SourceOutput<T> output, HBaseSourceSplitState splitState) {
        output.collect(element);
    }
}
