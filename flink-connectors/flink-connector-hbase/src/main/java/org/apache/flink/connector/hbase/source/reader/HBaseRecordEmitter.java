package org.apache.flink.connector.hbase.source.reader;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.hbase.source.split.HBaseSourceSplitState;

/** The {@link RecordEmitter} implementation for {@link HBaseSourceReader}. */
public class HBaseRecordEmitter<T> implements RecordEmitter<HBaseEvent, T, HBaseSourceSplitState> {

    private final DeserializationSchema<T> deserializationSchema;

    public HBaseRecordEmitter(DeserializationSchema<T> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public void emitRecord(
            HBaseEvent element, SourceOutput<T> output, HBaseSourceSplitState splitState)
            throws Exception {
        T deserializedPayload = deserializationSchema.deserialize(element.getPayload());
        output.collect(deserializedPayload, element.getTimestamp());
    }
}
