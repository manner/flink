package org.apache.flink.connector.hbase.source.reader;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.hbase.source.HBaseSource;

import java.io.IOException;

/**
 * This class wraps a {@link DeserializationSchema} so it can be used in an {@link HBaseSource} as a
 * {@link HBaseSourceDeserializer}.
 */
public class HBaseSourceDeserializerWrapper<T> implements HBaseSourceDeserializer<T> {
    DeserializationSchema<T> deserializationSchema;

    public HBaseSourceDeserializerWrapper(DeserializationSchema<T> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public T deserialize(HBaseSourceEvent event) throws IOException {
        return deserializationSchema.deserialize(event.getPayload());
    }
}
