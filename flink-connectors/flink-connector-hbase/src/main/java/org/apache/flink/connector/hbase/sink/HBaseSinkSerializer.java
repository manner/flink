package org.apache.flink.connector.hbase.sink;

/** TODO docs. */
public interface HBaseSinkSerializer<T> {
    byte[] serializePayload(T event);

    byte[] serializeColumnFamily(T event);

    byte[] serializeQualifier(T event);

    byte[] serializeRowKey(T event);
}
