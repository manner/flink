package org.apache.flink.connector.hbase.source.reader;

import org.apache.hadoop.hbase.Cell;

/** HBaseEvent. */
public class HBaseEvent {
    private final Cell.Type type;
    private final String rowId;
    private final String table;
    private final String cf;
    private final String qualifier;
    private final byte[] payload;
    private final long timestamp;
    private final long offset;

    public HBaseEvent(
            Cell.Type type,
            String rowId,
            String table,
            String cf,
            String qualifier,
            byte[] payload,
            long timestamp,
            long offset) {
        this.type = type;
        this.rowId = rowId;
        this.table = table;
        this.cf = cf;
        this.qualifier = qualifier;
        this.payload = payload;
        this.timestamp = timestamp;
        this.offset = offset;
    }

    public byte[] getPayload() {
        return payload;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String toString() {
        return type.name()
                + " "
                + table
                + " "
                + rowId
                + " "
                + cf
                + " "
                + qualifier
                + " "
                + new String(payload)
                + " "
                + timestamp
                + " "
                + offset;
    }
}
