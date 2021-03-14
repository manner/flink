package org.apache.flink.connector.hbase.source.reader;

import org.apache.flink.connector.hbase.HBaseEvent;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;

/** The HBaseSourceEvent is used to represent incoming events from HBase. */
public class HBaseSourceEvent extends HBaseEvent {

    private final String table;
    private final long timestamp;
    /** Index of operation inside one wal entry. */
    private final int index;

    public HBaseSourceEvent(
            Cell.Type type,
            String rowId,
            String table,
            String cf,
            String qualifier,
            byte[] payload,
            long timestamp,
            int index) {
        super(type, rowId, cf, qualifier, payload);
        this.table = table;
        this.timestamp = timestamp;
        this.index = index;
    }

    public static HBaseSourceEvent fromCell(String table, Cell cell, int index) {
        final String row = new String(CellUtil.cloneRow(cell), CHARSET);
        final String cf = new String(CellUtil.cloneFamily(cell), CHARSET);
        final String qualifier = new String(CellUtil.cloneQualifier(cell), CHARSET);
        final byte[] payload = CellUtil.cloneValue(cell);
        final long timestamp = cell.getTimestamp();
        final Cell.Type type = cell.getType();
        return new HBaseSourceEvent(type, row, table, cf, qualifier, payload, timestamp, index);
    }

    public long getTimestamp() {
        return timestamp;
    }

    public int getIndex() {
        return index;
    }

    public String getTable() {
        return table;
    }

    @Override
    public String toString() {
        return table + " " + super.toString() + " " + timestamp + " " + index;
    }

    public boolean isLaterThan(long timestamp, int index) {
        return timestamp < this.getTimestamp()
                || (timestamp == this.getTimestamp() && index < this.getIndex());
    }
}
