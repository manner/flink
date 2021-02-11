package org.apache.flink.connector.hbase.source.split;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.hadoop.conf.Configuration;

import java.io.Serializable;

/** A {@link SourceSplit} for a Hbase. */
public class HBaseSourceSplit implements SourceSplit, Serializable {

    private static final long serialVersionUID = 1L;

    private final String id;

    private final String host;

    private final String table;

    private final String columnFamily;

    private final Configuration hbaseConf; // TODO serialization

    private final Tuple2<Long, Integer> firstEventStamp;

    public HBaseSourceSplit(
            String id, String host, String table, String columnFamily, Configuration hbaseConf) {
        this(id, host, table, columnFamily, hbaseConf, Tuple2.of(-1L, -1));
    }

    public HBaseSourceSplit(
            String id,
            String host,
            String table,
            String columnFamily,
            Configuration hbaseConf,
            Tuple2<Long, Integer> firstEventStamp) {
        this.id = id;
        this.host = host;
        this.table = table;
        this.columnFamily = columnFamily;
        this.hbaseConf = hbaseConf;
        this.firstEventStamp = firstEventStamp;
    }

    public String getTable() {
        return table;
    }

    public String getHost() {
        return host;
    }

    public Configuration getHbaseConf() {
        return hbaseConf;
    }

    public String getColumnFamily() {
        return columnFamily;
    }

    @Override
    public String splitId() {
        return id;
    }

    @Override
    public String toString() {
        return String.format("HbaseSourceSplit: %s %s", getHost(), getTable());
    }

    public Tuple2<Long, Integer> getFirstEventStamp() {
        return firstEventStamp;
    }

    public HBaseSourceSplit withStamp(long lastTimeStamp, int lastIndex) {
        return new HBaseSourceSplit(
                id, host, table, columnFamily, hbaseConf, Tuple2.of(lastTimeStamp, lastIndex));
    }
}
