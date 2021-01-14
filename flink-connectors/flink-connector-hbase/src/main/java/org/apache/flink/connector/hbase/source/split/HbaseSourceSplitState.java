package org.apache.flink.connector.hbase.source.split;

/** Blah. */
public class HbaseSourceSplitState {
    private final HbaseSourceSplit split;

    public HbaseSourceSplitState(HbaseSourceSplit split) {
        this.split = split;
    }

    public HbaseSourceSplit getSplit() {
        return split;
    }
}
