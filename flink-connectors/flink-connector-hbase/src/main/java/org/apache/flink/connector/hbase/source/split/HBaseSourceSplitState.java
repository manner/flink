package org.apache.flink.connector.hbase.source.split;

/** Blah. */
public class HBaseSourceSplitState {
    private final HBaseSourceSplit split;

    public HBaseSourceSplitState(HBaseSourceSplit split) {
        this.split = split;
    }

    public HBaseSourceSplit getSplit() {
        return split;
    }
}
