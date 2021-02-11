package org.apache.flink.connector.hbase.source.split;

import org.apache.flink.connector.hbase.source.reader.HBaseEvent;

/**
 * State of source split. Tracks the stamp of the last emitted event to ensure no duplicates on
 * recovery
 */
public class HBaseSourceSplitState {
    private final HBaseSourceSplit split;

    private long lastTimeStamp = -1;
    private int lastIndex = -1;

    public HBaseSourceSplitState(HBaseSourceSplit split) {
        this.split = split;
        this.lastTimeStamp = this.split.getFirstEventStamp().f0;
        this.lastIndex = this.split.getFirstEventStamp().f1;
    }

    public HBaseSourceSplit toSplit() {
        return split.withStamp(lastTimeStamp, lastIndex);
    }

    /**
     * Update the state of which element was emitted last.
     *
     * @param event An {@link HBaseEvent} that has been emitted by the {@link
     *     org.apache.flink.connector.hbase.source.reader.HBaseRecordEmitter}
     */
    public void notifyEmittedEvent(HBaseEvent event) {
        lastTimeStamp = event.getTimestamp();
        lastIndex = event.getIndex();
    }

    public boolean isAlreadyProcessedEvent(HBaseEvent event) {
        System.out.println("Testing, last stamp is " + lastTimeStamp + "-" + lastIndex);
        return !event.isLaterThan(lastTimeStamp, lastIndex);
    }
}
