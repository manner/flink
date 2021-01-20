package org.apache.flink.connector.hbase.source.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.hbase.source.split.HBaseSourceSplit;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.List;
import java.util.Queue;

/** The enumerator class for Hbase source. */
@Internal
public class HBaseSplitEnumerator
        implements SplitEnumerator<HBaseSourceSplit, Collection<HBaseSourceSplit>> {
    private final SplitEnumeratorContext<HBaseSourceSplit> context;
    private final Queue<HBaseSourceSplit> remainingSplits;

    public HBaseSplitEnumerator(SplitEnumeratorContext<HBaseSourceSplit> context) {
        this.context = context;
        this.remainingSplits = new ArrayDeque<>();
    }

    @Override
    public void start() {
        System.out.println("Starting HbaseSourceEnumerator");
    }

    @Override
    public void close() {}

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        final HBaseSourceSplit nextSplit = remainingSplits.poll();
        if (nextSplit != null) {
            context.assignSplit(nextSplit, subtaskId);
        } else {
            context.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public void addSplitsBack(List<HBaseSourceSplit> splits, int subtaskId) {
        remainingSplits.addAll(splits);
    }

    @Override
    public void addReader(int subtaskId) {
        System.out.println("addReader");
        HBaseSourceSplit nextSplit = remainingSplits.poll();
        if (nextSplit != null) {
            context.assignSplit(nextSplit, subtaskId);
        } else {
            context.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public Collection<HBaseSourceSplit> snapshotState() throws Exception {
        return remainingSplits;
    }

    public void addSplits(Collection<HBaseSourceSplit> splits) {
        remainingSplits.addAll(splits);
    }
}
