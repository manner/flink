package org.apache.flink.connector.hbase.source.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.hbase.source.split.HbaseSourceSplit;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.List;
import java.util.Queue;

/** The enumerator class for Hbase source. */
@Internal
public class HbaseSplitEnumerator
        implements SplitEnumerator<HbaseSourceSplit, Collection<HbaseSourceSplit>> {
    private final SplitEnumeratorContext<HbaseSourceSplit> context;
    private final Queue<HbaseSourceSplit> remainingSplits;

    public HbaseSplitEnumerator(SplitEnumeratorContext<HbaseSourceSplit> context) {
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
        final HbaseSourceSplit nextSplit = remainingSplits.poll();
        if (nextSplit != null) {
            context.assignSplit(nextSplit, subtaskId);
        } else {
            context.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public void addSplitsBack(List<HbaseSourceSplit> splits, int subtaskId) {
        remainingSplits.addAll(splits);
    }

    @Override
    public void addReader(int subtaskId) {
        System.out.println("addReader");
        HbaseSourceSplit nextSplit = remainingSplits.poll();
        if (nextSplit != null) {
            context.assignSplit(nextSplit, subtaskId);
        } else {
            context.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public Collection<HbaseSourceSplit> snapshotState() throws Exception {
        return remainingSplits;
    }

    public void addSplits(Collection<HbaseSourceSplit> splits) {
        remainingSplits.addAll(splits);
    }
}
