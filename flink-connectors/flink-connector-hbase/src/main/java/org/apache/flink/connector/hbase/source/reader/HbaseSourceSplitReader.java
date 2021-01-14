package org.apache.flink.connector.hbase.source.reader;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.hbase.source.HbaseSource;
import org.apache.flink.connector.hbase.source.split.HbaseSourceSplit;
import org.apache.flink.connector.hbase.source.standalone.HbaseConsumer;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Set;

/** A {@link SplitReader} implementation for Hbase. */
public class HbaseSourceSplitReader implements SplitReader<byte[], HbaseSourceSplit> {

    private final Queue<HbaseSourceSplit> splits;
    private final HbaseConsumer hbaseConsumer;
    @Nullable private String currentSplitId;

    public HbaseSourceSplitReader() {
        System.out.println("constructing Split Reader");
        try {
            this.hbaseConsumer = new HbaseConsumer(HbaseSource.TEMP_hbaseConf);
        } catch (Exception e) {
            throw new RuntimeException("failed HBase consumer", e);
        }
        this.splits = new ArrayDeque<>();
    }

    @Override
    public RecordsWithSplitIds<byte[]> fetch() throws IOException {
        System.out.println("fetching in Split Reader");
        final HbaseSourceSplit nextSplit = splits.poll();
        if (nextSplit != null) {
            currentSplitId = nextSplit.splitId();
        }
        byte[] nextValue = hbaseConsumer.next();

        if (nextValue != null) {
            System.out.println(Arrays.toString(nextValue));
        }
        List<byte[]> records = Collections.singletonList(nextValue);

        return new HbaseSplitRecords(currentSplitId, records.iterator(), Collections.emptySet());
    }

    @Override
    public void handleSplitsChanges(SplitsChange<HbaseSourceSplit> splitsChanges) {
        splits.addAll(splitsChanges.splits());
    }

    @Override
    public void wakeUp() {}

    @Override
    public void close() throws Exception {}

    private static class HbaseSplitRecords implements RecordsWithSplitIds<byte[]> {
        private final Set<String> finishedSplits;
        private Iterator<byte[]> recordsForSplit;

        private String splitId;

        private HbaseSplitRecords(
                String splitId, Iterator<byte[]> recordsForSplit, Set<String> finishedSplits) {
            this.splitId = splitId;
            this.recordsForSplit = recordsForSplit;
            this.finishedSplits = finishedSplits;
        }

        @Nullable
        @Override
        public String nextSplit() {
            final String nextSplit = this.splitId;
            this.splitId = null;
            this.recordsForSplit = nextSplit != null ? this.recordsForSplit : null;

            return nextSplit;
        }

        @Nullable
        @Override
        public byte[] nextRecordFromSplit() {
            if (recordsForSplit != null && recordsForSplit.hasNext()) {
                return recordsForSplit.next();
            } else {
                return null;
            }
        }

        @Override
        public Set<String> finishedSplits() {
            return finishedSplits;
        }
    }
}
