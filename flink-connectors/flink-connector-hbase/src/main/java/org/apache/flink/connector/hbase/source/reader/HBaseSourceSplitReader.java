package org.apache.flink.connector.hbase.source.reader;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.hbase.source.HBaseSource;
import org.apache.flink.connector.hbase.source.split.HBaseSourceSplit;
import org.apache.flink.connector.hbase.source.standalone.HBaseConsumer;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Set;

/** A {@link SplitReader} implementation for Hbase. */
public class HBaseSourceSplitReader<T> implements SplitReader<T, HBaseSourceSplit> {

    private final Queue<HBaseSourceSplit> splits;
    private final HBaseConsumer hbaseConsumer;
    private final DeserializationSchema<T> deserializationSchema;

    @Nullable private String currentSplitId;

    public HBaseSourceSplitReader(DeserializationSchema<T> deserializationSchema) {
        System.out.println("constructing Split Reader");
        try {
            this.hbaseConsumer = new HBaseConsumer(HBaseSource.tempHbaseConfig, HBaseSource.tableName);
        } catch (Exception e) {
            throw new RuntimeException("failed HBase consumer", e);
        }
        this.splits = new ArrayDeque<>();
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public RecordsWithSplitIds<T> fetch() throws IOException {
        final HBaseSourceSplit nextSplit = splits.poll();
        if (nextSplit != null) {
            currentSplitId = nextSplit.splitId();
        }
        byte[] nextValue = hbaseConsumer.next();
        T value = deserializationSchema.deserialize(nextValue);
        List<T> records = Collections.singletonList(value);
        return new HbaseSplitRecords<>(currentSplitId, records.iterator(), Collections.emptySet());
    }

    @Override
    public void handleSplitsChanges(SplitsChange<HBaseSourceSplit> splitsChanges) {
        splits.addAll(splitsChanges.splits());
    }

    @Override
    public void wakeUp() {}

    @Override
    public void close() throws Exception {}

    private static class HbaseSplitRecords<T> implements RecordsWithSplitIds<T> {
        private final Set<String> finishedSplits;
        private Iterator<T> recordsForSplit;

        private String splitId;

        private HbaseSplitRecords(
                String splitId, Iterator<T> recordsForSplit, Set<String> finishedSplits) {
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
        public T nextRecordFromSplit() {
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
