/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.hbase.source.reader;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.hbase.source.hbaseendpoint.HBaseEndpoint;
import org.apache.flink.connector.hbase.source.split.HBaseSourceSplit;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Set;

/** A {@link SplitReader} implementation for Hbase. */
public class HBaseSourceSplitReader implements SplitReader<HBaseEvent, HBaseSourceSplit> {

    private final Queue<HBaseSourceSplit> splits;
    private final HBaseEndpoint hbaseEndpoint;

    @Nullable
    private String currentSplitId;

    public HBaseSourceSplitReader(byte[] serializedConfig) {
        try {
            this.hbaseEndpoint = new HBaseEndpoint(serializedConfig);
        } catch (Exception e) {
            throw new RuntimeException("failed HBase consumer", e);
        }
        this.splits = new ArrayDeque<>();
    }

    @Override
    public RecordsWithSplitIds<HBaseEvent> fetch() throws IOException {
        final HBaseSourceSplit nextSplit = splits.poll();
        if (nextSplit != null) {
            currentSplitId = nextSplit.splitId();
        }
        HBaseEvent nextValue = hbaseEndpoint.next();
        List<HBaseEvent> records = Collections.singletonList(nextValue);
        return new HbaseSplitRecords<>(currentSplitId, records.iterator(), Collections.emptySet());
    }

    @Override
    public void handleSplitsChanges(SplitsChange<HBaseSourceSplit> splitsChanges) {
        if (splitsChanges instanceof SplitsAddition) {
            HBaseSourceSplit split = splitsChanges.splits().get(0);
            try {
                this.hbaseEndpoint.startReplication(split.getTable(), split.getColumnFamilies());
            } catch (Exception e) {
                throw new RuntimeException("failed HBase consumer", e);
            }
            splits.addAll(splitsChanges.splits());
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported splits change type "
                            + splitsChanges.getClass().getSimpleName()
                            + " in "
                            + this.getClass().getSimpleName());
        }
    }

    @Override
    public void wakeUp() {
    }

    @Override
    public void close() throws Exception {
        // TODO close consumer and add test for that
    }

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
