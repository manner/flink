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

import org.apache.hadoop.hbase.Cell;

/** HBaseEvent. */
public class HBaseEvent {
    private final Cell.Type type;
    private final String rowId;
    private final String table;
    private final String cf;
    private final String qualifier;
    private final byte[] payload;
    private final long timestamp;
    /** Index of operation inside one wal entry. */
    private final int index;

    private final long offset;

    public HBaseEvent(
            Cell.Type type,
            String rowId,
            String table,
            String cf,
            String qualifier,
            byte[] payload,
            long timestamp,
            int index,
            long offset) {
        this.type = type;
        this.rowId = rowId;
        this.table = table;
        this.cf = cf;
        this.qualifier = qualifier;
        this.payload = payload;
        this.timestamp = timestamp;
        this.index = index;
        this.offset = offset;
    }

    public byte[] getPayload() {
        return payload;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public int getIndex() {
        return index;
    }

    public Cell.Type getType() {
        return type;
    }

    public String getRowId() {
        return rowId;
    }

    public String getTable() {
        return table;
    }

    public String getCf() {
        return cf;
    }

    public String getQualifier() {
        return qualifier;
    }

    public long getOffset() {
        return offset;
    }

    @Override
    public String toString() {
        return type.name()
                + " "
                + table
                + " "
                + rowId
                + " "
                + cf
                + " "
                + qualifier
                + " "
                + new String(payload)
                + " "
                + timestamp
                + " "
                + index
                + " "
                + offset;
    }

    public boolean isLaterThan(long timestamp, int index) {
        return timestamp < this.getTimestamp()
                || (timestamp == this.getTimestamp() && index < this.getIndex());
    }
}
