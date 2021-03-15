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

import org.apache.flink.connector.hbase.HBaseEvent;

import org.apache.hadoop.hbase.Cell;

/** The HBaseSourceEvent is used to represent incoming events from HBase. */
public class HBaseSourceEvent extends HBaseEvent {

    private final String table;
    private final long timestamp;
    /** Index of operation inside one wal entry. */
    private final int index;

    public HBaseSourceEvent(
            Cell.Type type,
            String rowId,
            String table,
            String cf,
            String qualifier,
            byte[] payload,
            long timestamp,
            int index) {
        super(type, rowId, cf, qualifier, payload);
        this.table = table;
        this.timestamp = timestamp;
        this.index = index;
    }

    public static HBaseSourceEvent fromCell(String table, Cell cell, int index) {
        final String row = new String(cell.getRowArray(), CHARSET);
        final String cf = new String(cell.getFamilyArray(), CHARSET);
        final String qualifier = new String(cell.getQualifierArray(), CHARSET);
        final byte[] payload = cell.getValueArray();
        final long timestamp = cell.getTimestamp();
        final Cell.Type type = cell.getType();
        return new HBaseSourceEvent(type, row, table, cf, qualifier, payload, timestamp, index);
    }

    public long getTimestamp() {
        return timestamp;
    }

    public int getIndex() {
        return index;
    }

    public String getTable() {
        return table;
    }

    @Override
    public String toString() {
        return table + " " + super.toString() + " " + timestamp + " " + index;
    }

    public boolean isLaterThan(long timestamp, int index) {
        return timestamp < this.getTimestamp()
                || (timestamp == this.getTimestamp() && index < this.getIndex());
    }
}
