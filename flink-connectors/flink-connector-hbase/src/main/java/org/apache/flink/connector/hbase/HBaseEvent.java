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

package org.apache.flink.connector.hbase;

import org.apache.hadoop.hbase.Cell;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * The base HBaseEvent which needs to be created in the SinkSerializer to write data to HBase. The
 * subclass {@link org.apache.flink.connector.hbase.source.reader.HBaseSourceEvent} contains
 * additional information and is used by the HBaseSource to represent an incoming event from HBase.
 */
public class HBaseEvent {

    protected static final Charset CHARSET = StandardCharsets.UTF_8;

    private final String rowId;
    private final String cf;
    private final String qualifier;
    private final byte[] payload;
    private final Cell.Type type;

    protected HBaseEvent(
            Cell.Type type, String rowId, String cf, String qualifier, byte[] payload) {
        this.rowId = rowId;
        this.cf = cf;
        this.qualifier = qualifier;
        this.payload = payload;
        this.type = type;
    }

    public static HBaseEvent deleteWith(String rowId, String cf, String qualifier) {
        return new HBaseEvent(Cell.Type.Delete, rowId, cf, qualifier, null);
    }

    public static HBaseEvent putWith(String rowId, String cf, String qualifier, byte[] payload) {
        return new HBaseEvent(Cell.Type.Put, rowId, cf, qualifier, payload);
    }

    @Override
    public String toString() {
        return type.name() + " " + rowId + " " + cf + " " + qualifier + " " + new String(payload);
    }

    public Cell.Type getType() {
        return type;
    }

    public byte[] getPayload() {
        return payload;
    }

    public byte[] getRowId() {
        return rowId.getBytes(CHARSET);
    }

    public byte[] getCf() {
        return cf.getBytes(CHARSET);
    }

    public byte[] getQualifier() {
        return qualifier.getBytes(CHARSET);
    }
}
