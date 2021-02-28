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

package org.apache.flink.connector.hbase.sink.writer;

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.hbase.sink.HBaseSinkCommittable;
import org.apache.flink.connector.hbase.sink.HBaseSinkSerializer;
import org.apache.flink.connector.hbase.util.HBaseConfigurationUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** HBaseWriter. */
public class HBaseWriter<IN> implements SinkWriter<IN, HBaseSinkCommittable, HBaseWriterState> {

    private static final int QUEUE_LIMIT = 100;
    private final HBaseSinkSerializer<IN> sinkSerializer;
    private Connection connection;
    private Table table;
    private List<Put> buffer;

    public HBaseWriter(
            Sink.InitContext context,
            String tableName,
            HBaseSinkSerializer<IN> sinkSerializer,
            byte[] serializedConfig) {
        System.out.println("Creating HBaseWriter");
        this.sinkSerializer = sinkSerializer;
        this.buffer = new ArrayList<>();
        Configuration hbaseConfiguration =
                HBaseConfigurationUtil.deserializeConfiguration(serializedConfig, null);
        try {
            connection = ConnectionFactory.createConnection(hbaseConfiguration);
            table = connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(IN element, Context context) throws IOException {
        Put put = new Put(sinkSerializer.serializeRowKey(element));
        put.addColumn(
                sinkSerializer.serializeColumnFamily(element),
                sinkSerializer.serializeQualifier(element),
                sinkSerializer.serializePayload(element));
        buffer.add(put);
        if (buffer.size() >= QUEUE_LIMIT) {
            table.put(buffer);
            buffer.clear();
        }
    }

    @Override
    public List<HBaseSinkCommittable> prepareCommit(boolean flush) throws IOException {
        return Collections.emptyList();
    }

    @Override
    public List<HBaseWriterState> snapshotState() throws IOException {
        return Collections.emptyList();
    }

    @Override
    public void close() throws Exception {
        this.table.close();
        this.connection.close();
    }
}
