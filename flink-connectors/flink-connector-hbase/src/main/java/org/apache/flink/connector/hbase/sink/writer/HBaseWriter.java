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
import org.apache.flink.connector.hbase.sink.HBaseSinkOptions;
import org.apache.flink.connector.hbase.sink.HBaseSinkSerializer;
import org.apache.flink.connector.hbase.util.HBaseConfigurationUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

/** HBaseWriter. */
public class HBaseWriter<IN> implements SinkWriter<IN, HBaseSinkCommittable, HBaseWriterState> {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseWriter.class);

    private final int queueLimit;
    private final int maxLatencyMs;
    private final HBaseSinkSerializer<IN> sinkSerializer;
    private final List<Mutation> buffer;
    private final Connection connection;
    private final Table table;
    private long lastFlushTimeStamp = 0;
    private TimerTask batchSendTimer;

    public HBaseWriter(
            Sink.InitContext context,
            HBaseSinkSerializer<IN> sinkSerializer,
            byte[] serializedConfig,
            Properties properties) {
        this.sinkSerializer = sinkSerializer;
        this.queueLimit = HBaseSinkOptions.getQueueLimit(properties);
        this.maxLatencyMs = HBaseSinkOptions.getMaxLatency(properties);
        String tableName = HBaseSinkOptions.getTableName(properties);

        this.buffer = new ArrayList<>(queueLimit);

        Configuration hbaseConfiguration =
                HBaseConfigurationUtil.deserializeConfiguration(serializedConfig, null);
        try {
            connection = ConnectionFactory.createConnection(hbaseConfiguration);
            table = connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            throw new RuntimeException("Connection to HBase couldn't be established", e);
        }

        startBatchSendTimer();
        LOG.debug("started sink writer");
    }

    private void startBatchSendTimer() {
        batchSendTimer =
                new TimerTask() {
                    @Override
                    public void run() {
                        long diff = System.currentTimeMillis() - lastFlushTimeStamp;
                        if (diff > maxLatencyMs) {
                            flushBuffer();
                        }
                    }
                };
        new Timer().scheduleAtFixedRate(batchSendTimer, 0, maxLatencyMs / 2);
    }

    private void flushBuffer() {
        lastFlushTimeStamp = System.currentTimeMillis();
        if (buffer.size() == 0) {
            return;
        }
        try {
            table.batch(buffer, null);
            buffer.clear();
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Failed storing batch data in HBase", e);
        }
    }

    @Override
    public void write(IN element, Context context) {
        Class<? extends Mutation> mutationClass = sinkSerializer.serializeRowType(element);
        if (mutationClass.isAssignableFrom(Put.class)) {
            Put put = new Put(sinkSerializer.serializeRowKey(element));
            put.addColumn(
                    sinkSerializer.serializeColumnFamily(element),
                    sinkSerializer.serializeQualifier(element),
                    sinkSerializer.serializePayload(element));
            buffer.add(put);
        } else if (mutationClass.isAssignableFrom(Delete.class)) {
            Delete delete = new Delete(sinkSerializer.serializeRowKey(element));
            delete.addColumn(
                    sinkSerializer.serializeColumnFamily(element),
                    sinkSerializer.serializeQualifier(element));
            buffer.add(delete);
        } else {
            throw new UnsupportedOperationException("row type not supported");
        }

        if (buffer.size() >= queueLimit) {
            flushBuffer();
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
        flushBuffer();
        batchSendTimer.cancel();
        table.close();
        connection.close();
    }
}
