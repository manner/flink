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

package org.apache.flink.connector.hbase.source.hbaseendpoint;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.connector.hbase.sink.HBaseSink;
import org.apache.flink.connector.hbase.sink.HBaseSinkSerializer;
import org.apache.flink.connector.hbase.source.TestsWithTestHBaseCluster;
import org.apache.flink.connector.hbase.source.hbasemocking.DemoSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.stream.LongStream;

import static org.junit.Assert.assertArrayEquals;

/** Test for {@link org.apache.flink.connector.hbase.sink.HBaseSink}. */
public class HBaseSinkTests extends TestsWithTestHBaseCluster {

    private static final String tableName = "test-table";
    private static final String columnFamily = "info";
    private static final String qualifier = "test";

    @Test
    public void testSimpleSink() throws Exception {
        DemoSchema schema = new DemoSchema();
        schema.createSchema(cluster.getConfig());

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration hbaseConfiguration = cluster.getConfig();

        int start = 1;
        int end = 10;

        final DataStream<Long> numberSource =
                env.fromSource(
                        new NumberSequenceSource(start, end),
                        WatermarkStrategy.noWatermarks(),
                        "numberSource");

        final HBaseSink<Long> hbaseSink =
                new HBaseSink<>(tableName, new HBaseTestSerializer(), hbaseConfiguration);
        numberSource.sinkTo(hbaseSink);
        env.execute();

        long[] expected = LongStream.rangeClosed(start, end).toArray();
        long[] actual = new long[end - start + 1];

        try (Connection connection = ConnectionFactory.createConnection(hbaseConfiguration)) {
            Table table = connection.getTable(TableName.valueOf(tableName));
            for (int i = start; i <= end; i++) {
                Get get = new Get(Bytes.toBytes(String.valueOf(i)));
                Result r = table.get(get);
                byte[] value = r.getValue(columnFamily.getBytes(), qualifier.getBytes());
                long l = Long.parseLong(new String(value));
                actual[i - start] = l;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        assertArrayEquals(expected, actual);
    }

    private static class HBaseTestSerializer implements HBaseSinkSerializer<Long>, Serializable {
        @Override
        public byte[] serializePayload(Long event) {
            return Bytes.toBytes(event.toString());
        }

        @Override
        public byte[] serializeColumnFamily(Long event) {
            return Bytes.toBytes(columnFamily);
        }

        @Override
        public byte[] serializeQualifier(Long event) {
            return Bytes.toBytes(qualifier);
        }

        @Override
        public byte[] serializeRowKey(Long event) {
            return Bytes.toBytes(event.toString());
        }
    }
}
