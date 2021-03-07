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

package org.apache.flink.connector.hbase.sink;

import org.apache.flink.connector.hbase.source.TestsWithTestHBaseCluster;
import org.apache.flink.connector.hbase.testutil.HBaseTestClusterUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.util.stream.LongStream;

import static org.junit.Assert.assertArrayEquals;

/** Test for {@link org.apache.flink.connector.hbase.sink.HBaseSink}. */
public class HBaseSinkTests extends TestsWithTestHBaseCluster {

    @Test
    public void testSimpleSink() throws Exception {
        cluster.makeTable(baseTableName);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration hbaseConfiguration = cluster.getConfig();

        int start = 1;
        int end = 10;

        DataStream<Long> numberStream = env.fromSequence(start, end);

        final HBaseSink<Long> hbaseSink =
                new HBaseSink<>(baseTableName, new HBaseTestSerializer(), hbaseConfiguration);
        numberStream.sinkTo(hbaseSink);
        env.execute();

        long[] expected = LongStream.rangeClosed(start, end).toArray();
        long[] actual = new long[end - start + 1];

        try (Connection connection = ConnectionFactory.createConnection(hbaseConfiguration)) {
            Table table = connection.getTable(TableName.valueOf(baseTableName));
            for (int i = start; i <= end; i++) {
                Get get = new Get(Bytes.toBytes(String.valueOf(i)));
                Result r = table.get(get);
                byte[] value =
                        r.getValue(
                                HBaseTestClusterUtil.DEFAULT_COLUMN_FAMILY.getBytes(),
                                HBaseTestClusterUtil.DEFAULT_QUALIFIER.getBytes());
                long l = Long.parseLong(new String(value));
                actual[i - start] = l;
            }
        }

        assertArrayEquals(expected, actual);
    }

    private static class HBaseTestSerializer implements HBaseSinkSerializer<Long> {
        @Override
        public byte[] serializePayload(Long event) {
            return Bytes.toBytes(event.toString());
        }

        @Override
        public byte[] serializeColumnFamily(Long event) {
            return Bytes.toBytes(HBaseTestClusterUtil.DEFAULT_COLUMN_FAMILY);
        }

        @Override
        public byte[] serializeQualifier(Long event) {
            return Bytes.toBytes(HBaseTestClusterUtil.DEFAULT_QUALIFIER);
        }

        @Override
        public byte[] serializeRowKey(Long event) {
            return Bytes.toBytes(event.toString());
        }

        @Override
        public Class<? extends Mutation> serializeRowType(Long event) {
            return Put.class;
        }
    }
}
