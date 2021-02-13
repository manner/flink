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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.hbase.source.TestsWithTestHBaseCluster;
import org.apache.flink.connector.hbase.source.hbasemocking.DemoIngester;
import org.apache.flink.connector.hbase.source.hbasemocking.DemoSchema;
import org.apache.flink.connector.hbase.source.hbasemocking.HBaseTestClusterUtil;
import org.apache.flink.connector.hbase.source.reader.HBaseEvent;

import org.apache.hadoop.hbase.client.Put;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** Tests for {@link org.apache.flink.connector.hbase.source.hbaseendpoint.HBaseConsumer}. */
public class HBaseConsumerTest extends TestsWithTestHBaseCluster {

    @Test
    public void testSetup() throws Exception {
        new HBaseConsumer(HBaseTestClusterUtil.getConfig())
                .startReplication(baseTableName, DemoSchema.COLUMN_FAMILY_NAME);
    }

    @Test
    public void testPutCreatesEvent() throws Exception {
        HBaseConsumer consumer = new HBaseConsumer(HBaseTestClusterUtil.getConfig());
        consumer.startReplication(baseTableName, DemoSchema.COLUMN_FAMILY_NAME);
        DemoIngester ingester = new DemoIngester(baseTableName);
        ingester.commitPut(ingester.createPut().f0);
        HBaseEvent result = CompletableFuture.supplyAsync(consumer::next).get(30, TimeUnit.SECONDS);
        assertNotNull(result);
    }

    @Test
    public void testTimestampsAndIndicesDefineStrictOrder() throws Exception {
        HBaseConsumer consumer = new HBaseConsumer(HBaseTestClusterUtil.getConfig());
        consumer.startReplication(baseTableName, DemoSchema.COLUMN_FAMILY_NAME);
        DemoIngester ingester = new DemoIngester(baseTableName);

        int numPuts = 3;
        int putSize = 0;
        for (int i = 0; i < numPuts; i++) {
            Tuple2<Put, String[]> put = ingester.createPut();
            ingester.commitPut(put.f0);
            putSize = put.f1.length;
        }

        long lastTimeStamp = -1;
        int lastIndex = -1;
        for (int i = 0; i < numPuts * putSize; i++) {
            HBaseEvent nextEvent =
                    CompletableFuture.supplyAsync(consumer::next).get(30, TimeUnit.SECONDS);
            assertTrue(
                    "Events were not collected with strictly ordered, unique timestamp x index",
                    nextEvent.isLaterThan(lastTimeStamp, lastIndex));
            lastTimeStamp = nextEvent.getTimestamp();
            lastIndex = nextEvent.getIndex();
        }
    }
}
