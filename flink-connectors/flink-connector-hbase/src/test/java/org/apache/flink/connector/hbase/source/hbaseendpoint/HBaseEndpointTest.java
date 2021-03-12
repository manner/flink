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

import org.apache.flink.connector.hbase.source.reader.HBaseSourceEvent;
import org.apache.flink.connector.hbase.testutil.HBaseTestCluster;
import org.apache.flink.connector.hbase.testutil.TestsWithTestHBaseCluster;

import org.apache.hadoop.hbase.Cell;
import org.junit.Test;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** Tests for {@link HBaseEndpoint}. */
public class HBaseEndpointTest extends TestsWithTestHBaseCluster {

    @Test
    public void testSetup() throws Exception {
        new HBaseEndpoint(cluster.getConfig(), cluster.getPropertiesForTable(baseTableName))
                .startReplication(
                        Collections.singletonList(HBaseTestCluster.DEFAULT_COLUMN_FAMILY));
    }

    @Test
    public void testPutCreatesEvent() throws Exception {
        cluster.makeTable(baseTableName);
        HBaseEndpoint consumer =
                new HBaseEndpoint(
                        cluster.getConfig(), cluster.getPropertiesForTable(baseTableName));
        consumer.startReplication(
                Collections.singletonList(HBaseTestCluster.DEFAULT_COLUMN_FAMILY));
        cluster.put(baseTableName, "foobar");
        HBaseSourceEvent result =
                CompletableFuture.supplyAsync(consumer::next).get(30, TimeUnit.SECONDS);
        assertNotNull(result);
        assertEquals(Cell.Type.Put, result.getType());
    }

    @Test
    public void testDeleteCreatesEvent() throws Exception {
        cluster.makeTable(baseTableName);
        HBaseEndpoint consumer =
                new HBaseEndpoint(
                        cluster.getConfig(), cluster.getPropertiesForTable(baseTableName));
        consumer.startReplication(
                Collections.singletonList(HBaseTestCluster.DEFAULT_COLUMN_FAMILY));

        String rowKey = cluster.put(baseTableName, "foobar");
        cluster.delete(
                baseTableName,
                rowKey,
                HBaseTestCluster.DEFAULT_COLUMN_FAMILY,
                HBaseTestCluster.DEFAULT_QUALIFIER);

        CompletableFuture.supplyAsync(consumer::next).get(30, TimeUnit.SECONDS);
        HBaseSourceEvent result =
                CompletableFuture.supplyAsync(consumer::next).get(30, TimeUnit.SECONDS);

        assertNotNull(result);
        assertEquals(Cell.Type.Delete, result.getType());
    }

    @Test
    public void testTimestampsAndIndicesDefineStrictOrder() throws Exception {
        HBaseEndpoint consumer =
                new HBaseEndpoint(
                        cluster.getConfig(), cluster.getPropertiesForTable(baseTableName));
        consumer.startReplication(
                Collections.singletonList(HBaseTestCluster.DEFAULT_COLUMN_FAMILY));
        cluster.makeTable(baseTableName);

        int numPuts = 3;
        int putSize = 2 * DEFAULT_CF_COUNT;
        for (int i = 0; i < numPuts; i++) {
            cluster.put(baseTableName, 1, uniqueValues(putSize));
        }

        long lastTimeStamp = -1;
        int lastIndex = -1;
        for (int i = 0; i < numPuts * putSize; i++) {
            HBaseSourceEvent nextEvent =
                    CompletableFuture.supplyAsync(consumer::next).get(30, TimeUnit.SECONDS);
            assertTrue(
                    "Events were not collected with strictly ordered, unique timestamp x index",
                    nextEvent.isLaterThan(lastTimeStamp, lastIndex));
            lastTimeStamp = nextEvent.getTimestamp();
            lastIndex = nextEvent.getIndex();
        }
    }

    @Test
    public void testUsingTheSameIdDoesNotShowAlreadyProcessedEventsAgain() throws Exception {
        cluster.makeTable(baseTableName);
        String id = UUID.randomUUID().toString().substring(0, 5);

        String firstValue = UUID.randomUUID().toString();
        HBaseEndpoint firstConsumer =
                new HBaseEndpoint(
                        id, cluster.getConfig(), cluster.getPropertiesForTable(baseTableName));
        firstConsumer.startReplication(
                Collections.singletonList(HBaseTestCluster.DEFAULT_COLUMN_FAMILY));
        cluster.put(baseTableName, firstValue);
        String firstResult = new String(firstConsumer.next().getPayload());
        assertEquals(firstValue, firstResult);
        firstConsumer.close();

        String secondValue = UUID.randomUUID().toString();
        HBaseEndpoint secondConsumer =
                new HBaseEndpoint(
                        id, cluster.getConfig(), cluster.getPropertiesForTable(baseTableName));
        secondConsumer.startReplication(
                Collections.singletonList(HBaseTestCluster.DEFAULT_COLUMN_FAMILY));
        cluster.put(baseTableName, secondValue);
        String secondResult = new String(secondConsumer.next().getPayload());
        assertEquals(secondValue, secondResult);
        firstConsumer.close();
    }
}
