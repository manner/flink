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

package org.apache.flink.connector.hbase.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.hbase.source.reader.HBaseEvent;
import org.apache.flink.connector.hbase.source.reader.HBaseSourceDeserializer;
import org.apache.flink.connector.hbase.testutil.FailureSink;
import org.apache.flink.connector.hbase.testutil.HBaseTestClusterUtil;
import org.apache.flink.connector.hbase.testutil.Util;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterJobClient;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import org.apache.hadoop.hbase.client.Put;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.connector.hbase.testutil.FileSignal.awaitSignalThrowOnFailure;
import static org.apache.flink.connector.hbase.testutil.FileSignal.awaitSuccess;
import static org.apache.flink.connector.hbase.testutil.FileSignal.cleanupFolder;
import static org.apache.flink.connector.hbase.testutil.FileSignal.cleanupSignal;
import static org.apache.flink.connector.hbase.testutil.FileSignal.makeFolder;
import static org.apache.flink.connector.hbase.testutil.FileSignal.signal;
import static org.apache.flink.connector.hbase.testutil.FileSignal.signalFailure;
import static org.apache.flink.connector.hbase.testutil.FileSignal.signalSuccess;
import static org.junit.Assert.assertArrayEquals;

/** Tests the most basic use cases of the source with a mocked HBase system. */
public class HBaseSourceITCase extends TestsWithTestHBaseCluster {

    private DataStream<String> streamFromHBaseSource(
            StreamExecutionEnvironment environment, String tableName)
            throws ParserConfigurationException, SAXException, IOException {
        HBaseStringDeserializationScheme deserializationScheme =
                new HBaseStringDeserializationScheme();
        HBaseSource<String> source =
                new HBaseSource<>(deserializationScheme, tableName, cluster.getConfig());
        environment.setParallelism(1);
        DataStream<String> stream =
                environment.fromSource(
                        source,
                        WatermarkStrategy.noWatermarks(),
                        "hbaseSourceITCase",
                        deserializationScheme.getProducedType());
        return stream;
    }

    private static <T> void expectFirstValuesToBe(
            DataStream<T> stream, T[] expectedValues, String message) {

        List<T> collectedValues = new ArrayList<>();
        stream.flatMap(
                new RichFlatMapFunction<T, Object>() {

                    @Override
                    public void flatMap(T value, Collector<Object> out) {
                        System.out.println("Test collected: " + value);
                        collectedValues.add(value);
                        if (collectedValues.size() == expectedValues.length) {
                            assertArrayEquals(message, expectedValues, collectedValues.toArray());
                            throw new SuccessException();
                        }
                    }
                });
    }

    private static void doAndWaitForSuccess(
            StreamExecutionEnvironment env, Runnable action, int timeout) {
        try {
            JobClient jobClient = env.executeAsync();
            MiniCluster miniCluster = Util.miniCluster((MiniClusterJobClient) jobClient);
            Util.waitForClusterStart(miniCluster, true);

            action.run();
            jobClient.getJobExecutionResult().get(timeout, TimeUnit.SECONDS);
            jobClient.cancel();
            throw new RuntimeException("Waiting for the correct data timed out");
        } catch (Exception exception) {
            if (!causedBySuccess(exception)) {
                throw new RuntimeException("Test failed", exception);
            } else {
                // Normal termination
            }
        }
    }

    @Before
    public void makeSignalFolder() {
        makeFolder();
    }

    @After
    public void cleanupSignalFolder() throws IOException {
        cleanupFolder();
    }

    @Test
    public void testBasicPut() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> stream = streamFromHBaseSource(env, baseTableName);
        cluster.makeTable(baseTableName, DEFAULT_CF_COUNT);
        String[] expectedValues = uniqueValues(2 * DEFAULT_CF_COUNT);

        expectFirstValuesToBe(
                stream,
                expectedValues,
                "HBase source did not produce the right values after a basic put operation");
        doAndWaitForSuccess(
                env, () -> cluster.put(baseTableName, DEFAULT_CF_COUNT, expectedValues), 120);
    }

    @Test
    public void testOnlyReplicateSpecifiedTable() throws Exception {
        String secondTable = baseTableName + "-table2";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> stream = streamFromHBaseSource(env, baseTableName);
        cluster.makeTable(baseTableName, DEFAULT_CF_COUNT);
        cluster.makeTable(secondTable, DEFAULT_CF_COUNT);
        String[] expectedValues = uniqueValues(DEFAULT_CF_COUNT);

        expectFirstValuesToBe(
                stream,
                expectedValues,
                "HBase source did not produce the values of the correct table");
        doAndWaitForSuccess(
                env,
                () -> {
                    cluster.put(secondTable, DEFAULT_CF_COUNT, uniqueValues(DEFAULT_CF_COUNT));
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    cluster.put(secondTable, DEFAULT_CF_COUNT, expectedValues);
                },
                180);
    }

    @Test
    public void testRecordsAreProducedExactlyOnceWithCheckpoints() throws Exception {
        final String collectedValueSignal = "collectedValue";
        String[] expectedValues = uniqueValues(20);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        DataStream<String> stream = streamFromHBaseSource(env, baseTableName);
        stream.addSink(
                new FailureSink<String>(true, 3500, TypeInformation.of(String.class)) {

                    private void checkForSuccess() {
                        List<String> checkpointed = getCheckpointedValues();
                        System.out.println(unCheckpointedValues + " " + checkpointed);
                        if (checkpointed.size() == expectedValues.length) {
                            try {
                                assertArrayEquals(
                                        "Wrong values were produced.",
                                        expectedValues,
                                        checkpointed.toArray());
                                signalSuccess();
                            } catch (Exception e) {
                                System.out.println("Exception occured: " + e.getMessage());
                                signalFailure();
                                e.printStackTrace();
                            }
                        }
                    }

                    @Override
                    public void collectValue(String value) throws Exception {

                        if (getCheckpointedValues().contains(value)) {
                            System.out.println("Unique value " + value + " was not seen only once");
                            signalFailure();
                            throw new RuntimeException(
                                    "Unique value " + value + " was not seen only once");
                        }
                        checkForSuccess();
                        signal(collectedValueSignal);
                    }

                    @Override
                    public void checkpoint() throws Exception {
                        checkForSuccess();
                    }
                });

        JobClient jobClient = env.executeAsync();
        MiniCluster miniCluster = Util.miniCluster((MiniClusterJobClient) jobClient);
        Util.waitForClusterStart(miniCluster, true);
        try {
            Thread.sleep(8000);
            int putsPerPackage = 5;
            for (int i = 0; i < expectedValues.length; i += putsPerPackage) {
                System.out.println("Sending next package ...");
                for (int j = i; j < expectedValues.length && j < i + putsPerPackage; j++) {
                    cluster.put(baseTableName, expectedValues[j]);
                }

                // Assert that values have actually been sent over so there was an opportunity to
                // checkpoint them
                awaitSignalThrowOnFailure(collectedValueSignal, 240, TimeUnit.SECONDS);
                Thread.sleep(3000);
                System.out.println("Consuming collection signal");
                cleanupSignal(collectedValueSignal);
            }
            System.out.println("Finished sending packages, awaiting success ...");
            awaitSuccess(120, TimeUnit.SECONDS);
            System.out.println("Received success, ending test ...");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            System.out.println("Cancelling job client");
            jobClient.cancel();
        }
        System.out.println("End of test method reached");
    }

    @Test
    public void testBasicPutWhenMoreCFsThanThreads() throws Exception {
        int parallelism = 1;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.setMaxParallelism(parallelism);
        DataStream<String> stream = streamFromHBaseSource(env, baseTableName);

        String[] expectedValues = new String[] {"foo", "bar", "baz", "boo"};
        assert expectedValues.length > parallelism;
        cluster.makeTable(baseTableName, expectedValues.length);
        Put put = new Put("rowkey".getBytes());
        for (int i = 0; i < expectedValues.length; i++) {
            put.addColumn(
                    (HBaseTestClusterUtil.COLUMN_FAMILY_BASE + i).getBytes(),
                    expectedValues[i].getBytes(),
                    expectedValues[i].getBytes());
        }
        cluster.commitPut(baseTableName, put);

        expectFirstValuesToBe(
                stream,
                expectedValues,
                "HBase source did not produce the right values after a multi-cf put");
        doAndWaitForSuccess(env, () -> {}, 120);
    }

    /** Bla. */
    public static class HBaseStringDeserializationScheme extends HBaseSourceDeserializer<String> {
        @Override
        public String deserialize(HBaseEvent event) {
            return new String(event.getPayload());
        }
    }
}
