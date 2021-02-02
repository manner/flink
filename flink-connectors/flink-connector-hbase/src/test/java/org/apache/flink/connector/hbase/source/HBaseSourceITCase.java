package org.apache.flink.connector.hbase.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.hbase.source.hbasemocking.DemoIngester;
import org.apache.flink.connector.hbase.source.hbasemocking.HBaseTestClusterUtil;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import org.apache.hadoop.hbase.client.Put;
import org.junit.Test;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertArrayEquals;

/** Tests the most basic use cases of the source with a mocked HBase system. */
public class HBaseSourceITCase extends TestsWithTestHBaseCluster {

    @Test
    public void testBasicPut() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> stream = streamFromHBaseSource(env, baseTableName);
        DemoIngester ingester = new DemoIngester(baseTableName);
        Tuple2<Put, String[]> put = ingester.createPut();
        String[] expectedValues = put.f1;

        expectFirstValuesToBe(
                stream,
                expectedValues,
                "HBase source did not produce the right values after a basic put operation");
        doAndWaitForSuccess(env, () -> ingester.commitPut(put.f0), 120);
    }

    @Test
    public void testOnlyReplicateSpecifiedTable() throws Exception {
        String secondTable = baseTableName + "-table2";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> stream = streamFromHBaseSource(env, baseTableName);
        DemoIngester ingester = new DemoIngester(baseTableName);
        DemoIngester ingester2 = new DemoIngester(secondTable);
        Tuple2<Put, String[]> put = ingester.createPut();
        Tuple2<Put, String[]> put2 = ingester2.createPut();
        String[] expectedValues = put.f1;

        expectFirstValuesToBe(
                stream,
                expectedValues,
                "HBase source did not produce the values of the correct table");
        doAndWaitForSuccess(
                env,
                () -> {
                    ingester2.commitPut(put2.f0);
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    ingester.commitPut(put.f0);
                },
                120);
    }

    private static DataStream<String> streamFromHBaseSource(
            StreamExecutionEnvironment environment, String tableName)
            throws ParserConfigurationException, SAXException, IOException {
        HBaseStringDeserializationScheme deserializationScheme =
                new HBaseStringDeserializationScheme();
        HBaseSource<String> source =
                new HBaseSource<>(
                        null, deserializationScheme, tableName, HBaseTestClusterUtil.getConfig());
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
            // TODO potentially wait for source to be up and running
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

    /** Bla. */
    public static class HBaseStringDeserializationScheme
            extends AbstractDeserializationSchema<String> {

        @Override
        public String deserialize(byte[] message) throws IOException {
            return new String(message);
        }
    }
}
