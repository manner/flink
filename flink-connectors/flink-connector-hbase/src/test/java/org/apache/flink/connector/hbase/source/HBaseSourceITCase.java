package org.apache.flink.connector.hbase.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.hbase.source.hbasemocking.DemoIngester;
import org.apache.flink.connector.hbase.source.hbasemocking.DemoSchema;
import org.apache.flink.connector.hbase.source.hbasemocking.TestClusterStarter;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import org.apache.hadoop.hbase.client.Put;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertArrayEquals;

/** Tests the most basic use cases of the source with a mocked HBase system. */
public class HBaseSourceITCase {

    /** For debug purposes. Allows to run the test quickly without starting a fresh cluster */
    public static final boolean USE_EXISTING_CLUSTER = false;

    /** Shadowed from org.apache.flink.test.util.SuccessException. */
    public static class SuccessException extends RuntimeException {}

    @BeforeClass
    public static void setup() {
        if (!USE_EXISTING_CLUSTER) {
            TestClusterStarter.startCluster();
        }
        assert TestClusterStarter.isClusterAlreadyRunning();
    }

    @AfterClass
    public static void teardown() throws IOException {
        if (!USE_EXISTING_CLUSTER) TestClusterStarter.shutdownCluster();
    }

    @After
    public void clearReplicationPeers() {
        TestClusterStarter.clearReplicationPeers();
        // TODO also cleanup data
    }

    @Test
    public void testBasicPut() throws Exception {
        CustomHBaseDeserializationScheme deserializationScheme =
                new CustomHBaseDeserializationScheme();
        HBaseSource<String> source =
                new HBaseSource<>(
                        null,
                        deserializationScheme,
                        DemoSchema.TABLE_NAME,
                        TestClusterStarter.getConfig());
        // NumberSequenceSource source = new NumberSequenceSource(1, 10);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> stream =
                env.fromSource(
                        source,
                        WatermarkStrategy.noWatermarks(),
                        "testBasicPut",
                        deserializationScheme.getProducedType());
        DemoIngester ingester = new DemoIngester();
        Tuple2<Put, String[]> put = ingester.createPut();
        String[] expectedValues = put.f1;
        List<String> collectedValues = new ArrayList<>();
        stream.flatMap(
                new RichFlatMapFunction<String, Object>() {

                    @Override
                    public void flatMap(String value, Collector<Object> out) throws Exception {
                        collectedValues.add(value);
                        if (collectedValues.size() == expectedValues.length) {
                            assertArrayEquals(
                                    "HBase source did not produce the right values after a basic put operation",
                                    expectedValues,
                                    collectedValues.toArray());
                            throw new SuccessException();
                        }
                    }
                });
        doAndWaitForSuccess(env, () -> ingester.commitPut(put.f0), 60);
    }

    private static void doAndWaitForSuccess(
            StreamExecutionEnvironment env, Runnable action, int timeout) {
        try {
            JobClient jobClient = env.executeAsync();
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

    private static boolean causedBySuccess(Exception exception) {
        boolean success = false;
        for (Throwable e = exception; !success && e != null; e = e.getCause()) {
            success = success || e instanceof SuccessException;
        }
        return success;
    }

    /** Bla. */
    public static class CustomHBaseDeserializationScheme
            extends AbstractDeserializationSchema<String> {

        @Override
        public String deserialize(byte[] message) throws IOException {
            return new String(message);
        }
    }
}
