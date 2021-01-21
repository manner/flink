package org.apache.flink.connector.hbase.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.connector.hbase.source.hbasemocking.DemoIngester;
import org.apache.flink.connector.hbase.source.hbasemocking.TestClusterStarter;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.StreamCollector;
import org.apache.flink.util.Collector;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/** Tests the most basic use cases of the source with a mocked HBase system. */
public class HBaseSourceITCase {

    /** Shadowed from org.apache.flink.test.util.SuccessException. */
    public static class SuccessException extends RuntimeException {}

    @Rule public StreamCollector collector = new StreamCollector();

    @BeforeClass
    public static void setup() {
        TestClusterStarter.startCluster();
    }

    @AfterClass
    public static void teardown() throws IOException {
        TestClusterStarter.shutdownCluster();
    }

    @After
    public void clearReplicationPeers() {
        TestClusterStarter.clearReplicationPeers();
    }

    @Test
    public void testBasicPut() throws Exception {
        HBaseSource source = new HBaseSource(null, "test_table", TestClusterStarter.getConfig());
        // NumberSequenceSource source = new NumberSequenceSource(1, 10);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<byte[]> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "testBasicPut");

        // CompletableFuture<Collection<Long>> result = collector.collect(stream);
        stream.flatMap(
                new RichFlatMapFunction<byte[], Object>() {
                    @Override
                    public void flatMap(byte[] value, Collector<Object> out) throws Exception {
                        System.out.println("Test collected " + (new String(value)));
                        // TODO assertion goes here
                        //        assertEquals(
                        //                "HBase source did not produce the right values after a
                        // basic put operation",
                        //                new String[] {},1
                        //                result.get().toArray());
                        throw new SuccessException();
                    }
                });
        doAndWaitForSuccess(
                env,
                () -> {
                    DemoIngester ingester = new DemoIngester();
                    ingester.addRow();
                },
                10);
    }

    private static void doAndWaitForSuccess(
            StreamExecutionEnvironment env, Runnable action, int timeout) {
        try {
            JobClient jobClient = env.executeAsync();
            action.run();
            jobClient.getJobExecutionResult().get(5, TimeUnit.SECONDS);
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
}
