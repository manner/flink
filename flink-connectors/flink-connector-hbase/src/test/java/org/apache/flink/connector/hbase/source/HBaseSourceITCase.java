package org.apache.flink.connector.hbase.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.hbase.source.hbasemocking.DemoIngester;
import org.apache.flink.connector.hbase.source.hbasemocking.HBaseTestClusterUtil;
import org.apache.flink.connector.hbase.testutil.FailureSink;
import org.apache.flink.connector.hbase.testutil.Util;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterJobClient;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FileUtils;

import org.apache.hadoop.hbase.client.Put;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertArrayEquals;

/** Tests the most basic use cases of the source with a mocked HBase system. */
public class HBaseSourceITCase extends TestsWithTestHBaseCluster {

    private static final File SIGNAL_FOLDER = new File("signal");

    private static final String SUCCESS_SIGNAL = "success";
    private static final String FAILURE_SIGNAL = "failure";

    private static void signalSuccess() {
        signal(SUCCESS_SIGNAL);
    }

    private static void signalFailure() {
        signal(FAILURE_SIGNAL);
    }

    private static void awaitSuccess(long timeout, TimeUnit timeUnit)
            throws InterruptedException, ExecutionException, TimeoutException {
        awaitSignalThrowOnFailure(SUCCESS_SIGNAL, timeout, timeUnit);
    }

    @Before
    public void makeSignalFolder() {
        SIGNAL_FOLDER.mkdirs();
    }

    @After
    public void cleanupSignalFolder() throws IOException {
        FileUtils.deleteDirectory(SIGNAL_FOLDER);
    }

    private static File signalFile(String signalName) {
        return SIGNAL_FOLDER.toPath().resolve(signalName + ".signal").toFile();
    }

    private static void signal(String signalName) {
        File signalFile = signalFile(signalName);
        try {
            signalFile.createNewFile();
            System.out.println("Created signal file at " + signalFile.getAbsolutePath());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void awaitSignalThrowOnFailure(
            String signalName, long timeout, TimeUnit timeUnit)
            throws InterruptedException, ExecutionException, TimeoutException {
        String result =
                (String)
                        CompletableFuture.anyOf(
                                        awaitSignal(signalName), awaitSignal(FAILURE_SIGNAL))
                                .get(timeout, timeUnit);
        if (result.equals(FAILURE_SIGNAL)) {
            throw new RuntimeException("Waiting for signal " + signalName + " yielded failure");
        }
    }

    private static CompletableFuture<String> awaitSignal(String signalName)
            throws InterruptedException, ExecutionException, TimeoutException {
        File signalFile = signalFile(signalName);
        return CompletableFuture.supplyAsync(
                () -> {
                    while (!signalFile.exists()) {
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    cleanupSignal(signalName);
                    return signalName;
                });
    }

    private static void cleanupSignal(String signalName) {
        File signalFile = signalFile(signalName);
        signalFile.delete();
    }

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

    @Test
    public void testRecordsAreProducedExactlyOnceWithCheckpoints() throws Exception {
        final String collectedValueSignal = "collectedValue";
        DemoIngester ingester = new DemoIngester(baseTableName);
        List<Put> puts = new ArrayList<>();
        List<String> expectedValues = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            Tuple2<Put, String> put = ingester.createOneColumnUniquePut();
            puts.add(put.f0);
            expectedValues.add(put.f1);
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        DataStream<String> stream = streamFromHBaseSource(env, baseTableName);
        stream.addSink(
                new FailureSink<String>(true, 3500, TypeInformation.of(String.class)) {

                    private void checkForSuccess() {
                        List<String> checkpointed = getCheckpointedValues();
                        System.out.println(unCheckpointedValues + " " + checkpointed);
                        if (checkpointed.size() == expectedValues.size()) {
                            try {
                                assertArrayEquals(
                                        "Wrong values were produced.",
                                        expectedValues.toArray(),
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
            for (int i = 0; i < puts.size(); i += putsPerPackage) {
                System.out.println("Sending next package ...");
                for (int j = i; j < puts.size() && j < i + putsPerPackage; j++) {
                    ingester.commitPut(puts.get(j));
                }
                // Assert that values have actually been sent over so there was an opportunity to
                // checkpoint them
                awaitSignalThrowOnFailure(collectedValueSignal, 120, TimeUnit.SECONDS);
                Thread.sleep(3000);
                cleanupSignal(collectedValueSignal);
            }
            awaitSuccess(120, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            jobClient.cancel();
        }
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

    /** Bla. */
    public static class HBaseStringDeserializationScheme
            extends AbstractDeserializationSchema<String> {

        @Override
        public String deserialize(byte[] message) throws IOException {
            return new String(message);
        }
    }
}
