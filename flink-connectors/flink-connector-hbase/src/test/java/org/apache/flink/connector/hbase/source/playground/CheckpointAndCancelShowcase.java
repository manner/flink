package org.apache.flink.connector.hbase.source.playground;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterJobClient;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Iterator;

/** Playground. */
public class CheckpointAndCancelShowcase {

    private static final Logger LOG = LoggerFactory.getLogger(CheckpointAndCancelShowcase.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        DataStream<String> stream = env.fromCollection(new Numbers(), String.class);

        stream.addSink(new FailOnCheckPoint<>(true));
        MiniClusterJobClient jobClient = (MiniClusterJobClient) env.executeAsync();
        MiniCluster miniCluster = miniCluster(jobClient);
        System.out.println("Started execution ...");
        while (!miniCluster.isRunning()) {
            Thread.sleep(100);
        }
        System.out.println("Flinkcluster is running");
        Thread.sleep(5000);
        miniCluster.close();
        while (miniCluster.isRunning()) {
            Thread.sleep(100);
        }
        System.out.println("Terminated ...");
    }

    private static class Numbers implements Iterator<String>, Serializable {

        private int i = 0;

        {
            System.out.println("Constructed iterator");
        }

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public String next() {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("Next called with i=" + i);
            return "" + (i++);
        }
    }

    private static class FailOnCheckPoint<T>
            implements SinkFunction<T>, CheckpointedFunction, CheckpointListener {

        private final boolean isPrinting;

        private FailOnCheckPoint(boolean isPrinting) {
            this.isPrinting = isPrinting;
        }

        @Override
        public void invoke(T value, Context context) throws Exception {
            if (isPrinting) System.out.println(value);
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            System.out.println(
                    "notifyCheckpointComplete has been called with checkpointId=" + checkpointId);
            throw new RuntimeException("Abort after checkpoint to trigger restart");
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            // System.out.println("snapshotState has been called");
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            // System.out.println("initializeState has been called");
        }
    }

    private static MiniCluster miniCluster(MiniClusterJobClient jobClient)
            throws IllegalAccessException, NoSuchFieldException {
        Field field = MiniClusterJobClient.class.getDeclaredField("miniCluster");
        field.setAccessible(true);
        return (MiniCluster) field.get(jobClient);
    }
}
