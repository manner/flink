package org.apache.flink.connector.hbase.testutil;

import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterJobClient;

import java.lang.reflect.Field;

/** Provides utility for testing with flink miniclusters. */
public class Util {

    public static MiniCluster miniCluster(MiniClusterJobClient jobClient)
            throws IllegalAccessException, NoSuchFieldException {
        Field field = MiniClusterJobClient.class.getDeclaredField("miniCluster");
        field.setAccessible(true);
        return (MiniCluster) field.get(jobClient);
    }

    public static void waitForClusterStart(MiniCluster miniCluster, boolean verbose)
            throws InterruptedException {
        if (verbose) {
            System.out.println("Started flink minicluster execution ...");
        }

        while (!miniCluster.isRunning()) {
            Thread.sleep(100);
        }

        if (verbose) {
            System.out.println("Flink minicluster is running");
        }
    }
}
