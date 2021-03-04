package org.apache.flink.connector.hbase.testutil;

import org.apache.flink.util.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.connector.hbase.testutil.Logging.LOG;

/**
 * Utility class to signal events of tests such as success or failure. Allows to get signals out of
 * flink without using web sockets or success exceptions or similar.
 */
public class FileSignal {

    private static final long POLL_INTERVAL = 100; // ms
    private static final File SIGNAL_FOLDER = new File("signal");

    private static final String SUCCESS_SIGNAL = "success";
    private static final String FAILURE_SIGNAL = "failure";

    public static void signal(String signalName) {
        File signalFile = signalFile(signalName);
        try {
            signalFile.createNewFile();
            LOG.debug("Created signal file at " + signalFile.getAbsolutePath());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void signalSuccess() {
        signal(SUCCESS_SIGNAL);
    }

    public static void signalFailure() {
        signal(FAILURE_SIGNAL);
    }

    public static CompletableFuture<String> awaitSignal(String signalName) {
        File signalFile = signalFile(signalName);
        return CompletableFuture.supplyAsync(
                () -> {
                    while (!signalFile.exists()) {
                        try {
                            Thread.sleep(POLL_INTERVAL);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    cleanupSignal(signalName);
                    return signalName;
                });
    }

    public static void awaitSignalThrowOnFailure(String signalName, long timeout, TimeUnit timeUnit)
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

    public static void awaitSuccess(long timeout, TimeUnit timeUnit)
            throws InterruptedException, ExecutionException, TimeoutException {
        awaitSignalThrowOnFailure(SUCCESS_SIGNAL, timeout, timeUnit);
    }

    public static void makeFolder() {
        SIGNAL_FOLDER.mkdirs();
    }

    public static void cleanupSignal(String signalName) {
        File signalFile = signalFile(signalName);
        signalFile.delete();
    }

    public static void cleanupFolder() throws IOException {
        FileUtils.deleteDirectory(SIGNAL_FOLDER);
    }

    private static File signalFile(String signalName) {
        return SIGNAL_FOLDER.toPath().resolve(signalName + ".signal").toFile();
    }
}
