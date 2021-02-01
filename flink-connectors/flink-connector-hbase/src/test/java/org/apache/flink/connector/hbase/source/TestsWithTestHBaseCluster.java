package org.apache.flink.connector.hbase.source;

import org.apache.flink.connector.hbase.source.hbasemocking.HBaseTestClusterUtil;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.UUID;

/**
 * Abstract test class that provides that {@link
 * org.apache.flink.connector.hbase.source.hbasemocking.HBaseTestClusterUtil} is up and running.
 */
public abstract class TestsWithTestHBaseCluster {

    /** For debug purposes. Allows to run the test quickly without starting a fresh cluster */
    public static final boolean USE_EXISTING_CLUSTER = false;

    /** Shadowed from org.apache.flink.test.util.SuccessException. */
    public static class SuccessException extends RuntimeException {}

    /**
     * Unique table name provided for each test; can be used to minimize interference between tests
     * on the same cluster.
     */
    protected String baseTableName;

    @Before
    public void determineBaseTableName() {
        baseTableName =
                String.format(
                        "%s-table-%s", getClass().getSimpleName().toLowerCase(), UUID.randomUUID());
    }

    @BeforeClass
    public static void setup() throws IOException {
        if (!USE_EXISTING_CLUSTER) {
            HBaseTestClusterUtil.startCluster();
        }
        assert HBaseTestClusterUtil.isClusterAlreadyRunning();
    }

    @AfterClass
    public static void teardown() throws IOException {
        if (!USE_EXISTING_CLUSTER) {
            HBaseTestClusterUtil.shutdownCluster();
        }
    }

    @After
    public void clearReplicationPeers() {
        HBaseTestClusterUtil.clearReplicationPeers();
        HBaseTestClusterUtil.clearTables();
    }

    protected static boolean causedBySuccess(Exception exception) {
        boolean success = false;
        for (Throwable e = exception; !success && e != null; e = e.getCause()) {
            success = success || e instanceof SuccessException;
        }
        return success;
    }
}
