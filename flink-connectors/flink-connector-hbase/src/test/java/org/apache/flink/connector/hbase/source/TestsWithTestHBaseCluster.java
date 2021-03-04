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

import org.apache.flink.connector.hbase.testutil.HBaseTestClusterUtil;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/** Abstract test class that provides that {@link HBaseTestClusterUtil} is up and running. */
public abstract class TestsWithTestHBaseCluster {

    public static final int DEFAULT_CF_COUNT = 4;

    /**
     * For local debug purposes. Allows to run the test quickly without starting a fresh cluster for
     * each new test.
     */
    public static final boolean SHARE_CLUSTER = false;

    private static final HBaseTestClusterUtil sharedCluster = new HBaseTestClusterUtil();
    protected HBaseTestClusterUtil cluster;

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
    public static void setupSharedCluster()
            throws IOException, InterruptedException, ExecutionException, TimeoutException {
        if (SHARE_CLUSTER) {
            sharedCluster.startCluster();
            assert sharedCluster.isClusterAlreadyRunning();
        }
    }

    @AfterClass
    public static void teardownSharedCluster()
            throws IOException, InterruptedException, ExecutionException, TimeoutException {
        if (SHARE_CLUSTER) {
            sharedCluster.shutdownCluster();
        }
    }

    @After
    public void clearReplicationPeers() {
        if (SHARE_CLUSTER) {
            cluster.clearReplicationPeers();
            cluster.clearTables();
        }
    }

    @Before
    public void setupIndividualCluster()
            throws IOException, InterruptedException, ExecutionException, TimeoutException {
        if (!SHARE_CLUSTER) {
            cluster = new HBaseTestClusterUtil();
            cluster.startCluster();
            assert cluster.isClusterAlreadyRunning();
        } else {
            cluster = sharedCluster;
        }
    }

    @After
    public void teardownIndividualCluster()
            throws IOException, InterruptedException, ExecutionException, TimeoutException {
        if (!SHARE_CLUSTER) {
            cluster.shutdownCluster();
        }
    }

    protected static boolean causedBySuccess(Exception exception) {
        for (Throwable e = exception; e != null; e = e.getCause()) {
            if (e instanceof SuccessException) {
                return true;
            }
        }
        return false;
    }

    protected static String[] uniqueValues(int count) {
        String[] values = new String[count];
        for (int i = 0; i < count; i++) {
            values[i] = UUID.randomUUID().toString();
        }
        return values;
    }
}
