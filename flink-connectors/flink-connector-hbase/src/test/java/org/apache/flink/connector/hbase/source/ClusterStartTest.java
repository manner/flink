package org.apache.flink.connector.hbase.source;

import org.apache.flink.connector.hbase.source.hbasemocking.HBaseTestClusterUtil;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;

/** This class is only used to start a (reusable) test cluster from the test root. */
@Deprecated
public class ClusterStartTest {

    @BeforeClass
    public static void setup() throws IOException {
        HBaseTestClusterUtil.startCluster();
    }

    @AfterClass
    public static void teardown() {}

    public void startCluster() throws Exception {
        System.out.println("Pass");
        while (true) {
            Thread.sleep(30000);
        }
    }
}
