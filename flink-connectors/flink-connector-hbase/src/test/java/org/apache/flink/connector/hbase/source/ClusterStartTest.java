package org.apache.flink.connector.hbase.source;

import org.apache.flink.connector.hbase.source.hbasemocking.HBaseTestClusterUtil;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class ClusterStartTest {

    @BeforeClass
    public static void setup() {
        HBaseTestClusterUtil.startCluster();
    }

    @AfterClass
    public static void teardown() throws IOException {}

    @Test
    public void testBasicPut() throws Exception {
        System.out.println("Pass");
        while (true) Thread.sleep(30000);
    }
}
