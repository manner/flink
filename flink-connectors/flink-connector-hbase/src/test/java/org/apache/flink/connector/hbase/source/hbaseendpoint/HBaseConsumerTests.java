package org.apache.flink.connector.hbase.source.hbaseendpoint;

import org.apache.flink.connector.hbase.source.TestsWithTestHBaseCluster;
import org.apache.flink.connector.hbase.source.hbasemocking.DemoIngester;
import org.apache.flink.connector.hbase.source.hbasemocking.DemoSchema;
import org.apache.flink.connector.hbase.source.hbasemocking.HBaseTestClusterUtil;
import org.apache.flink.connector.hbase.source.reader.HBaseEvent;
import org.apache.flink.connector.hbase.source.standalone.HBaseConsumer;

import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNotNull;

/** Tests for {@link org.apache.flink.connector.hbase.source.standalone.HBaseConsumer}. */
public class HBaseConsumerTests extends TestsWithTestHBaseCluster {

    @Test
    public void testSetup() throws Exception {
        new HBaseConsumer(HBaseTestClusterUtil.getConfig())
                .startReplication(DemoSchema.TABLE_NAME, DemoSchema.COLUMN_FAMILY_NAME);
    }

    @Test
    public void testPutCreatesEvent() throws Exception {
        HBaseConsumer consumer = new HBaseConsumer(HBaseTestClusterUtil.getConfig());
        consumer.startReplication(DemoSchema.TABLE_NAME, DemoSchema.COLUMN_FAMILY_NAME);
        DemoIngester ingester = new DemoIngester();
        ingester.commitPut(ingester.createPut().f0);
        HBaseEvent result = CompletableFuture.supplyAsync(consumer::next).get(30, TimeUnit.SECONDS);
        assertNotNull(result);
    }
}
