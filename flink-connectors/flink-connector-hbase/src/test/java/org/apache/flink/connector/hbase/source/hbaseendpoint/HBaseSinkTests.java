package org.apache.flink.connector.hbase.source.hbaseendpoint;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.connector.hbase.sink.HBaseSink;
import org.apache.flink.connector.hbase.source.TestsWithTestHBaseCluster;
import org.apache.flink.connector.hbase.source.hbasemocking.HBaseTestClusterUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/** Test for {@link org.apache.flink.connector.hbase.sink.HBaseSink}. */
public class HBaseSinkTests extends TestsWithTestHBaseCluster {

    @Test
    public void testSimpleSink() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration hbaseConfiguration = HBaseTestClusterUtil.getConfig();

        String tableName = "test-table";
        long start = 1;
        long end = 10;

        final DataStream<Long> numberSource =
                env.fromSource(
                        new NumberSequenceSource(start, end),
                        WatermarkStrategy.noWatermarks(),
                        "numberSource");

        final HBaseSink<Long> hbaseSink = new HBaseSink<>(tableName, hbaseConfiguration);
        numberSource.sinkTo(hbaseSink);
        env.execute();

        try (Connection connection = ConnectionFactory.createConnection(hbaseConfiguration)) {
            Table table = connection.getTable(TableName.valueOf(tableName));
            for (long i = start; i <= end; i++) {
                Get get = new Get(Bytes.toBytes(String.valueOf(i)));
                Result r = table.get(get);
                byte[] value = r.getValue("info".getBytes(), "age".getBytes());
                assertEquals(Long.parseLong(new String(value)), i);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
