package org.apache.flink.connector.hbase.source.hbaseendpoint;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.connector.hbase.sink.HBaseSink;
import org.apache.flink.connector.hbase.source.TestsWithTestHBaseCluster;
import org.apache.flink.connector.hbase.source.hbasemocking.DemoSchema;
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
import java.util.stream.LongStream;

import static org.junit.Assert.assertArrayEquals;

/** Test for {@link org.apache.flink.connector.hbase.sink.HBaseSink}. */
public class HBaseSinkTests extends TestsWithTestHBaseCluster {

    @Test
    public void testSimpleSink() throws Exception {
        DemoSchema schema = new DemoSchema();
        schema.createSchema(HBaseTestClusterUtil.getConfig());

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration hbaseConfiguration = HBaseTestClusterUtil.getConfig();

        int start = 1;
        int end = 10;

        final DataStream<Long> numberSource =
                env.fromSource(
                        new NumberSequenceSource(start, end),
                        WatermarkStrategy.noWatermarks(),
                        "numberSource");

        String tableName = "test-table";
        String columnFamily = "info";
        String qualifier = "test";

        final HBaseSink<Long> hbaseSink =
                new HBaseSink<>(tableName, columnFamily, qualifier, hbaseConfiguration);
        numberSource.sinkTo(hbaseSink);
        env.execute();

        long[] expected = LongStream.rangeClosed(start, end).toArray();
        long[] actual = new long[end - start + 1];

        try (Connection connection = ConnectionFactory.createConnection(hbaseConfiguration)) {
            Table table = connection.getTable(TableName.valueOf(tableName));
            for (int i = start; i <= end; i++) {
                Get get = new Get(Bytes.toBytes(String.valueOf(i)));
                Result r = table.get(get);
                byte[] value = r.getValue(columnFamily.getBytes(), qualifier.getBytes());
                long l = Long.parseLong(new String(value));
                actual[i - start] = l;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        assertArrayEquals(expected, actual);
    }
}
