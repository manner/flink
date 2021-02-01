package org.apache.flink.connector.hbase.source.hbaseendpoint;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.connector.hbase.sink.HBaseSink;
import org.apache.flink.connector.hbase.source.TestsWithTestHBaseCluster;
import org.apache.flink.connector.hbase.source.hbasemocking.HBaseTestClusterUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.Test;

/** Test for {@link org.apache.flink.connector.hbase.sink.HBaseSink}. */
public class HBaseSinkTests extends TestsWithTestHBaseCluster {

    @Test
    public void testSimpleSink() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final DataStream<Long> numberSource =
                env.fromSource(
                        new NumberSequenceSource(1L, 10L),
                        WatermarkStrategy.noWatermarks(),
                        "numberSource");

        final HBaseSink<Long> hbaseSink =
                new HBaseSink<>("test-table", HBaseTestClusterUtil.getConfig());
        numberSource.sinkTo(hbaseSink);
        env.execute();
    }
}
