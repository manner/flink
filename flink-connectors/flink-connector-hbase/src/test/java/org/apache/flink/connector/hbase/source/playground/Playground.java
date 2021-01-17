package org.apache.flink.connector.hbase.source.playground;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.connector.hbase.source.HbaseSource;
import org.apache.flink.connector.hbase.source.hbasemocking.TestClusterStarter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.config.Configurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Playground. */
public class Playground {

    private static final Logger LOG = LoggerFactory.getLogger(Playground.class);

    public static void main(String[] args) throws Exception {
        // Configurator.setRootLevel(Level.ERROR);
        Configurator.setAllLevels(LogManager.getRootLogger().getName(), Level.ERROR);
        Configurator.setLevel(LogManager.getLogger(Playground.class).getName(), Level.ERROR);

        // testBasicSource();
        testHBaseSource();
    }

    public static void testBasicSource() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final DataStream<Long> source1 =
                env.fromSource(
                        new NumberSequenceSource(1L, 10L),
                        WatermarkStrategy.noWatermarks(),
                        "source-1");
        source1.print();
        env.execute();
        System.out.println("Playground out");
    }

    public static void testHBaseSource() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        HbaseSource source =
                new HbaseSource(Boundedness.BOUNDED, "TestTable", TestClusterStarter.getConfig());

        DataStream<byte[]> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "HBaseSource");
        stream.map(String::new).print();

        env.execute("HbaseTestJob");
    }
}
