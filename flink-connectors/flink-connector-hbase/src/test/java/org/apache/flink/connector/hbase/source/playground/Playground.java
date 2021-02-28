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

package org.apache.flink.connector.hbase.source.playground;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.connector.hbase.source.HBaseSource;
import org.apache.flink.connector.hbase.source.hbasemocking.HBaseTestClusterUtil;
import org.apache.flink.connector.hbase.source.reader.HBaseEvent;
import org.apache.flink.connector.hbase.source.reader.HBaseSourceDeserializer;
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

        CustomHBaseDeserializationSchema deserializationSchema =
                new CustomHBaseDeserializationSchema();

        HBaseSource<String> source =
                new HBaseSource<>(
                        deserializationSchema,
                        "test-table",
                        new HBaseTestClusterUtil().getConfig());

        DataStream<String> stream =
                env.fromSource(
                        source,
                        WatermarkStrategy.noWatermarks(),
                        "HBaseSource",
                        deserializationSchema.getProducedType());

        stream.print();

        env.execute("HbaseTestJob");
    }

    /** Bla. */
    public static class CustomHBaseDeserializationSchema extends HBaseSourceDeserializer<String> {

        @Override
        public String deserialize(HBaseEvent event) {
            return new String(event.getPayload());
        }
    }
}
