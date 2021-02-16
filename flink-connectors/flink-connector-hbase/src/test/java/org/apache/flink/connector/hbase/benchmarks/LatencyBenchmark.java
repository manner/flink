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

package org.apache.flink.connector.hbase.benchmarks;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.hbase.sink.HBaseSink;
import org.apache.flink.connector.hbase.sink.HBaseSinkSerializer;
import org.apache.flink.connector.hbase.source.HBaseSource;
import org.apache.flink.connector.hbase.source.reader.HBaseEvent;
import org.apache.flink.connector.hbase.source.reader.HBaseSourceDeserializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;

/** TODO docs. */
public class LatencyBenchmark {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String tableName = "latency";

        /* Need external process that inserts elements in HBase */

        HBaseSource<String> source =
                new HBaseSource<>(
                        Boundedness.CONTINUOUS_UNBOUNDED,
                        new HBaseStringDeserializationSchema(),
                        tableName + "-in",
                        new Configuration());

        DataStream<String> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "HBaseSource");

        HBaseSink<String> sink =
                new HBaseSink<>(
                        tableName + "-out",
                        new HBaseStringSerializationSchema(),
                        new Configuration());

        stream.sinkTo(sink);

        env.execute("HBaseBenchmark");

        /*
         * After the job has finished, we run an external process that goes through source & sink
         * table in HBase and compares timestamp for all elements with same id.
         *
         * e.g. "row-1200" in table "latency-in" has timestamp 21:00:02
         *      "row-1200" in table "latency-out" has timestamp 21:00:14
         *      => latency is 12s
         *
         * We check all elements and calculate the average latency.
         */
    }

    /** HBaseStringDeserializationSchema. */
    public static class HBaseStringDeserializationSchema extends HBaseSourceDeserializer<String> {

        public String deserialize(HBaseEvent event) {
            return new String(event.getPayload());
        }
    }

    /** HBaseStringSerializationSchema. */
    public static class HBaseStringSerializationSchema
            implements HBaseSinkSerializer<String>, Serializable {

        @Override
        public byte[] serializePayload(String event) {
            return Bytes.toBytes(event);
        }

        @Override
        public byte[] serializeColumnFamily(String event) {
            return Bytes.toBytes("cf");
        }

        @Override
        public byte[] serializeQualifier(String event) {
            return Bytes.toBytes("qualifier");
        }

        @Override
        public byte[] serializeRowKey(String event) {
            return Bytes.toBytes(event);
        }
    }
}
