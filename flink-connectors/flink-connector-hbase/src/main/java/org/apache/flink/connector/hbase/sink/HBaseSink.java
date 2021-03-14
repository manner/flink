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

package org.apache.flink.connector.hbase.sink;

import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.hbase.sink.writer.HBaseWriter;
import org.apache.flink.connector.hbase.util.HBaseConfigurationUtil;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Mutation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

/**
 * A Sink Connector for HBase. Please use an {@link HBaseSinkBuilder} to construct a {@link
 * HBaseSink}. The following example shows how to create an HBaseSink that writes Long values to
 * HBase.
 *
 * <pre>{@code
 * HBaseSink<Long> hbaseSink =
 *      HBaseSink.<Long>builder()
 *          .setTableName(tableName)
 *          .setSinkSerializer(new HBaseLongSerializer())
 *          .setHBaseConfiguration(hbaseConfig)
 *          .build();
 * }</pre>
 *
 * <p>Here is an example for the Serializer:
 *
 * <pre>{@code
 * static class HBaseLongSerializer implements HBaseSinkSerializer<Long> {
 *     @Override
 *     public HBaseEvent serialize(Long event) {
 *         return HBaseEvent.putWith(                  // or deleteWith()
 *                 event.toString(),                   // rowId
 *                 "exampleColumnFamily",              // column family
 *                 "exampleQualifier",                 // qualifier
 *                 Bytes.toBytes(event.toString()));   // payload
 *     }
 * }
 * }</pre>
 *
 * <p>See {@link HBaseSinkBuilder} for more details.
 */
public class HBaseSink<IN> implements Sink<IN, HBaseSinkCommittable, Mutation, Void> {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseSink.class);

    private final HBaseSinkSerializer<IN> sinkSerializer;
    private final byte[] serializedConfig;
    private final Properties properties;

    HBaseSink(
            HBaseSinkSerializer<IN> sinkSerializer,
            Configuration hbaseConfiguration,
            Properties properties) {
        this.sinkSerializer = sinkSerializer;
        this.serializedConfig = HBaseConfigurationUtil.serializeConfiguration(hbaseConfiguration);
        this.properties = properties;
        LOG.debug("constructed sink");
    }

    public static <IN> HBaseSinkBuilder<IN> builder() {
        return new HBaseSinkBuilder<>();
    }

    @Override
    public SinkWriter<IN, HBaseSinkCommittable, Mutation> createWriter(
            InitContext context, List<Mutation> states) throws IOException {
        return new HBaseWriter<>(context, states, sinkSerializer, serializedConfig, properties);
    }

    @Override
    public Optional<Committer<HBaseSinkCommittable>> createCommitter() throws IOException {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalCommitter<HBaseSinkCommittable, Void>> createGlobalCommitter()
            throws IOException {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<HBaseSinkCommittable>> getCommittableSerializer() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getGlobalCommittableSerializer() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<Mutation>> getWriterStateSerializer() {
        return Optional.of(new HBaseSinkMutationSerializer());
    }
}
